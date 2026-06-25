// Package credential resolves engine credentials from references.
//
// The registry and config store only references (env://, file://, vault://);
// the actual secret is resolved here at connection time. Phase 1 ships the
// static provider; the passthrough provider (which extracts the caller's
// credential from the request) is added in Phase 2 behind the same interface.
package credential

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// Credential is the resolved secret material for one engine request. At most one
// of AuthHeader / Token / Password is set: AuthHeader is a verbatim Authorization
// value (passthrough), Token drives bearer auth, Password drives basic auth, and
// none means unsecured.
type Credential struct {
	// User is the engine username (X-{Presto,Trino}-User header) and the username
	// for basic auth. It may be empty for passthrough, in which case the engine
	// derives the identity from the forwarded Authorization header (e.g. a JWT
	// subject) and no user header is sent.
	User string
	// AuthHeader, when set, is forwarded verbatim as the Authorization header.
	// Used by the passthrough strategy so the caller's credential (any scheme) is
	// preserved exactly. It takes precedence over Token and Password.
	AuthHeader string
	// Token is a bearer token sent as "Authorization: Bearer <token>".
	Token string
	// Password is the basic-auth password sent as part of
	// "Authorization: Basic <base64(user:password)>".
	Password string
}

// Provider yields a credential for an outgoing engine request. Implementations
// are injected so handlers and the client can be tested with fakes.
type Provider interface {
	// Resolve returns the credential to use. The static provider ignores ctx;
	// the passthrough provider (Phase 2) reads the caller identity from it.
	Resolve(ctx context.Context) (Credential, error)
}

// RefResolver turns a reference string into its secret value. It is injectable
// so tests need not touch the environment or filesystem.
type RefResolver func(ref string) (string, error)

// DefaultResolver resolves env:// and file:// references. vault:// is recognized
// but not yet implemented (Phase 2/3).
func DefaultResolver(ref string) (string, error) {
	switch {
	case strings.HasPrefix(ref, "env://"):
		name := strings.TrimPrefix(ref, "env://")
		if name == "" {
			return "", fmt.Errorf("env reference is missing a variable name")
		}
		v, ok := os.LookupEnv(name)
		if !ok {
			return "", fmt.Errorf("environment variable %q is not set", name)
		}
		return v, nil
	case strings.HasPrefix(ref, "file://"):
		path := strings.TrimPrefix(ref, "file://")
		if path == "" {
			return "", fmt.Errorf("file reference is missing a path")
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read credential file: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	case strings.HasPrefix(ref, "vault://"):
		return "", fmt.Errorf("vault references are not supported yet")
	default:
		return "", fmt.Errorf("unsupported credential reference %q (want env://, file://)", ref)
	}
}

// StaticProvider returns a fixed credential resolved once at construction.
type StaticProvider struct {
	cred Credential
}

// NewStatic builds a static provider, resolving credentialRef eagerly so a
// missing or malformed reference fails at startup. An empty credentialRef means
// no token (unsecured engine). resolver may be nil to use DefaultResolver.
func NewStatic(user, credentialRef string, resolver RefResolver) (*StaticProvider, error) {
	if user == "" {
		return nil, fmt.Errorf("static credential requires a user")
	}
	cred := Credential{User: user}
	if credentialRef != "" {
		if resolver == nil {
			resolver = DefaultResolver
		}
		token, err := resolver(credentialRef)
		if err != nil {
			return nil, fmt.Errorf("resolve credential_ref: %w", err)
		}
		cred.Token = token
	}
	return &StaticProvider{cred: cred}, nil
}

// NewBasic builds a static provider for HTTP Basic auth, resolving passwordRef
// eagerly. Both a user and a password reference are required. resolver may be nil
// to use DefaultResolver.
func NewBasic(user, passwordRef string, resolver RefResolver) (*StaticProvider, error) {
	if user == "" {
		return nil, fmt.Errorf("basic credential requires a user")
	}
	if passwordRef == "" {
		return nil, fmt.Errorf("basic credential requires a password reference")
	}
	if resolver == nil {
		resolver = DefaultResolver
	}
	password, err := resolver(passwordRef)
	if err != nil {
		return nil, fmt.Errorf("resolve password_ref: %w", err)
	}
	return &StaticProvider{cred: Credential{User: user, Password: password}}, nil
}

// Resolve returns the pre-resolved credential.
func (p *StaticProvider) Resolve(context.Context) (Credential, error) {
	return p.cred, nil
}

// Caller is the credential material extracted from an incoming MCP request for
// the passthrough strategy (enterprise shape). The tool middleware places it on
// the request context; PassthroughProvider.Resolve reads it. It deliberately
// carries no MCP types so this package stays transport-agnostic.
type Caller struct {
	// AuthHeader is the verbatim incoming Authorization header value, forwarded
	// to the engine as-is so any scheme (Bearer/Basic) is preserved exactly.
	AuthHeader string
	// VerifiedUser is the username proven by edge-auth verification (the verified
	// bearer token's subject), set only when the optional verify mode ran. Empty
	// in opaque passthrough, where the engine derives identity from AuthHeader.
	VerifiedUser string
}

type callerKey struct{}

// WithCaller returns a context carrying the caller's passthrough credential.
func WithCaller(ctx context.Context, c Caller) context.Context {
	return context.WithValue(ctx, callerKey{}, c)
}

// CallerFromContext returns the caller credential placed on ctx, if any.
func CallerFromContext(ctx context.Context) (Caller, bool) {
	c, ok := ctx.Value(callerKey{}).(Caller)
	return c, ok
}

// PassthroughProvider forwards the caller's credential (extracted per request
// from the context) to the engine. The server holds no identity of its own; the
// engine performs authn/authz. This is the enterprise-shape credential strategy.
type PassthroughProvider struct {
	// fallbackUser is the engine username used when the request carries no
	// verified identity (config auth.user). Empty means send no user header and
	// let the engine derive identity from the forwarded Authorization header.
	fallbackUser string
}

// NewPassthrough builds a passthrough provider. fallbackUser may be empty.
func NewPassthrough(fallbackUser string) *PassthroughProvider {
	return &PassthroughProvider{fallbackUser: fallbackUser}
}

// Resolve returns a credential that forwards the caller's Authorization header.
// It errors when the request carried no credential, since enterprise mode
// requires an authenticated caller (the engine, not the server, authorizes it).
func (p *PassthroughProvider) Resolve(ctx context.Context) (Credential, error) {
	caller, ok := CallerFromContext(ctx)
	if !ok || caller.AuthHeader == "" {
		return Credential{}, fmt.Errorf("passthrough credential: request carried no Authorization header (enterprise mode requires an authenticated caller)")
	}
	user := caller.VerifiedUser
	if user == "" {
		user = p.fallbackUser
	}
	return Credential{User: user, AuthHeader: caller.AuthHeader}, nil
}
