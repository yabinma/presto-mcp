// Package config loads and validates the presto-mcp YAML configuration.
//
// Credentials are never stored inline: the config holds only references
// (for example env://VAR or vault://path) that a credential provider resolves
// at connection time. See the credential package.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// DeploymentMode selects the default transport and credential strategy. It only
// switches the edges of the server; it does not fork the core logic.
type DeploymentMode string

// Deployment modes.
const (
	ModeLocal      DeploymentMode = "local"
	ModeEnterprise DeploymentMode = "enterprise"
)

// Transport is the MCP transport the server listens on.
type Transport string

// Supported transports.
const (
	TransportStdio Transport = "stdio"
	TransportHTTP  Transport = "http"
)

// Dialect identifies the engine flavor. Phase 1 supports presto and trino; wxd
// is accepted by config validation but not yet implemented by the client.
type Dialect string

// Supported engine dialects.
const (
	DialectPresto Dialect = "presto"
	DialectTrino  Dialect = "trino"
	DialectWxd    Dialect = "wxd"
)

// AuthMode selects how an engine's credential is obtained (the strategy).
type AuthMode string

const (
	// AuthStatic resolves a credential from a config reference (local shape).
	AuthStatic AuthMode = "static"
	// AuthPassthrough forwards the caller's credential (enterprise shape, Phase 2).
	AuthPassthrough AuthMode = "passthrough"
)

// AuthScheme selects how the resolved credential is sent on the wire.
type AuthScheme string

const (
	// SchemeBearer sends "Authorization: Bearer <token>" (JWT / OAuth2). Default.
	SchemeBearer AuthScheme = "bearer"
	// SchemeBasic sends "Authorization: Basic <base64(user:password)>" and
	// requires an https endpoint.
	SchemeBasic AuthScheme = "basic"
)

// Config is the root configuration document.
type Config struct {
	DeploymentMode DeploymentMode `yaml:"deployment_mode"`
	Server         ServerConfig   `yaml:"server"`
	Engines        []EngineConfig `yaml:"engines"`
}

// ServerConfig configures the transport edge.
type ServerConfig struct {
	Transport Transport   `yaml:"transport"`
	HTTP      *HTTPConfig `yaml:"http,omitempty"`
}

// HTTPConfig configures the streamable-HTTP transport edge (enterprise shape).
type HTTPConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	// TLSCertRef / TLSKeyRef reference a PEM certificate and private key
	// (file://, env://). When both are set the server listens with TLS.
	TLSCertRef string `yaml:"tls_cert_ref,omitempty"`
	TLSKeyRef  string `yaml:"tls_key_ref,omitempty"`
	// AllowedOrigins is the Origin-header allowlist (DNS-rebinding protection for
	// browser clients). Empty disables the check (e.g. when fronted by a gateway).
	AllowedOrigins []string `yaml:"allowed_origins,omitempty"`
	// EdgeAuth optionally verifies the caller's bearer token at the MCP edge
	// before forwarding it to the engine. Nil/omitted means opaque passthrough
	// (the default): the Authorization header is forwarded unverified.
	EdgeAuth *EdgeAuthConfig `yaml:"edge_auth,omitempty"`
}

// EdgeAuthScheme selects how the edge verifies a caller's bearer token.
type EdgeAuthScheme string

const (
	// EdgeAuthJWTRS256 verifies an RS256 JWT signature against a configured PEM
	// public key and checks the expiry claim.
	EdgeAuthJWTRS256 EdgeAuthScheme = "jwt_rs256"
)

// EdgeAuthConfig configures optional bearer-token verification at the MCP edge.
type EdgeAuthConfig struct {
	// Scheme is the verification method. Currently only jwt_rs256.
	Scheme EdgeAuthScheme `yaml:"scheme"`
	// PublicKeyRef references a PEM public key (file://, env://) used to verify
	// bearer tokens. Required for jwt_rs256.
	PublicKeyRef string `yaml:"public_key_ref,omitempty"`
}

// EngineConfig describes one reachable Presto/Trino engine.
type EngineConfig struct {
	ID       string        `yaml:"id"`
	Endpoint string        `yaml:"endpoint"`
	Dialect  Dialect       `yaml:"dialect"`
	Auth     AuthConfig    `yaml:"auth"`
	History  HistoryConfig `yaml:"history"`
	// TLSInsecureSkipVerify disables TLS certificate verification for this engine.
	// Intended for self-signed certificates in dev/test; do not use in production.
	TLSInsecureSkipVerify bool `yaml:"tls_insecure_skip_verify,omitempty"`
}

// AuthConfig configures credential resolution for an engine.
type AuthConfig struct {
	Mode AuthMode `yaml:"mode"`
	// Scheme is how the credential is sent: bearer (default) or basic.
	Scheme AuthScheme `yaml:"scheme,omitempty"`
	// User is the engine username sent in the X-{Presto,Trino}-User header (and
	// the username for basic auth). Defaults to "presto-mcp" for bearer/unsecured
	// static engines; required (no default) for the basic scheme.
	User string `yaml:"user,omitempty"`
	// CredentialRef is a reference (env://, file://, vault://) to a bearer token,
	// resolved at connection time. Empty means no token (unsecured dev engine).
	// Used by the bearer scheme.
	CredentialRef string `yaml:"credential_ref,omitempty"`
	// PasswordRef is a reference to the password for the basic scheme, resolved
	// the same way as CredentialRef.
	PasswordRef string `yaml:"password_ref,omitempty"`
}

// HistoryConfig selects an optional history sink for an engine. Concrete
// providers are added in Phase 2; in Phase 1 queries always come from the
// coordinator's memory.
type HistoryConfig struct {
	Enabled    bool              `yaml:"enabled"`
	Provider   string            `yaml:"provider,omitempty"`
	Connection map[string]string `yaml:"connection,omitempty"`
	Mapping    map[string]string `yaml:"mapping,omitempty"`
}

// Load reads, parses, applies defaults to, and validates a config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(data)
}

// Parse parses, defaults, and validates config from raw YAML bytes.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// applyDefaults fills in mode-derived defaults without overriding explicit values.
func (c *Config) applyDefaults() {
	if c.DeploymentMode == "" {
		c.DeploymentMode = ModeLocal
	}
	if c.Server.Transport == "" {
		if c.DeploymentMode == ModeEnterprise {
			c.Server.Transport = TransportHTTP
		} else {
			c.Server.Transport = TransportStdio
		}
	}
	defaultAuth := AuthStatic
	if c.DeploymentMode == ModeEnterprise {
		defaultAuth = AuthPassthrough
	}
	for i := range c.Engines {
		e := &c.Engines[i]
		if e.Dialect == "" {
			e.Dialect = DialectPresto
		}
		if e.Auth.Mode == "" {
			e.Auth.Mode = defaultAuth
		}
		if e.Auth.Scheme == "" {
			e.Auth.Scheme = SchemeBearer
		}
		// Default a username only when one isn't semantically required; the basic
		// scheme must carry the real account name, so it is never defaulted.
		if e.Auth.Mode == AuthStatic && e.Auth.Scheme != SchemeBasic && e.Auth.User == "" {
			e.Auth.User = "presto-mcp"
		}
	}
}

// Validate checks structural and semantic constraints.
func (c *Config) Validate() error {
	switch c.DeploymentMode {
	case ModeLocal, ModeEnterprise:
	default:
		return fmt.Errorf("deployment_mode: invalid value %q (want local|enterprise)", c.DeploymentMode)
	}
	switch c.Server.Transport {
	case TransportStdio:
	case TransportHTTP:
		if c.Server.HTTP == nil || c.Server.HTTP.Port == 0 {
			return fmt.Errorf("server.http.port: required when transport is http")
		}
		if err := c.Server.HTTP.validate(); err != nil {
			return fmt.Errorf("server.http: %w", err)
		}
	default:
		return fmt.Errorf("server.transport: invalid value %q (want stdio|http)", c.Server.Transport)
	}
	if len(c.Engines) == 0 {
		return fmt.Errorf("engines: at least one engine is required")
	}
	seen := make(map[string]bool, len(c.Engines))
	for i := range c.Engines {
		if err := c.Engines[i].validate(); err != nil {
			return fmt.Errorf("engines[%d]: %w", i, err)
		}
		id := c.Engines[i].ID
		if seen[id] {
			return fmt.Errorf("engines: duplicate id %q", id)
		}
		seen[id] = true
	}
	return nil
}

func (h *HTTPConfig) validate() error {
	if (h.TLSCertRef == "") != (h.TLSKeyRef == "") {
		return fmt.Errorf("tls_cert_ref and tls_key_ref must be set together")
	}
	if h.EdgeAuth != nil {
		switch h.EdgeAuth.Scheme {
		case EdgeAuthJWTRS256:
			if h.EdgeAuth.PublicKeyRef == "" {
				return fmt.Errorf("edge_auth.public_key_ref: required for scheme %q", h.EdgeAuth.Scheme)
			}
		default:
			return fmt.Errorf("edge_auth.scheme: invalid value %q (want jwt_rs256)", h.EdgeAuth.Scheme)
		}
	}
	return nil
}

func (e *EngineConfig) validate() error {
	if e.ID == "" {
		return fmt.Errorf("id is required")
	}
	if e.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	u, err := url.Parse(e.Endpoint)
	if err != nil {
		return fmt.Errorf("endpoint %q: %w", e.Endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("endpoint %q: scheme must be http or https", e.Endpoint)
	}
	if u.Host == "" {
		return fmt.Errorf("endpoint %q: missing host", e.Endpoint)
	}
	switch e.Dialect {
	case DialectPresto, DialectTrino, DialectWxd:
	default:
		return fmt.Errorf("dialect: invalid value %q (want presto|trino|wxd)", e.Dialect)
	}
	switch e.Auth.Mode {
	case AuthStatic, AuthPassthrough:
	default:
		return fmt.Errorf("auth.mode: invalid value %q (want static|passthrough)", e.Auth.Mode)
	}
	switch e.Auth.Scheme {
	case SchemeBearer, "":
	case SchemeBasic:
		if e.Auth.User == "" {
			return fmt.Errorf("auth.user: required for the basic scheme")
		}
		if e.Auth.PasswordRef == "" {
			return fmt.Errorf("auth.password_ref: required for the basic scheme")
		}
		if u.Scheme != "https" {
			return fmt.Errorf("auth.scheme basic requires an https endpoint (Basic credentials must not be sent over plaintext http)")
		}
	default:
		return fmt.Errorf("auth.scheme: invalid value %q (want bearer|basic)", e.Auth.Scheme)
	}
	if e.History.Enabled && e.History.Provider == "" {
		return fmt.Errorf("history.provider: required when history.enabled is true")
	}
	return nil
}
