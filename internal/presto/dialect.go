package presto

import (
	"fmt"
	"net/http"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
)

// dialect captures the per-flavor request differences. Presto and Trino differ
// only in their header prefix; both use "Authorization: Bearer" for tokens.
// The wxd flavor is deferred (see the plan); NewClient rejects it for now.
type dialect struct {
	name   string
	prefix string // header prefix, e.g. "X-Trino-"
}

func dialectFor(d config.Dialect) (dialect, error) {
	switch d {
	case config.DialectTrino:
		return dialect{name: "trino", prefix: "X-Trino-"}, nil
	case config.DialectPresto:
		return dialect{name: "presto", prefix: "X-Presto-"}, nil
	case config.DialectWxd:
		return dialect{}, fmt.Errorf("dialect wxd is not yet supported (deferred after the Presto deployment)")
	default:
		return dialect{}, fmt.Errorf("unknown dialect %q", d)
	}
}

// header builds a flavor-specific header name, e.g. "User" -> "X-Trino-User".
func (d dialect) header(name string) string { return d.prefix + name }

// apply sets the user, optional session catalog/schema, source, and the
// authorization header on a request. A verbatim passthrough AuthHeader takes
// precedence, then basic (username/password), then bearer; none set means an
// unsecured engine. The user header is omitted when empty (passthrough without a
// configured user) so the engine derives identity from the forwarded credential.
func (d dialect) apply(req *http.Request, cred credential.Credential, catalog, schema, source string) {
	if cred.User != "" {
		req.Header.Set(d.header("User"), cred.User)
	}
	req.Header.Set(d.header("Source"), source)
	if catalog != "" {
		req.Header.Set(d.header("Catalog"), catalog)
	}
	if schema != "" {
		req.Header.Set(d.header("Schema"), schema)
	}
	switch {
	case cred.AuthHeader != "":
		req.Header.Set("Authorization", cred.AuthHeader)
	case cred.Password != "":
		req.SetBasicAuth(cred.User, cred.Password)
	case cred.Token != "":
		req.Header.Set("Authorization", "Bearer "+cred.Token)
	}
}
