package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const validLocal = `
deployment_mode: local
engines:
  - id: dev
    endpoint: https://presto-dev.internal:8443
    dialect: presto
    auth:
      mode: static
      credential_ref: env://TOK
`

func TestParse_DefaultsLocal(t *testing.T) {
	cfg, err := Parse([]byte(validLocal))
	require.NoError(t, err)
	assert.Equal(t, ModeLocal, cfg.DeploymentMode)
	assert.Equal(t, TransportStdio, cfg.Server.Transport)
	assert.Equal(t, AuthStatic, cfg.Engines[0].Auth.Mode)
	assert.Equal(t, "presto-mcp", cfg.Engines[0].Auth.User, "static user defaults")
}

func TestParse_EnterpriseDefaults(t *testing.T) {
	cfg, err := Parse([]byte(`
deployment_mode: enterprise
server:
  transport: http
  http: { host: 0.0.0.0, port: 8080 }
engines:
  - id: prod
    endpoint: https://trino:8443
    dialect: trino
`))
	require.NoError(t, err)
	assert.Equal(t, TransportHTTP, cfg.Server.Transport)
	assert.Equal(t, AuthPassthrough, cfg.Engines[0].Auth.Mode, "enterprise defaults to passthrough")
	assert.Empty(t, cfg.Engines[0].Auth.User, "passthrough does not default a user")
}

func TestParse_HTTPEdgeAuthAndTLS(t *testing.T) {
	cfg, err := Parse([]byte(`
deployment_mode: enterprise
server:
  transport: http
  http:
    host: 0.0.0.0
    port: 8443
    tls_cert_ref: file:///etc/presto-mcp/tls.crt
    tls_key_ref: file:///etc/presto-mcp/tls.key
    allowed_origins: ["https://agent.internal"]
    edge_auth:
      scheme: jwt_rs256
      public_key_ref: file:///etc/presto-mcp/jwt-pub.pem
engines:
  - id: e
    endpoint: https://trino:8443
    auth: {mode: passthrough}
`))
	require.NoError(t, err)
	h := cfg.Server.HTTP
	require.NotNil(t, h)
	assert.Equal(t, "file:///etc/presto-mcp/tls.crt", h.TLSCertRef)
	assert.Equal(t, []string{"https://agent.internal"}, h.AllowedOrigins)
	require.NotNil(t, h.EdgeAuth)
	assert.Equal(t, EdgeAuthJWTRS256, h.EdgeAuth.Scheme)
	assert.Equal(t, "file:///etc/presto-mcp/jwt-pub.pem", h.EdgeAuth.PublicKeyRef)
}

func TestParse_DialectDefaultsToPresto(t *testing.T) {
	cfg, err := Parse([]byte(`
engines:
  - id: e
    endpoint: http://h:8080
    auth: { mode: static }
`))
	require.NoError(t, err)
	assert.Equal(t, DialectPresto, cfg.Engines[0].Dialect)
}

func TestParse_Errors(t *testing.T) {
	cases := map[string]string{
		"bad mode": `
deployment_mode: weird
engines: [{id: e, endpoint: http://h:1, auth: {mode: static}}]`,
		"bad transport": `
server: {transport: ftp}
engines: [{id: e, endpoint: http://h:1, auth: {mode: static}}]`,
		"http without port": `
deployment_mode: enterprise
server: {transport: http}
engines: [{id: e, endpoint: http://h:1, auth: {mode: passthrough}}]`,
		"no engines": `
deployment_mode: local`,
		"missing id": `
engines: [{endpoint: http://h:1, auth: {mode: static}}]`,
		"missing endpoint": `
engines: [{id: e, auth: {mode: static}}]`,
		"bad endpoint scheme": `
engines: [{id: e, endpoint: ftp://h:1, auth: {mode: static}}]`,
		"endpoint no host": `
engines: [{id: e, endpoint: "http://", auth: {mode: static}}]`,
		"bad dialect": `
engines: [{id: e, endpoint: http://h:1, dialect: oracle, auth: {mode: static}}]`,
		"bad auth mode": `
engines: [{id: e, endpoint: http://h:1, auth: {mode: kerberos}}]`,
		"dup ids": `
engines:
  - {id: e, endpoint: http://h:1, auth: {mode: static}}
  - {id: e, endpoint: http://h:2, auth: {mode: static}}`,
		"history without provider": `
engines:
  - id: e
    endpoint: http://h:1
    auth: {mode: static}
    history: {enabled: true}`,
		"unknown field": `
engines: [{id: e, endpoint: http://h:1, auth: {mode: static}}]
bogus: 1`,
		"tls cert without key": `
deployment_mode: enterprise
server: {transport: http, http: {port: 8080, tls_cert_ref: file:///c.pem}}
engines: [{id: e, endpoint: http://h:1, auth: {mode: passthrough}}]`,
		"edge auth bad scheme": `
deployment_mode: enterprise
server: {transport: http, http: {port: 8080, edge_auth: {scheme: hmac}}}
engines: [{id: e, endpoint: http://h:1, auth: {mode: passthrough}}]`,
		"edge auth jwt without key": `
deployment_mode: enterprise
server: {transport: http, http: {port: 8080, edge_auth: {scheme: jwt_rs256}}}
engines: [{id: e, endpoint: http://h:1, auth: {mode: passthrough}}]`,
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(doc))
			assert.Error(t, err)
		})
	}
}

func TestParse_SchemeDefaultsToBearer(t *testing.T) {
	cfg, err := Parse([]byte(validLocal))
	require.NoError(t, err)
	assert.Equal(t, SchemeBearer, cfg.Engines[0].Auth.Scheme)
}

func TestParse_BasicScheme(t *testing.T) {
	t.Run("valid over https", func(t *testing.T) {
		cfg, err := Parse([]byte(`
engines:
  - id: e
    endpoint: https://trino:8443
    auth:
      mode: static
      scheme: basic
      user: alice
      password_ref: env://PW`))
		require.NoError(t, err)
		assert.Equal(t, SchemeBasic, cfg.Engines[0].Auth.Scheme)
		assert.Equal(t, "alice", cfg.Engines[0].Auth.User, "basic user is not defaulted")
	})

	t.Run("rejected over http", func(t *testing.T) {
		_, err := Parse([]byte(`
engines:
  - id: e
    endpoint: http://trino:8080
    auth: {mode: static, scheme: basic, user: alice, password_ref: env://PW}`))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "https")
	})

	t.Run("requires user", func(t *testing.T) {
		_, err := Parse([]byte(`
engines:
  - id: e
    endpoint: https://trino:8443
    auth: {mode: static, scheme: basic, password_ref: env://PW}`))
		assert.Error(t, err)
	})

	t.Run("requires password_ref", func(t *testing.T) {
		_, err := Parse([]byte(`
engines:
  - id: e
    endpoint: https://trino:8443
    auth: {mode: static, scheme: basic, user: alice}`))
		assert.Error(t, err)
	})

	t.Run("invalid scheme", func(t *testing.T) {
		_, err := Parse([]byte(`
engines:
  - id: e
    endpoint: https://trino:8443
    auth: {mode: static, scheme: kerberos, user: alice}`))
		assert.Error(t, err)
	})
}

func TestParse_NotYAML(t *testing.T) {
	_, err := Parse([]byte("\t not: [valid"))
	assert.Error(t, err)
}

func TestParse_WxdAcceptedAtConfig(t *testing.T) {
	cfg, err := Parse([]byte(`
engines: [{id: e, endpoint: http://h:1, dialect: wxd, auth: {mode: static}}]`))
	require.NoError(t, err)
	assert.Equal(t, DialectWxd, cfg.Engines[0].Dialect)
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validLocal), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Len(t, cfg.Engines, 1)

	_, err = Load(filepath.Join(dir, "missing.yaml"))
	assert.Error(t, err)
}
