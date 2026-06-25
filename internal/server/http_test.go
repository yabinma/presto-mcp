package server

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/registry"
)

// recordingEngine is a fake Presto engine that answers SHOW CATALOGS and records
// the auth headers it received, so a test can assert what was forwarded to it.
type recordingEngine struct {
	*httptest.Server
	mu   sync.Mutex
	auth string
	user string
}

func newRecordingEngine(t *testing.T) *recordingEngine {
	re := &recordingEngine{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/statement", func(w http.ResponseWriter, r *http.Request) {
		re.mu.Lock()
		re.auth = r.Header.Get("Authorization")
		re.user = r.Header.Get("X-Trino-User")
		re.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"columns":[{"name":"Catalog"}],"data":[["hive"],["system"]]}`))
	})
	re.Server = httptest.NewServer(mux)
	t.Cleanup(re.Close)
	return re
}

func (re *recordingEngine) seen() (auth, user string) {
	re.mu.Lock()
	defer re.mu.Unlock()
	return re.auth, re.user
}

func passthroughRegistry(t *testing.T, endpoint string) *registry.Registry {
	cfg := &config.Config{
		DeploymentMode: config.ModeEnterprise,
		Engines: []config.EngineConfig{{
			ID: "e", Endpoint: endpoint, Dialect: config.DialectTrino,
			Auth: config.AuthConfig{Mode: config.AuthPassthrough},
		}},
	}
	r, err := registry.New(cfg, registry.DefaultCredentialFactory, nil)
	require.NoError(t, err)
	return r
}

// authRoundTripper attaches a fixed Authorization header to every request, the
// way an agent passes its credential to the MCP HTTP endpoint.
type authRoundTripper struct {
	header string
	base   http.RoundTripper
}

func (a authRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if a.header != "" {
		req.Header.Set("Authorization", a.header)
	}
	return a.base.RoundTrip(req)
}

// startServer runs serveHTTP on an ephemeral listener and returns its base URL.
func startServer(t *testing.T, hc *config.HTTPConfig, reg *registry.Registry) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveHTTP(ctx, ln, hc, New(reg)) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			assert.NoError(t, err, "serveHTTP should drain cleanly on shutdown")
		case <-time.After(5 * time.Second):
			t.Error("serveHTTP did not return after context cancel")
		}
	})
	return "http://" + ln.Addr().String()
}

func mcpHTTPClient(t *testing.T, base, authHeader string) *mcp.ClientSession {
	t.Helper()
	transport := &mcp.StreamableClientTransport{
		Endpoint:   base,
		HTTPClient: &http.Client{Transport: authRoundTripper{header: authHeader, base: http.DefaultTransport}},
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	cs, err := client.Connect(context.Background(), transport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// TestServeHTTP_PassthroughEndToEnd drives the assembled server over real HTTP
// with a passthrough engine and asserts the caller's credential is forwarded.
func TestServeHTTP_PassthroughEndToEnd(t *testing.T) {
	eng := newRecordingEngine(t)
	base := startServer(t, &config.HTTPConfig{Host: "127.0.0.1"}, passthroughRegistry(t, eng.URL))

	// /healthz answers without auth.
	resp, err := http.Get(base + "/healthz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cs := mcpHTTPClient(t, base, "Bearer caller.jwt.token")
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "list_catalogs", Arguments: map[string]any{"engine": "e"},
	})
	require.NoError(t, err)
	require.False(t, res.IsError, "tool error: %+v", res.Content)

	gotAuth, _ := eng.seen()
	assert.Equal(t, "Bearer caller.jwt.token", gotAuth, "the caller's credential must be forwarded verbatim")
}

// TestServeHTTP_PassthroughMissingCredential proves a request with no credential
// is rejected (opaque passthrough still requires an authenticated caller).
func TestServeHTTP_PassthroughMissingCredential(t *testing.T) {
	eng := newRecordingEngine(t)
	base := startServer(t, &config.HTTPConfig{Host: "127.0.0.1"}, passthroughRegistry(t, eng.URL))

	cs := mcpHTTPClient(t, base, "") // no Authorization header
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "list_catalogs", Arguments: map[string]any{"engine": "e"},
	})
	require.NoError(t, err)
	assert.True(t, res.IsError, "a missing credential must produce a tool error")
}

// TestServeHTTP_EdgeAuth verifies the optional edge bearer verification: a token
// signed by the trusted key is accepted and its subject becomes the engine user;
// a request without a valid token is rejected at the edge (the MCP handshake fails).
func TestServeHTTP_EdgeAuth(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	dir := t.TempDir()
	pubPath := filepath.Join(dir, "jwt-pub.pem")
	require.NoError(t, os.WriteFile(pubPath, publicKeyPEM(t, key), 0o600))

	eng := newRecordingEngine(t)
	hc := &config.HTTPConfig{
		Host:     "127.0.0.1",
		EdgeAuth: &config.EdgeAuthConfig{Scheme: config.EdgeAuthJWTRS256, PublicKeyRef: "file://" + pubPath},
	}
	base := startServer(t, hc, passthroughRegistry(t, eng.URL))

	t.Run("valid token accepted and subject becomes the engine user", func(t *testing.T) {
		token := signJWT(t, key, "alice", time.Now().Add(time.Hour))
		cs := mcpHTTPClient(t, base, "Bearer "+token)
		res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
			Name: "list_catalogs", Arguments: map[string]any{"engine": "e"},
		})
		require.NoError(t, err)
		require.False(t, res.IsError, "tool error: %+v", res.Content)
		gotAuth, gotUser := eng.seen()
		assert.Equal(t, "Bearer "+token, gotAuth, "the original token is still forwarded")
		assert.Equal(t, "alice", gotUser, "the verified subject is sent as the engine user")
	})

	t.Run("untrusted token rejected at the edge", func(t *testing.T) {
		other, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		bad := signJWT(t, other, "mallory", time.Now().Add(time.Hour))
		transport := &mcp.StreamableClientTransport{
			Endpoint:   base,
			HTTPClient: &http.Client{Transport: authRoundTripper{header: "Bearer " + bad, base: http.DefaultTransport}},
		}
		client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
		_, err = client.Connect(context.Background(), transport, nil)
		require.Error(t, err, "the MCP handshake must fail when the edge rejects the token")
	})
}

func TestOriginGuard(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })

	t.Run("empty allowlist disables the check", func(t *testing.T) {
		g := originGuard(nil, next)
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("Origin", "https://evil.example")
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		assert.Equal(t, http.StatusTeapot, w.Code)
	})

	t.Run("allowed origin passes, disallowed is forbidden, missing is allowed", func(t *testing.T) {
		g := originGuard([]string{"https://good.example"}, next)

		cases := map[string]int{
			"https://good.example": http.StatusTeapot,
			"https://bad.example":  http.StatusForbidden,
			"":                     http.StatusTeapot,
		}
		for origin, want := range cases {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			if origin != "" {
				r.Header.Set("Origin", origin)
			}
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			assert.Equal(t, want, w.Code, "origin %q", origin)
		}
	})
}

func TestVerifyRS256(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	t.Run("valid", func(t *testing.T) {
		sub, _, err := verifyRS256(signJWT(t, key, "bob", time.Now().Add(time.Hour)), &key.PublicKey)
		require.NoError(t, err)
		assert.Equal(t, "bob", sub)
	})

	t.Run("expired", func(t *testing.T) {
		_, _, err := verifyRS256(signJWT(t, key, "bob", time.Now().Add(-time.Minute)), &key.PublicKey)
		assert.Error(t, err)
	})

	t.Run("wrong key", func(t *testing.T) {
		other, _ := rsa.GenerateKey(rand.Reader, 2048)
		_, _, err := verifyRS256(signJWT(t, key, "bob", time.Now().Add(time.Hour)), &other.PublicKey)
		assert.Error(t, err)
	})

	t.Run("malformed", func(t *testing.T) {
		_, _, err := verifyRS256("not-a-jwt", &key.PublicKey)
		assert.Error(t, err)
	})
}

func TestEdgeVerifier_Errors(t *testing.T) {
	_, err := edgeVerifier(&config.EdgeAuthConfig{Scheme: "bogus"})
	assert.Error(t, err)

	_, err = edgeVerifier(&config.EdgeAuthConfig{Scheme: config.EdgeAuthJWTRS256, PublicKeyRef: "file:///no/such/key"})
	assert.Error(t, err)
}

func TestTLSConfig(t *testing.T) {
	t.Run("no cert means plaintext", func(t *testing.T) {
		c, err := tlsConfig(&config.HTTPConfig{})
		require.NoError(t, err)
		assert.Nil(t, c)
	})

	t.Run("loads a cert/key pair from refs", func(t *testing.T) {
		dir := t.TempDir()
		certPath := filepath.Join(dir, "cert.pem")
		keyPath := filepath.Join(dir, "key.pem")
		certPEM, keyPEM := genCertKeyPEM(t)
		require.NoError(t, os.WriteFile(certPath, certPEM, 0o600))
		require.NoError(t, os.WriteFile(keyPath, keyPEM, 0o600))

		c, err := tlsConfig(&config.HTTPConfig{TLSCertRef: "file://" + certPath, TLSKeyRef: "file://" + keyPath})
		require.NoError(t, err)
		require.NotNil(t, c)
		assert.Len(t, c.Certificates, 1)
	})

	t.Run("bad ref errors", func(t *testing.T) {
		_, err := tlsConfig(&config.HTTPConfig{TLSCertRef: "file:///nope", TLSKeyRef: "file:///nope"})
		assert.Error(t, err)
	})
}

// --- test crypto helpers --------------------------------------------------

func signJWT(t *testing.T, key *rsa.PrivateKey, subject string, exp time.Time) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{"sub": subject, "iat": time.Now().Unix(), "exp": exp.Unix()})
	require.NoError(t, err)
	payload := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := header + "." + payload
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	require.NoError(t, err)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func publicKeyPEM(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func genCertKeyPEM(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}
