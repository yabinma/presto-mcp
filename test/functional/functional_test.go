//go:build functional

// Package functional drives the assembled presto-mcp server, in local
// deployment mode, against real Trino and Presto engines started with
// testcontainers. It exercises every read-only tool end-to-end through an
// in-memory MCP client and asserts the tools behave as expected for both
// dialects.
//
// These tests require Docker and are gated behind the `functional` build tag, so
// `go test ./...` does not run them and they do not count toward the unit
// coverage gate. Run with: go test -tags functional ./test/functional/...
package functional

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/crypto/bcrypt"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
	"github.com/yabinma/presto-mcp/internal/registry"
	"github.com/yabinma/presto-mcp/internal/server"
)

type engineSpec struct {
	name    string
	dialect config.Dialect
	image   string
	files   []testcontainers.ContainerFile
}

func specs() []engineSpec {
	return []engineSpec{
		{
			name:    "trino",
			dialect: config.DialectTrino,
			image:   "trinodb/trino:latest", // tpch catalog is enabled by default
		},
		{
			name:    "presto",
			dialect: config.DialectPresto,
			image:   "prestodb/presto:latest",
			// Ensure the tpch catalog exists.
			files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader("connector.name=tpch\n"),
				ContainerFilePath: "/opt/presto-server/etc/catalog/tpch.properties",
				FileMode:          0o644,
			}},
		},
	}
}

func TestToolsAgainstEngines(t *testing.T) {
	for _, spec := range specs() {
		t.Run(spec.name, func(t *testing.T) {
			t.Parallel()
			endpoint := startEngine(t, spec)
			runToolTests(t, spec, endpoint)
		})
	}
}

func startEngine(t *testing.T, spec engineSpec) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        spec.image,
		ExposedPorts: []string{"8080/tcp"},
		Files:        spec.files,
		WaitingFor: wait.ForHTTP("/v1/info").
			WithPort("8080/tcp").
			WithStartupTimeout(5 * time.Minute).
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
			WithResponseMatcher(func(body io.Reader) bool {
				b, _ := io.ReadAll(body)
				return strings.Contains(string(b), `"starting":false`)
			}),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "start %s container", spec.name)
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "8080")
	require.NoError(t, err)
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

func runToolTests(t *testing.T, spec engineSpec, endpoint string) {
	ec := config.EngineConfig{
		ID: "e", Endpoint: endpoint, Dialect: spec.dialect,
		Auth: config.AuthConfig{Mode: config.AuthStatic, User: "test"},
	}
	reg, err := registry.New(singleEngine(ec), registry.DefaultCredentialFactory, nil)
	require.NoError(t, err)
	cs := connect(t, server.New(reg))

	t.Run("list_engines", func(t *testing.T) {
		var out enginesOut
		callStructured(t, cs, "list_engines", nil, &out)
		require.Len(t, out.Engines, 1)
		assert.Equal(t, "e", out.Engines[0].ID)
	})

	t.Run("list_catalogs", func(t *testing.T) {
		var out catalogsOut
		callStructured(t, cs, "list_catalogs", map[string]any{"engine": "e"}, &out)
		assert.Contains(t, out.Catalogs, "tpch")
	})

	t.Run("list_schemas", func(t *testing.T) {
		var out schemasOut
		callStructured(t, cs, "list_schemas", map[string]any{"engine": "e", "catalog": "tpch"}, &out)
		assert.Contains(t, out.Schemas, "tiny")
	})

	t.Run("list_tables", func(t *testing.T) {
		var out tablesOut
		callStructured(t, cs, "list_tables", map[string]any{"engine": "e", "catalog": "tpch", "schema": "tiny"}, &out)
		assert.Contains(t, out.Tables, "nation")
	})

	t.Run("describe_table", func(t *testing.T) {
		var out columnsOut
		callStructured(t, cs, "describe_table", map[string]any{"engine": "e", "catalog": "tpch", "schema": "tiny", "table": "nation"}, &out)
		// tpch.nation is a known schema: nationkey(bigint), name(varchar),
		// regionkey(bigint), comment(varchar) — assert both names and types.
		types := make(map[string]string, len(out.Columns))
		for _, c := range out.Columns {
			types[c.Name] = c.Type
		}
		assert.Subset(t, keys(types), []string{"nationkey", "name", "regionkey", "comment"})
		assert.Contains(t, types["nationkey"], "bigint")
		assert.Contains(t, types["regionkey"], "bigint")
		assert.Contains(t, types["name"], "varchar")
	})

	t.Run("get_table_stats", func(t *testing.T) {
		var out statsOut
		callStructured(t, cs, "get_table_stats", map[string]any{"engine": "e", "catalog": "tpch", "schema": "tiny", "table": "nation"}, &out)
		// tpch.nation always has exactly 25 rows.
		require.NotNil(t, out.Stats.RowCount, "expected a row count from SHOW STATS")
		assert.EqualValues(t, 25, *out.Stats.RowCount)
		assert.NotEmpty(t, out.Stats.Columns, "expected per-column stats")
	})

	t.Run("get_cluster_info", func(t *testing.T) {
		var out clusterOut
		callStructured(t, cs, "get_cluster_info", map[string]any{"engine": "e"}, &out)
		assert.NotEmpty(t, out.Cluster.Version)
		assert.GreaterOrEqual(t, len(out.Cluster.Nodes), 1, "expected at least the coordinator node")
	})

	// Generate a query so the coordinator has something to report, then audit it.
	queryID := runQuery(t, queryConn{endpoint: endpoint, dialect: spec.dialect, user: "test"},
		"SELECT count(*) FROM tpch.tiny.nation")

	t.Run("list_queries", func(t *testing.T) {
		var out queriesOut
		callStructured(t, cs, "list_queries", map[string]any{"engine": "e"}, &out)
		assert.Equal(t, "live", out.Source)
		ids := make([]string, 0, len(out.Queries))
		for _, q := range out.Queries {
			ids = append(ids, q.QueryID)
		}
		assert.Contains(t, ids, queryID)
	})

	t.Run("get_query", func(t *testing.T) {
		var out queryDetailOut
		callStructured(t, cs, "get_query", map[string]any{"engine": "e", "query_id": queryID}, &out)
		assert.Equal(t, "live", out.Source)
		assert.Equal(t, queryID, out.Summary.QueryID)
		// runQuery drove the statement to completion, so the coordinator reports it finished.
		assert.Equal(t, "FINISHED", out.Summary.State)
		assert.Contains(t, out.AvailableSections, "summary")
		// A real query has measurable wall time.
		assert.Positive(t, out.Summary.ElapsedMillis)
	})

	t.Run("get_query_raw", func(t *testing.T) {
		var out struct {
			Raw string `json:"raw"`
		}
		callStructured(t, cs, "get_query", map[string]any{"engine": "e", "query_id": queryID, "raw": true}, &out)
		// The raw fragment is the engine's own JSON and must reference this query.
		assert.True(t, json.Valid([]byte(out.Raw)), "raw should be valid JSON")
		assert.Contains(t, out.Raw, queryID)
	})

	t.Run("run_query_select", func(t *testing.T) {
		var out runQueryOut
		callStructured(t, cs, "run_query", map[string]any{
			"engine": "e", "sql": "SELECT name FROM tpch.tiny.nation ORDER BY nationkey",
		}, &out)
		require.Len(t, out.Columns, 1)
		assert.Equal(t, "name", out.Columns[0].Name)
		assert.Contains(t, out.Columns[0].Type, "varchar")
		require.Equal(t, 25, out.RowCount, "tpch.tiny.nation has 25 rows")
		assert.False(t, out.Truncated)
		// nationkey 0 is ALGERIA, so the first row by nationkey is deterministic.
		require.NotEmpty(t, out.Rows)
		assert.Equal(t, "ALGERIA", out.Rows[0][0])
	})

	t.Run("run_query_show", func(t *testing.T) {
		var out runQueryOut
		callStructured(t, cs, "run_query", map[string]any{"engine": "e", "sql": "SHOW SCHEMAS FROM tpch"}, &out)
		schemas := make([]string, 0, len(out.Rows))
		for _, r := range out.Rows {
			if s, ok := r[0].(string); ok {
				schemas = append(schemas, s)
			}
		}
		assert.Contains(t, schemas, "tiny")
	})

	t.Run("run_query_truncates", func(t *testing.T) {
		var out runQueryOut
		callStructured(t, cs, "run_query", map[string]any{
			"engine": "e", "sql": "SELECT * FROM tpch.tiny.nation", "max_rows": 1,
		}, &out)
		assert.Equal(t, 1, out.RowCount)
		assert.True(t, out.Truncated, "more rows were available than the cap allowed")
	})

	t.Run("run_query_rejects_write", func(t *testing.T) {
		// A write must be refused by the guard before it ever reaches the engine.
		res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
			Name:      "run_query",
			Arguments: map[string]any{"engine": "e", "sql": "CREATE TABLE tpch.tiny.x AS SELECT 1"},
		})
		require.NoError(t, err)
		assert.True(t, res.IsError, "a write statement must produce a tool error")
	})
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

const securedUser = "analyst"

// securedEngine knows how to start one dialect with TLS + an authenticator.
type securedEngine struct {
	name        string
	dialect     config.Dialect
	image       string
	etcDir      string   // config directory inside the container
	baseConfig  []string // dialect-specific base config.properties lines
	tpchCatalog bool     // mount a tpch catalog (Presto's catalog dir is empty)
	jwtKeyProp  string   // property naming the JWT verification key (differs by engine)
}

func securedEngines() []securedEngine {
	return []securedEngine{
		{
			name: "trino", dialect: config.DialectTrino, image: "trinodb/trino:latest",
			etcDir:     "/etc/trino",
			jwtKeyProp: "http-server.authentication.jwt.key-file",
			baseConfig: []string{
				"coordinator=true",
				"node-scheduler.include-coordinator=true",
				"http-server.http.port=8080",
				"discovery.uri=http://localhost:8080",
			},
		},
		{
			name: "presto", dialect: config.DialectPresto, image: "prestodb/presto:latest",
			etcDir: "/opt/presto-server/etc", tpchCatalog: true,
			jwtKeyProp: "http.authentication.jwt.key-file", // PrestoDB diverges from Trino here
			baseConfig: []string{
				"coordinator=true",
				"node-scheduler.include-coordinator=true",
				"discovery-server.enabled=true",
				"http-server.http.port=8080",
				"discovery.uri=http://localhost:8080",
			},
		},
	}
}

// TestSecuredBasicAuth verifies the basic (username/password) scheme end-to-end
// over TLS against both Trino and Presto with the file password authenticator,
// including a negative check that a wrong password is rejected.
func TestSecuredBasicAuth(t *testing.T) {
	const pass = "sup3r-secret"
	for _, e := range securedEngines() {
		t.Run(e.name, func(t *testing.T) {
			authFiles := []testcontainers.ContainerFile{
				{Reader: strings.NewReader("password-authenticator.name=file\nfile.password-file=" + e.etcDir + "/password.db\n"),
					ContainerFilePath: e.etcDir + "/password-authenticator.properties", FileMode: 0o644},
				{Reader: bytes.NewReader(genPasswordDB(t, securedUser, pass)),
					ContainerFilePath: e.etcDir + "/password.db", FileMode: 0o644},
			}
			endpoint := e.start(t, []string{"http-server.authentication.type=PASSWORD"}, authFiles,
				func(ws *wait.HTTPStrategy) { ws.WithBasicAuth(securedUser, pass) })

			// Positive: real config chain DefaultCredentialFactory -> NewBasic -> env://.
			t.Setenv("SECURED_PW", pass)
			ec := config.EngineConfig{
				ID: "e", Endpoint: endpoint, Dialect: e.dialect,
				Auth:                  config.AuthConfig{Mode: config.AuthStatic, Scheme: config.SchemeBasic, User: securedUser, PasswordRef: "env://SECURED_PW"},
				TLSInsecureSkipVerify: true,
			}
			reg, err := registry.New(singleEngine(ec), registry.DefaultCredentialFactory, nil)
			require.NoError(t, err)
			assertSecuredTools(t, connect(t, server.New(reg)),
				queryConn{endpoint: endpoint, dialect: e.dialect, user: securedUser, password: pass, insecureTLS: true})

			// Negative: a wrong password must be rejected by the engine.
			badReg, err := registry.New(singleEngine(ec), basicFactory(securedUser, "wrong-password"), nil)
			require.NoError(t, err)
			assertRejected(t, connect(t, server.New(badReg)))
		})
	}
}

// TestSecuredJWTAuth verifies the bearer (JWT) scheme end-to-end over TLS against
// both engines: a token signed by the key the engine trusts is accepted, and one
// signed by a different key is rejected.
func TestSecuredJWTAuth(t *testing.T) {
	for _, e := range securedEngines() {
		t.Run(e.name, func(t *testing.T) {
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)
			token := signJWT(t, key, securedUser)

			authFiles := []testcontainers.ContainerFile{
				{Reader: bytes.NewReader(publicKeyPEM(t, key)), ContainerFilePath: e.etcDir + "/jwt-key.pem", FileMode: 0o644},
			}
			endpoint := e.start(t, []string{
				"http-server.authentication.type=JWT",
				e.jwtKeyProp + "=" + e.etcDir + "/jwt-key.pem",
			}, authFiles, func(ws *wait.HTTPStrategy) {
				ws.WithHeaders(map[string]string{"Authorization": "Bearer " + token})
			})

			// Positive: bearer token via the real config chain (env:// -> Bearer).
			t.Setenv("SECURED_JWT", token)
			ec := config.EngineConfig{
				ID: "e", Endpoint: endpoint, Dialect: e.dialect,
				Auth:                  config.AuthConfig{Mode: config.AuthStatic, Scheme: config.SchemeBearer, User: securedUser, CredentialRef: "env://SECURED_JWT"},
				TLSInsecureSkipVerify: true,
			}
			reg, err := registry.New(singleEngine(ec), registry.DefaultCredentialFactory, nil)
			require.NoError(t, err)
			assertSecuredTools(t, connect(t, server.New(reg)),
				queryConn{endpoint: endpoint, dialect: e.dialect, user: securedUser, bearer: token, insecureTLS: true})

			// Negative: a token signed with an untrusted key must be rejected.
			otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)
			badReg, err := registry.New(singleEngine(ec), bearerFactory(securedUser, signJWT(t, otherKey, securedUser)), nil)
			require.NoError(t, err)
			assertRejected(t, connect(t, server.New(badReg)))
		})
	}
}

// assertSecuredTools runs representative read-only tools over a secured engine.
func assertSecuredTools(t *testing.T, cs *mcp.ClientSession, q queryConn) {
	t.Helper()
	var cats catalogsOut
	callStructured(t, cs, "list_catalogs", map[string]any{"engine": "e"}, &cats)
	assert.Contains(t, cats.Catalogs, "tpch")

	var tables tablesOut
	callStructured(t, cs, "list_tables", map[string]any{"engine": "e", "catalog": "tpch", "schema": "tiny"}, &tables)
	assert.Contains(t, tables.Tables, "nation")

	qid := runQuery(t, q, "SELECT count(*) FROM tpch.tiny.nation")
	var detail queryDetailOut
	callStructured(t, cs, "get_query", map[string]any{"engine": "e", "query_id": qid}, &detail)
	assert.Equal(t, qid, detail.Summary.QueryID)
	assert.Equal(t, "FINISHED", detail.Summary.State)
}

// assertRejected confirms an engine rejects a bad credential (auth is enforced).
func assertRejected(t *testing.T, cs *mcp.ClientSession) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
		Name: "list_catalogs", Arguments: map[string]any{"engine": "e"},
	})
	require.NoError(t, err)
	assert.True(t, res.IsError, "a bad credential must produce a tool error (engine enforces auth)")
}

// --- MCP / engine helpers -------------------------------------------------

func singleEngine(ec config.EngineConfig) *config.Config {
	return &config.Config{
		DeploymentMode: config.ModeLocal,
		Server:         config.ServerConfig{Transport: config.TransportStdio},
		Engines:        []config.EngineConfig{ec},
	}
}

// connect wires an in-memory MCP client to the server and returns the session.
func connect(t *testing.T, srv *mcp.Server) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	serverT, clientT := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, serverT, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })

	client := mcp.NewClient(&mcp.Implementation{Name: "func-test", Version: "0"}, nil)
	cs, err := client.Connect(ctx, clientT, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// basicFactory builds a credential factory that hands out a fixed basic-auth
// password (used to inject a deliberately wrong password).
func basicFactory(user, password string) registry.CredentialFactory {
	return func(config.EngineConfig) (credential.Provider, error) {
		return credential.NewBasic(user, "literal", func(string) (string, error) { return password, nil })
	}
}

// start launches the engine with TLS plus the given authenticator config lines
// and files, waiting for readiness via the (authenticated) probe customizer.
func (e securedEngine) start(t *testing.T, authConfig []string, authFiles []testcontainers.ContainerFile, probe func(*wait.HTTPStrategy)) string {
	t.Helper()
	ctx := context.Background()

	cfg := append([]string{}, e.baseConfig...)
	cfg = append(cfg,
		"http-server.https.enabled=true",
		"http-server.https.port=8443",
		"http-server.https.keystore.path="+e.etcDir+"/server.pem",
		"internal-communication.shared-secret="+randomHex(t, 24),
	)
	cfg = append(cfg, authConfig...)

	files := []testcontainers.ContainerFile{
		{Reader: strings.NewReader(strings.Join(cfg, "\n") + "\n"), ContainerFilePath: e.etcDir + "/config.properties", FileMode: 0o644},
		{Reader: bytes.NewReader(genSelfSignedPEM(t)), ContainerFilePath: e.etcDir + "/server.pem", FileMode: 0o644},
	}
	if e.tpchCatalog {
		files = append(files, testcontainers.ContainerFile{
			Reader: strings.NewReader("connector.name=tpch\n"), ContainerFilePath: e.etcDir + "/catalog/tpch.properties", FileMode: 0o644})
	}
	files = append(files, authFiles...)

	ws := wait.ForHTTP("/v1/info").
		WithPort("8443/tcp").
		WithTLS(true, &tls.Config{InsecureSkipVerify: true}).
		WithStartupTimeout(5 * time.Minute).
		WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
		WithResponseMatcher(func(body io.Reader) bool {
			b, _ := io.ReadAll(body)
			return strings.Contains(string(b), `"starting":false`)
		})
	probe(ws)

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: e.image, ExposedPorts: []string{"8443/tcp"}, Files: files, WaitingFor: ws,
		},
		Started: true,
	})
	if err != nil {
		if c != nil {
			dumpLogs(t, c)
		}
		t.Fatalf("start secured %s: %v", e.name, err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "8443")
	require.NoError(t, err)
	return fmt.Sprintf("https://%s:%s", host, port.Port())
}

// bearerFactory builds a credential factory yielding a fixed bearer token.
func bearerFactory(user, token string) registry.CredentialFactory {
	return func(config.EngineConfig) (credential.Provider, error) {
		return credential.NewStatic(user, "literal", func(string) (string, error) { return token, nil })
	}
}

// signJWT returns an RS256-signed JWT with the given subject (no external dep).
func signJWT(t *testing.T, key *rsa.PrivateKey, subject string) string {
	t.Helper()
	now := time.Now()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	claims, err := json.Marshal(map[string]any{"sub": subject, "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	require.NoError(t, err)
	payload := base64.RawURLEncoding.EncodeToString(claims)
	signingInput := header + "." + payload
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	require.NoError(t, err)
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// publicKeyPEM returns the PEM-encoded public key the engine uses to verify JWTs.
func publicKeyPEM(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

// dumpLogs prints the tail of a container's logs to aid debugging on failure.
func dumpLogs(t *testing.T, c testcontainers.Container) {
	r, err := c.Logs(context.Background())
	if err != nil {
		return
	}
	defer func() { _ = r.Close() }()
	b, _ := io.ReadAll(r)
	s := string(b)
	if len(s) > 4000 {
		s = s[len(s)-4000:]
	}
	t.Logf("container logs (tail):\n%s", s)
}

// genSelfSignedPEM returns a PEM bundle (private key + certificate) for the
// engine's PEM keystore, valid for localhost / 127.0.0.1 and the container
// network aliases used by the enterprise suite ("trino", "presto").
//
// The aliases matter because newer engine images (e.g. prestodb/presto:latest)
// bundle a Jetty with SNI host checking enabled: on an HTTPS connection it
// rejects (400) any request whose SNI/Host is not present in the server
// certificate. The local suite connects to 127.0.0.1, but the enterprise suite's
// containerized client connects to https://<alias>:8443, so the alias must be in
// the cert's SAN. (A real deployment uses a cert covering the engine's hostname.)
func genSelfSignedPEM(t *testing.T) []byte {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{"localhost", "trino", "presto"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, pem.Encode(&buf, &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	require.NoError(t, pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: der}))
	return buf.Bytes()
}

// genPasswordDB returns a Trino file-authenticator entry: "user:<bcrypt>". Trino
// expects the $2y$ bcrypt variant (functionally identical to Go's $2a$).
func genPasswordDB(t *testing.T, user, pass string) []byte {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(pass), bcrypt.DefaultCost)
	require.NoError(t, err)
	hash = bytes.Replace(hash, []byte("$2a$"), []byte("$2y$"), 1)
	return []byte(fmt.Sprintf("%s:%s\n", user, hash))
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return fmt.Sprintf("%x", b)
}

func callStructured(t *testing.T, cs *mcp.ClientSession, name string, args map[string]any, out any) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	require.NoError(t, err)
	require.False(t, res.IsError, "%s returned a tool error: %+v", name, res.Content)
	b, err := json.Marshal(res.StructuredContent)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, out))
}

// queryConn describes how to reach an engine's statement endpoint directly.
type queryConn struct {
	endpoint    string
	dialect     config.Dialect
	user        string
	password    string // non-empty => HTTP Basic auth
	bearer      string // non-empty => Authorization: Bearer
	insecureTLS bool   // skip cert verification (self-signed test certs)
}

func (q queryConn) prefix() string {
	if q.dialect == config.DialectPresto {
		return "X-Presto-"
	}
	return "X-Trino-"
}

func (q queryConn) httpClient() *http.Client {
	c := &http.Client{Timeout: 30 * time.Second}
	if q.insecureTLS {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
		c.Transport = tr
	}
	return c
}

func (q queryConn) auth(req *http.Request) {
	req.Header.Set(q.prefix()+"User", q.user)
	switch {
	case q.bearer != "":
		req.Header.Set("Authorization", "Bearer "+q.bearer)
	case q.password != "":
		req.SetBasicAuth(q.user, q.password)
	}
}

// runQuery drives the engine's statement protocol directly (read-only SELECT)
// and returns the query id, so the audit tools have a real query to find. It
// retries the "No nodes available" startup race (the engine reports ready via
// /v1/info before a worker node has registered).
func runQuery(t *testing.T, q queryConn, sql string) string {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		id, err := q.execute(sql)
		if err == nil {
			require.NotEmpty(t, id)
			return id
		}
		if time.Now().Before(deadline) && strings.Contains(err.Error(), "No nodes available") {
			time.Sleep(2 * time.Second)
			continue
		}
		require.NoError(t, err, "runQuery")
	}
}

// execute runs one statement to completion, returning the query id or an error.
func (q queryConn) execute(sql string) (string, error) {
	httpc := q.httpClient()
	req, err := http.NewRequest(http.MethodPost, q.endpoint+"/v1/statement", strings.NewReader(sql))
	if err != nil {
		return "", err
	}
	q.auth(req)
	req.Header.Set(q.prefix()+"Source", "func-test")
	req.Header.Set("Content-Type", "text/plain")

	var queryID string
	for {
		resp, err := httpc.Do(req)
		if err != nil {
			return "", err
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			return "", fmt.Errorf("statement status %d: %s", resp.StatusCode, body)
		}
		var sr struct {
			ID      string `json:"id"`
			NextURI string `json:"nextUri"`
			Error   *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(body, &sr); err != nil {
			return "", err
		}
		if sr.Error != nil {
			return "", fmt.Errorf("%s", sr.Error.Message)
		}
		if sr.ID != "" {
			queryID = sr.ID
		}
		if sr.NextURI == "" {
			return queryID, nil
		}
		req, err = http.NewRequest(http.MethodGet, sr.NextURI, nil)
		if err != nil {
			return "", err
		}
		q.auth(req)
	}
}

// --- decoded tool outputs (mirror the JSON the tools emit) ----------------

type enginesOut struct {
	Engines []struct {
		ID string `json:"id"`
	} `json:"engines"`
}

type catalogsOut struct {
	Catalogs []string `json:"catalogs"`
}

type schemasOut struct {
	Schemas []string `json:"schemas"`
}

type tablesOut struct {
	Tables []string `json:"tables"`
}

type columnsOut struct {
	Columns []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"columns"`
}

type statsOut struct {
	Stats struct {
		RowCount *int64 `json:"row_count"`
		Columns  []struct {
			Column string `json:"column"`
		} `json:"columns"`
	} `json:"stats"`
}

type clusterOut struct {
	Cluster struct {
		Version string `json:"version"`
		Nodes   []struct {
			NodeID string `json:"node_id"`
		} `json:"nodes"`
	} `json:"cluster"`
}

type queriesOut struct {
	Source  string `json:"source"`
	Queries []struct {
		QueryID string `json:"query_id"`
	} `json:"queries"`
}

type runQueryOut struct {
	Columns []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"columns"`
	Rows      [][]any `json:"rows"`
	RowCount  int     `json:"row_count"`
	Truncated bool    `json:"truncated"`
}

type queryDetailOut struct {
	Source            string   `json:"source"`
	AvailableSections []string `json:"available_sections"`
	Summary           struct {
		QueryID       string  `json:"query_id"`
		State         string  `json:"state"`
		ElapsedMillis float64 `json:"elapsed_ms"`
	} `json:"summary"`
}
