//go:build functional

// This file extends the functional suite to the enterprise deployment shape:
// the presto-mcp Docker image runs as a container alongside real Trino and
// Presto engines on a shared Docker network, and is driven over the
// streamable-HTTP transport by a real MCP HTTP client. It proves passthrough
// credentials end-to-end — the caller's bearer token is forwarded to the engine,
// a wrong token is rejected by the engine, and the server holds no identity of
// its own (a request with no credential is refused).
package functional

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/yabinma/presto-mcp/internal/config"
)

// TestEnterprisePassthrough runs the MCP image in enterprise mode against a
// JWT-secured engine and verifies passthrough for both dialects.
func TestEnterprisePassthrough(t *testing.T) {
	for _, e := range securedEngines() {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()

			net, err := tcnetwork.New(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = net.Remove(ctx) })

			// The engine trusts tokens signed by this key; the caller signs with it.
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)
			goodToken := signJWT(t, key, securedUser)

			alias := e.name // the engine's in-network hostname for the MCP
			e.startNetworked(t, net.Name, alias, key)

			mcpURL := startMCPContainer(t, net.Name, enterpriseConfig(e.dialect, alias))

			// With a valid forwarded credential, every read-only tool must work over
			// the HTTP transport exactly as it does locally — this mirrors the real
			// end-user flow (discover engines, browse metadata, profile a query, audit).
			t.Run("every read-only tool works with the forwarded credential", func(t *testing.T) {
				cs := mcpSession(t, mcpURL, "Bearer "+goodToken)

				var engines enginesOut
				callStructured(t, cs, "list_engines", nil, &engines)
				require.Len(t, engines.Engines, 1)
				assert.Equal(t, "e", engines.Engines[0].ID)

				var cats catalogsOut
				callStructured(t, cs, "list_catalogs", map[string]any{"engine": "e"}, &cats)
				assert.Contains(t, cats.Catalogs, "tpch")

				var schemas schemasOut
				callStructured(t, cs, "list_schemas", map[string]any{"engine": "e", "catalog": "tpch"}, &schemas)
				assert.Contains(t, schemas.Schemas, "tiny")

				var tables tablesOut
				callStructured(t, cs, "list_tables", map[string]any{"engine": "e", "catalog": "tpch", "schema": "tiny"}, &tables)
				assert.Contains(t, tables.Tables, "nation")

				var cols columnsOut
				callStructured(t, cs, "describe_table", map[string]any{"engine": "e", "catalog": "tpch", "schema": "tiny", "table": "nation"}, &cols)
				colTypes := make(map[string]string, len(cols.Columns))
				for _, c := range cols.Columns {
					colTypes[c.Name] = c.Type
				}
				assert.Subset(t, keys(colTypes), []string{"nationkey", "name", "regionkey", "comment"})

				var stats statsOut
				callStructured(t, cs, "get_table_stats", map[string]any{"engine": "e", "catalog": "tpch", "schema": "tiny", "table": "nation"}, &stats)
				require.NotNil(t, stats.Stats.RowCount)
				assert.EqualValues(t, 25, *stats.Stats.RowCount)

				var cluster clusterOut
				callStructured(t, cs, "get_cluster_info", map[string]any{"engine": "e"}, &cluster)
				assert.NotEmpty(t, cluster.Cluster.Version)
				assert.GreaterOrEqual(t, len(cluster.Cluster.Nodes), 1)

				var rq runQueryOut
				runQueryMCP(t, cs, "SELECT name FROM tpch.tiny.nation ORDER BY nationkey", &rq)
				require.Equal(t, 25, rq.RowCount)
				assert.Equal(t, "ALGERIA", rq.Rows[0][0])

				// Audit tools: the query we just ran is visible in coordinator memory.
				var queries queriesOut
				callStructured(t, cs, "list_queries", map[string]any{"engine": "e"}, &queries)
				assert.Equal(t, "live", queries.Source)
				require.NotEmpty(t, queries.Queries, "the coordinator should report recent queries")

				var detail queryDetailOut
				callStructured(t, cs, "get_query", map[string]any{"engine": "e", "query_id": queries.Queries[0].QueryID}, &detail)
				assert.Equal(t, queries.Queries[0].QueryID, detail.Summary.QueryID)
				assert.Contains(t, detail.AvailableSections, "summary")
			})

			t.Run("a token signed by an untrusted key is rejected by the engine", func(t *testing.T) {
				other, err := rsa.GenerateKey(rand.Reader, 2048)
				require.NoError(t, err)
				cs := mcpSession(t, mcpURL, "Bearer "+signJWT(t, other, securedUser))
				assertRejected(t, cs)
			})

			t.Run("no credential is refused (the server holds no identity)", func(t *testing.T) {
				cs := mcpSession(t, mcpURL, "")
				assertRejected(t, cs)
			})
		})
	}
}

// enterpriseConfig renders an MCP config that targets the engine over the shared
// network with passthrough auth and no static secret of its own.
func enterpriseConfig(dialect config.Dialect, engineAlias string) string {
	return fmt.Sprintf(`deployment_mode: enterprise
server:
  transport: http
  http:
    host: 0.0.0.0
    port: 8080
engines:
  - id: e
    endpoint: https://%s:8443
    dialect: %s
    auth:
      mode: passthrough
      user: %s
    tls_insecure_skip_verify: true
    history:
      enabled: false
`, engineAlias, dialect, securedUser)
}

// startNetworked launches a JWT-secured engine attached to the given network
// under alias, so the MCP container can reach it at https://<alias>:8443.
func (e securedEngine) startNetworked(t *testing.T, netName, alias string, key *rsa.PrivateKey) {
	t.Helper()
	ctx := context.Background()

	cfg := append([]string{}, e.baseConfig...)
	cfg = append(cfg,
		"http-server.https.enabled=true",
		"http-server.https.port=8443",
		"http-server.https.keystore.path="+e.etcDir+"/server.pem",
		"internal-communication.shared-secret="+randomHex(t, 24),
		"http-server.authentication.type=JWT",
		e.jwtKeyProp+"="+e.etcDir+"/jwt-key.pem",
	)

	files := []testcontainers.ContainerFile{
		{Reader: strings.NewReader(strings.Join(cfg, "\n") + "\n"), ContainerFilePath: e.etcDir + "/config.properties", FileMode: 0o644},
		{Reader: bytes.NewReader(genSelfSignedPEM(t)), ContainerFilePath: e.etcDir + "/server.pem", FileMode: 0o644},
		{Reader: bytes.NewReader(publicKeyPEM(t, key)), ContainerFilePath: e.etcDir + "/jwt-key.pem", FileMode: 0o644},
	}
	if e.tpchCatalog {
		files = append(files, testcontainers.ContainerFile{
			Reader: strings.NewReader("connector.name=tpch\n"), ContainerFilePath: e.etcDir + "/catalog/tpch.properties", FileMode: 0o644})
	}

	ws := wait.ForHTTP("/v1/info").
		WithPort("8443/tcp").
		WithTLS(true, &tls.Config{InsecureSkipVerify: true}). //nolint:gosec
		WithStartupTimeout(5 * time.Minute).
		WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }).
		WithResponseMatcher(func(body io.Reader) bool {
			b, _ := io.ReadAll(body)
			return strings.Contains(string(b), `"starting":false`)
		}).
		WithHeaders(map[string]string{"Authorization": "Bearer " + signJWT(t, key, securedUser)})

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: e.image, ExposedPorts: []string{"8443/tcp"}, Files: files, WaitingFor: ws,
			Networks: []string{netName}, NetworkAliases: map[string][]string{netName: {alias}},
		},
		Started: true,
	})
	if err != nil {
		if c != nil {
			dumpLogs(t, c)
		}
		t.Fatalf("start networked %s: %v", e.name, err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t, c)
		}
		_ = c.Terminate(ctx)
	})
}

// startMCPContainer builds the presto-mcp image and runs it on netName with the
// given config mounted, returning the host base URL of its HTTP endpoint.
func startMCPContainer(t *testing.T, netName, configYAML string) string {
	t.Helper()
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../..",
			Dockerfile: "Dockerfile",
			Repo:       "presto-mcp-functest",
			Tag:        "latest",
			KeepImage:  true, // reuse across subtests; layers are cached
		},
		ExposedPorts: []string{"8080/tcp"},
		Networks:     []string{netName},
		Files: []testcontainers.ContainerFile{{
			Reader: strings.NewReader(configYAML), ContainerFilePath: "/etc/presto-mcp/config.yaml", FileMode: 0o644,
		}},
		WaitingFor: wait.ForHTTP("/healthz").WithPort("8080/tcp").
			WithStartupTimeout(90 * time.Second).
			WithStatusCodeMatcher(func(status int) bool { return status == http.StatusOK }),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req, Started: true,
	})
	if err != nil {
		if c != nil {
			dumpLogs(t, c)
		}
		t.Fatalf("start mcp container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	require.NoError(t, err)
	port, err := c.MappedPort(ctx, "8080")
	require.NoError(t, err)
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// mcpSession connects a real MCP HTTP client to the server, attaching authHeader
// (if non-empty) as the Authorization header on every request — i.e. the caller
// credential the enterprise shape forwards to the engine.
func mcpSession(t *testing.T, base, authHeader string) *mcp.ClientSession {
	t.Helper()
	transport := &mcp.StreamableClientTransport{
		Endpoint:   base,
		HTTPClient: &http.Client{Transport: bearerTransport{header: authHeader, base: http.DefaultTransport}},
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "enterprise-func-test", Version: "0"}, nil)
	cs, err := client.Connect(context.Background(), transport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// runQueryMCP drives the run_query tool, decoding the result into out. It
// retries the engine's "No nodes available" startup race (the coordinator
// reports ready before a worker registers) the same way the direct runQuery
// helper does, since an actual query — unlike coordinator-only metadata — needs
// a worker node.
func runQueryMCP(t *testing.T, cs *mcp.ClientSession, sql string, out *runQueryOut) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{
			Name: "run_query", Arguments: map[string]any{"engine": "e", "sql": sql},
		})
		require.NoError(t, err)
		if !res.IsError {
			b, err := json.Marshal(res.StructuredContent)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(b, out))
			return
		}
		msg := toolErrorText(res)
		if time.Now().Before(deadline) && strings.Contains(msg, "No nodes available") {
			time.Sleep(2 * time.Second)
			continue
		}
		t.Fatalf("run_query failed: %s", msg)
	}
}

func toolErrorText(res *mcp.CallToolResult) string {
	var sb strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			sb.WriteString(tc.Text)
		}
	}
	return sb.String()
}

type bearerTransport struct {
	header string
	base   http.RoundTripper
}

func (b bearerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if b.header != "" {
		req.Header.Set("Authorization", b.header)
	}
	return b.base.RoundTrip(req)
}

func asInt(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	default:
		return 0
	}
}
