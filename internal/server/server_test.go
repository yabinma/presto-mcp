package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/registry"
)

func fakeEngine(t *testing.T) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/statement", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"columns":[{"name":"Catalog"}],"data":[["hive"],["system"]]}`)
	})
	s := httptest.NewServer(mux)
	t.Cleanup(s.Close)
	return s
}

func testRegistry(t *testing.T, url string) *registry.Registry {
	cfg := &config.Config{
		DeploymentMode: config.ModeLocal,
		Engines: []config.EngineConfig{{
			ID: "e", Endpoint: url, Dialect: config.DialectTrino,
			Auth: config.AuthConfig{Mode: config.AuthStatic, User: "u"},
		}},
	}
	r, err := registry.New(cfg, registry.DefaultCredentialFactory, nil)
	require.NoError(t, err)
	return r
}

// TestServerRoundTrip drives the server through a real MCP client over the
// in-memory transport. This also validates that every tool's input/output
// schema infers and validates (AddTool would otherwise panic).
func TestServerRoundTrip(t *testing.T) {
	eng := fakeEngine(t)
	srv := New(testRegistry(t, eng.URL))

	ctx := context.Background()
	serverT, clientT := mcp.NewInMemoryTransports()
	ss, err := srv.Connect(ctx, serverT, nil)
	require.NoError(t, err)
	defer ss.Close()

	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "0"}, nil)
	cs, err := client.Connect(ctx, clientT, nil)
	require.NoError(t, err)
	defer cs.Close()

	// All 10 read-only tools are exposed.
	tools, err := cs.ListTools(ctx, nil)
	require.NoError(t, err)
	names := make([]string, 0, len(tools.Tools))
	for _, tl := range tools.Tools {
		names = append(names, tl.Name)
	}
	assert.ElementsMatch(t, []string{
		"list_engines", "list_catalogs", "list_schemas", "list_tables",
		"describe_table", "get_table_stats", "get_cluster_info",
		"list_queries", "get_query", "run_query",
	}, names)

	// list_engines
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "list_engines"})
	require.NoError(t, err)
	assert.False(t, res.IsError)
	assert.Contains(t, structured(t, res), `"id":"e"`)

	// list_catalogs against the fake engine
	res, err = cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "list_catalogs",
		Arguments: map[string]any{"engine": "e"},
	})
	require.NoError(t, err)
	assert.False(t, res.IsError)
	assert.Contains(t, structured(t, res), "hive")

	// unknown engine -> tool error surfaced to the client
	res, err = cs.CallTool(ctx, &mcp.CallToolParams{
		Name:      "list_catalogs",
		Arguments: map[string]any{"engine": "ghost"},
	})
	require.NoError(t, err)
	assert.True(t, res.IsError)
}

func structured(t *testing.T, res *mcp.CallToolResult) string {
	t.Helper()
	b, err := json.Marshal(res.StructuredContent)
	require.NoError(t, err)
	return string(b)
}

func TestRun_HTTPListenError(t *testing.T) {
	// Occupy a port, then ask Run to bind the same one: the listen must fail.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)

	cfg := &config.Config{
		DeploymentMode: config.ModeEnterprise,
		Server: config.ServerConfig{Transport: config.TransportHTTP,
			HTTP: &config.HTTPConfig{Host: "127.0.0.1", Port: port}},
		Engines: []config.EngineConfig{{
			ID: "e", Endpoint: "http://h:8080", Dialect: config.DialectTrino,
			Auth: config.AuthConfig{Mode: config.AuthPassthrough},
		}},
	}
	err = Run(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listen")
}

func TestRun_RegistryError(t *testing.T) {
	cfg := &config.Config{
		DeploymentMode: config.ModeLocal,
		Server:         config.ServerConfig{Transport: config.TransportStdio},
		Engines: []config.EngineConfig{{
			ID: "e", Endpoint: "http://h:8080", Dialect: config.DialectWxd,
			Auth: config.AuthConfig{Mode: config.AuthStatic, User: "u"},
		}},
	}
	err := Run(context.Background(), cfg)
	require.Error(t, err)
}

func TestRun_UnknownTransport(t *testing.T) {
	cfg := &config.Config{
		DeploymentMode: config.ModeLocal,
		Server:         config.ServerConfig{Transport: "carrier-pigeon"},
		Engines: []config.EngineConfig{{
			ID: "e", Endpoint: "http://h:8080", Dialect: config.DialectTrino,
			Auth: config.AuthConfig{Mode: config.AuthStatic, User: "u"},
		}},
	}
	err := Run(context.Background(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown transport")
}

func TestNew_NotNil(t *testing.T) {
	eng := fakeEngine(t)
	assert.NotNil(t, New(testRegistry(t, eng.URL)))
	assert.True(t, strings.HasPrefix(Version, "0."))
}
