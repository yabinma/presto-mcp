package presto

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
)

// fakeCred is a controllable credential provider.
type fakeCred struct {
	cred credential.Credential
	err  error
}

func (f fakeCred) Resolve(context.Context) (credential.Credential, error) {
	return f.cred, f.err
}

// fakeEngine is an httptest server that speaks a small slice of the Presto/Trino
// REST protocol.
type fakeEngine struct {
	*httptest.Server
	mu          sync.Mutex
	lastHeaders http.Header
	status      int // override status for the statement endpoint when non-zero
	badJSON     bool
	stmtCalls   int // POSTs to /v1/statement
	cancels     int // DELETEs (statement cancellation)
}

func newFakeEngine(tb testing.TB) *fakeEngine {
	fe := &fakeEngine{}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/statement", fe.handleStatement)
	mux.HandleFunc("/v1/statement/next", fe.handleNext)
	mux.HandleFunc("/v1/info", fe.handleInfo)
	mux.HandleFunc("/v1/query", fe.handleQueryList)
	mux.HandleFunc("/v1/query/", fe.handleQueryDetail)
	fe.Server = httptest.NewServer(fe.record(mux))
	tb.Cleanup(fe.Close)
	return fe
}

func (fe *fakeEngine) record(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fe.mu.Lock()
		fe.lastHeaders = r.Header.Clone()
		if r.Method == http.MethodDelete {
			fe.cancels++
		}
		fe.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func (fe *fakeEngine) headers() http.Header {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	return fe.lastHeaders
}

func (fe *fakeEngine) counts() (stmtCalls, cancels int) {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	return fe.stmtCalls, fe.cancels
}

func (fe *fakeEngine) handleStatement(w http.ResponseWriter, r *http.Request) {
	fe.mu.Lock()
	fe.stmtCalls++
	fe.mu.Unlock()
	if fe.status != 0 {
		w.WriteHeader(fe.status)
		fmt.Fprint(w, "boom")
		return
	}
	if fe.badJSON {
		fmt.Fprint(w, "{not-json")
		return
	}
	body, _ := io.ReadAll(r.Body)
	sql := string(body)
	switch {
	case strings.Contains(sql, "SELECTOK"):
		writeJSON(w, `{"id":"q","columns":[{"name":"n","type":"bigint"},{"name":"label","type":"varchar"}],"data":[[1,"a"],[2,"b"]]}`)
	case strings.Contains(sql, "ROWSQ"):
		// Three rows on the first page plus a nextUri, so a cap below 3 truncates.
		writeJSON(w, fmt.Sprintf(`{"id":"q","columns":[{"name":"id","type":"bigint"}],"data":[[1],[2],[3]],"nextUri":"%s/v1/statement/next"}`, fe.URL))
	case strings.Contains(sql, "SHOW CATALOGS"):
		writeJSON(w, `{"id":"q","columns":[{"name":"Catalog","type":"varchar"}],"data":[["hive"],["system"]]}`)
	case strings.Contains(sql, "SHOW SCHEMAS"):
		writeJSON(w, `{"id":"q","columns":[{"name":"Schema","type":"varchar"}],"data":[["default"],["information_schema"]]}`)
	case strings.Contains(sql, "SHOW TABLES"):
		writeJSON(w, `{"id":"q","columns":[{"name":"Table","type":"varchar"}],"data":[["t1"],["t2"]]}`)
	case strings.Contains(sql, "DESCRIBE"):
		writeJSON(w, `{"id":"q","columns":[{"name":"Column","type":"varchar"},{"name":"Type","type":"varchar"},{"name":"Extra","type":"varchar"},{"name":"Comment","type":"varchar"}],"data":[["id","bigint","",""],["name","varchar","","the name"]]}`)
	case strings.Contains(sql, "SHOW STATS"):
		writeJSON(w, `{"id":"q","columns":[{"name":"column_name","type":"varchar"},{"name":"data_size","type":"double"},{"name":"distinct_values_count","type":"double"},{"name":"nulls_fraction","type":"double"},{"name":"row_count","type":"double"},{"name":"low_value","type":"varchar"},{"name":"high_value","type":"varchar"}],"data":[["id",800,100.0,0.0,null,"1","100"],[null,null,null,null,1000.0,null,null]]}`)
	case strings.Contains(sql, "system.runtime.nodes"):
		writeJSON(w, `{"id":"q","columns":[{"name":"node_id","type":"varchar"},{"name":"http_uri","type":"varchar"},{"name":"node_version","type":"varchar"},{"name":"coordinator","type":"boolean"},{"name":"state","type":"varchar"}],"data":[["n1","http://n1","440",true,"active"]]}`)
	case strings.Contains(sql, "ERRORQ"):
		writeJSON(w, `{"id":"q","error":{"message":"syntax error","errorName":"SYNTAX_ERROR","errorCode":1}}`)
	case strings.Contains(sql, "PAGEQ"):
		writeJSON(w, fmt.Sprintf(`{"id":"q","columns":[{"name":"Catalog","type":"varchar"}],"data":[["a"]],"nextUri":"%s/v1/statement/next"}`, fe.URL))
	default:
		writeJSON(w, `{"id":"q","columns":[{"name":"x","type":"varchar"}],"data":[]}`)
	}
}

func (fe *fakeEngine) handleNext(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, `{"id":"q","data":[["b"]]}`)
}

func (fe *fakeEngine) handleInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, `{"nodeVersion":{"version":"440"},"environment":"test","coordinator":true,"uptime":"5.00m"}`)
}

func (fe *fakeEngine) handleQueryList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, `[{"queryId":"q1","state":"FINISHED","query":"SELECT 1","sessionUser":"alice","queryStats":{"createTime":"2026-06-24T10:00:00.000Z","elapsedTime":"1.50s"}},
	{"queryId":"q2","state":"RUNNING","query":"SELECT 2","session":{"user":"bob"},"queryStats":{"createTime":"2026-06-24T11:00:00.000Z","elapsedTime":"0.50s"}}]`)
}

func (fe *fakeEngine) handleQueryDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/query/")
	if id == "missing" {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "not found")
		return
	}
	writeJSON(w, `{"queryId":"q1","state":"FINISHED","query":"SELECT 1","session":{"user":"alice"},
		"queryStats":{"createTime":"t0","endTime":"t1","elapsedTime":"2.00s","totalCpuTime":"1.00s","peakMemoryReservation":"10.00MB","rawInputDataSize":"1.00kB","rawInputPositions":5,"outputDataSize":"512B","outputPositions":1,
		"operatorSummaries":[{"stageId":0,"pipelineId":0,"operatorId":1,"operatorType":"ScanFilter","totalDrivers":4,"addInputCpu":"0.50s","getOutputCpu":"0.20s","addInputWall":"0.60s","getOutputWall":"0.30s","blockedWall":"0.10s","inputDataSize":"1.00kB","inputPositions":5,"outputDataSize":"512B","outputPositions":1}]}}`)
}

func writeJSON(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, s)
}

func newTestClient(tb testing.TB, fe *fakeEngine, dialect config.Dialect, cred credential.Provider) *Client {
	c, err := NewClient(config.EngineConfig{ID: "e", Endpoint: fe.URL, Dialect: dialect}, cred,
		WithHTTPClient(fe.Client()), WithSource("test-src"))
	require.NoError(tb, err)
	return c
}

func staticCred() credential.Provider {
	return fakeCred{cred: credential.Credential{User: "svc", Token: "tok"}}
}

func TestNewClient_Errors(t *testing.T) {
	_, err := NewClient(config.EngineConfig{Dialect: config.DialectWxd}, staticCred())
	assert.Error(t, err, "wxd unsupported")

	_, err = NewClient(config.EngineConfig{Dialect: "oracle"}, staticCred())
	assert.Error(t, err, "unknown dialect")

	_, err = NewClient(config.EngineConfig{Dialect: config.DialectTrino}, nil)
	assert.Error(t, err, "nil credential")
}

func TestMetadata(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	ctx := context.Background()

	cats, err := c.ListCatalogs(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"hive", "system"}, cats)

	schemas, err := c.ListSchemas(ctx, "hive")
	require.NoError(t, err)
	assert.Equal(t, []string{"default", "information_schema"}, schemas)

	tables, err := c.ListTables(ctx, "hive", "default")
	require.NoError(t, err)
	assert.Equal(t, []string{"t1", "t2"}, tables)

	cols, err := c.DescribeTable(ctx, "hive", "default", "t1")
	require.NoError(t, err)
	require.Len(t, cols, 2)
	assert.Equal(t, "id", cols[0].Name)
	assert.Equal(t, "the name", cols[1].Comment)
}

func TestDialectHeaders(t *testing.T) {
	fe := newFakeEngine(t)
	ctx := context.Background()

	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	_, err := c.ListCatalogs(ctx)
	require.NoError(t, err)
	h := fe.headers()
	assert.Equal(t, "svc", h.Get("X-Trino-User"))
	assert.Equal(t, "test-src", h.Get("X-Trino-Source"))
	assert.Equal(t, "Bearer tok", h.Get("Authorization"))

	cp := newTestClient(t, fe, config.DialectPresto, fakeCred{cred: credential.Credential{User: "u2"}})
	_, err = cp.ListCatalogs(ctx)
	require.NoError(t, err)
	h = fe.headers()
	assert.Equal(t, "u2", h.Get("X-Presto-User"))
	assert.Empty(t, h.Get("Authorization"), "no token, no auth header")
}

// TestAuthSchemeHeaders verifies the three auth scenarios at the wire level:
// unsecured (no token/password), bearer (JWT/OAuth2), and basic (user/password).
func TestAuthSchemeHeaders(t *testing.T) {
	ctx := context.Background()

	t.Run("unsecured", func(t *testing.T) {
		fe := newFakeEngine(t)
		c := newTestClient(t, fe, config.DialectTrino, fakeCred{cred: credential.Credential{User: "anon"}})
		_, err := c.ListCatalogs(ctx)
		require.NoError(t, err)
		h := fe.headers()
		assert.Equal(t, "anon", h.Get("X-Trino-User"))
		assert.Empty(t, h.Get("Authorization"), "no token/password => no Authorization header")
	})

	t.Run("bearer", func(t *testing.T) {
		fe := newFakeEngine(t)
		c := newTestClient(t, fe, config.DialectTrino, fakeCred{cred: credential.Credential{User: "svc", Token: "jwt-abc"}})
		_, err := c.ListCatalogs(ctx)
		require.NoError(t, err)
		h := fe.headers()
		assert.Equal(t, "svc", h.Get("X-Trino-User"))
		assert.Equal(t, "Bearer jwt-abc", h.Get("Authorization"))
	})

	t.Run("basic", func(t *testing.T) {
		fe := newFakeEngine(t)
		c := newTestClient(t, fe, config.DialectPresto, fakeCred{cred: credential.Credential{User: "alice", Password: "secret"}})
		_, err := c.ListCatalogs(ctx)
		require.NoError(t, err)
		h := fe.headers()
		assert.Equal(t, "alice", h.Get("X-Presto-User"))
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:secret"))
		assert.Equal(t, want, h.Get("Authorization"))
	})

	t.Run("basic takes precedence over token", func(t *testing.T) {
		fe := newFakeEngine(t)
		c := newTestClient(t, fe, config.DialectTrino, fakeCred{cred: credential.Credential{User: "alice", Password: "pw", Token: "ignored"}})
		_, err := c.ListCatalogs(ctx)
		require.NoError(t, err)
		want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:pw"))
		assert.Equal(t, want, fe.headers().Get("Authorization"))
	})

	t.Run("passthrough forwards the auth header verbatim and omits the user header", func(t *testing.T) {
		fe := newFakeEngine(t)
		// A passthrough credential with no user: the engine derives identity from
		// the forwarded JWT, so no X-Trino-User header is sent.
		c := newTestClient(t, fe, config.DialectTrino, fakeCred{cred: credential.Credential{AuthHeader: "Bearer verbatim.jwt.token"}})
		_, err := c.ListCatalogs(ctx)
		require.NoError(t, err)
		h := fe.headers()
		assert.Equal(t, "Bearer verbatim.jwt.token", h.Get("Authorization"))
		assert.Empty(t, h.Get("X-Trino-User"), "an empty user must not send a user header")
	})

	t.Run("passthrough auth header takes precedence over token and password", func(t *testing.T) {
		fe := newFakeEngine(t)
		c := newTestClient(t, fe, config.DialectPresto, fakeCred{cred: credential.Credential{
			User: "carol", AuthHeader: "Basic Zm9vOmJhcg==", Token: "ignored", Password: "ignored",
		}})
		_, err := c.ListCatalogs(ctx)
		require.NoError(t, err)
		h := fe.headers()
		assert.Equal(t, "carol", h.Get("X-Presto-User"), "a configured fallback user is still sent")
		assert.Equal(t, "Basic Zm9vOmJhcg==", h.Get("Authorization"))
	})
}

func TestGetTableStats(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	stats, err := c.GetTableStats(context.Background(), "hive", "default", "t1")
	require.NoError(t, err)
	require.NotNil(t, stats.RowCount)
	assert.EqualValues(t, 1000, *stats.RowCount)
	require.Len(t, stats.Columns, 1)
	assert.Equal(t, "id", stats.Columns[0].Column)
	require.NotNil(t, stats.Columns[0].DataSize)
	assert.EqualValues(t, 800, *stats.Columns[0].DataSize)
}

func TestGetClusterInfo(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	ci, err := c.GetClusterInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "440", ci.Version)
	assert.Equal(t, "test", ci.Environment)
	require.Len(t, ci.Nodes, 1)
	assert.Equal(t, "n1", ci.Nodes[0].NodeID)
	assert.True(t, ci.Nodes[0].Coordinator)
}

func TestPagedStatement(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	rs, err := c.runStatement(context.Background(), "PAGEQ", "", "")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, rs.firstColumn())
}

func TestRunReadQuery_Success(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	res, err := c.RunReadQuery(context.Background(), "SELECT n, label FROM t /* SELECTOK */", "", "", 0)
	require.NoError(t, err)
	assert.False(t, res.Truncated)
	require.Len(t, res.Columns, 2)
	assert.Equal(t, "n", res.Columns[0].Name)
	assert.Equal(t, "bigint", res.Columns[0].Type)
	assert.Equal(t, "varchar", res.Columns[1].Type)
	require.Len(t, res.Rows, 2)
	assert.EqualValues(t, 1, res.Rows[0][0])
	assert.Equal(t, "a", res.Rows[0][1])
}

func TestRunReadQuery_SessionContext(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	_, err := c.RunReadQuery(context.Background(), "SELECT n FROM t /* SELECTOK */", "hive", "default", 0)
	require.NoError(t, err)
	h := fe.headers()
	assert.Equal(t, "hive", h.Get("X-Trino-Catalog"))
	assert.Equal(t, "default", h.Get("X-Trino-Schema"))
}

func TestRunReadQuery_Paged(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	res, err := c.RunReadQuery(context.Background(), "SELECT x /* PAGEQ */", "", "", 0)
	require.NoError(t, err)
	assert.False(t, res.Truncated)
	require.Len(t, res.Rows, 2, "rows accumulated across nextUri pages")
	assert.Equal(t, "a", res.Rows[0][0])
	assert.Equal(t, "b", res.Rows[1][0])
}

func TestRunReadQuery_Truncates(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	res, err := c.RunReadQuery(context.Background(), "SELECT * FROM big /* ROWSQ */", "", "", 2)
	require.NoError(t, err)
	assert.True(t, res.Truncated)
	require.Len(t, res.Rows, 2, "trimmed to the cap")
	_, cancels := fe.counts()
	assert.GreaterOrEqual(t, cancels, 1, "the still-running statement is cancelled")
}

func TestRunReadQuery_RejectsWriteBeforeHTTP(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	_, err := c.RunReadQuery(context.Background(), "DELETE FROM t", "", "", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")
	stmtCalls, _ := fe.counts()
	assert.Zero(t, stmtCalls, "a rejected statement never reaches the engine")
}

func TestRunReadQuery_EngineError(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	_, err := c.RunReadQuery(context.Background(), "SELECT x /* ERRORQ */", "", "", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "syntax error")
}

func TestListQueries(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	qs, err := c.ListQueries(context.Background())
	require.NoError(t, err)
	require.Len(t, qs, 2)
	assert.Equal(t, "alice", qs[0].User())
	assert.Equal(t, "bob", qs[1].User(), "falls back to nested session.user")
}

func TestGetQuery(t *testing.T) {
	fe := newFakeEngine(t)
	c := newTestClient(t, fe, config.DialectTrino, staticCred())
	qi, err := c.GetQuery(context.Background(), "q1")
	require.NoError(t, err)
	assert.Equal(t, "FINISHED", qi.State)
	assert.Equal(t, "alice", qi.Session.User)
	require.Len(t, qi.QueryStats.OperatorSummaries, 1)

	raw, err := c.GetQueryRaw(context.Background(), "q1")
	require.NoError(t, err)
	assert.Contains(t, string(raw), "ScanFilter")

	_, err = c.GetQueryRaw(context.Background(), "missing")
	assert.Error(t, err)
}

func TestErrorPaths(t *testing.T) {
	t.Run("statement error field", func(t *testing.T) {
		fe := newFakeEngine(t)
		c := newTestClient(t, fe, config.DialectTrino, staticCred())
		_, err := c.runStatement(context.Background(), "ERRORQ", "", "")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "syntax error")
	})

	t.Run("http 401", func(t *testing.T) {
		fe := newFakeEngine(t)
		fe.status = http.StatusUnauthorized
		c := newTestClient(t, fe, config.DialectTrino, staticCred())
		_, err := c.ListCatalogs(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "401")
	})

	t.Run("http 500", func(t *testing.T) {
		fe := newFakeEngine(t)
		fe.status = http.StatusInternalServerError
		c := newTestClient(t, fe, config.DialectTrino, staticCred())
		_, err := c.ListCatalogs(context.Background())
		require.Error(t, err)
	})

	t.Run("bad json", func(t *testing.T) {
		fe := newFakeEngine(t)
		fe.badJSON = true
		c := newTestClient(t, fe, config.DialectTrino, staticCred())
		_, err := c.ListCatalogs(context.Background())
		require.Error(t, err)
	})

	t.Run("credential error", func(t *testing.T) {
		fe := newFakeEngine(t)
		c := newTestClient(t, fe, config.DialectTrino, fakeCred{err: fmt.Errorf("vault down")})
		_, err := c.ListCatalogs(context.Background())
		require.Error(t, err)
		_, err = c.GetClusterInfo(context.Background())
		require.Error(t, err)
		_, err = c.GetQueryRaw(context.Background(), "q1")
		require.Error(t, err)
	})

	t.Run("unknown engine host", func(t *testing.T) {
		c, err := NewClient(config.EngineConfig{Endpoint: "http://127.0.0.1:0", Dialect: config.DialectTrino}, staticCred())
		require.NoError(t, err)
		_, err = c.ListCatalogs(context.Background())
		require.Error(t, err)
	})
}

func TestTLSInsecureSkipVerify(t *testing.T) {
	// A TLS server with a self-signed (httptest) certificate.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, `{"columns":[{"name":"Catalog"}],"data":[["hive"]]}`)
	}))
	t.Cleanup(srv.Close)
	ctx := context.Background()

	// Without skip-verify, the default transport rejects the self-signed cert.
	cVerify, err := NewClient(config.EngineConfig{Endpoint: srv.URL, Dialect: config.DialectTrino}, staticCred())
	require.NoError(t, err)
	_, err = cVerify.ListCatalogs(ctx)
	require.Error(t, err, "self-signed cert should fail verification")

	// With skip-verify, the call succeeds.
	cSkip, err := NewClient(config.EngineConfig{Endpoint: srv.URL, Dialect: config.DialectTrino, TLSInsecureSkipVerify: true}, staticCred())
	require.NoError(t, err)
	cats, err := cSkip.ListCatalogs(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"hive"}, cats)
}

func TestQuoteIdent(t *testing.T) {
	assert.Equal(t, `"a"`, quoteIdent("a"))
	assert.Equal(t, `"a""b"`, quoteIdent(`a"b`), "embedded quote doubled")
	assert.Equal(t, `"c"."s"."t"`, fqTable("c", "s", "t"))
}
