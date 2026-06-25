package tools

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
	"github.com/yabinma/presto-mcp/internal/history"
	"github.com/yabinma/presto-mcp/internal/normalize"
	"github.com/yabinma/presto-mcp/internal/registry"
)

func TestRegister_NoPanic(t *testing.T) {
	s := fakeEngineServer(t)
	reg := testRegistry(t, s.URL, false)
	srv := mcp.NewServer(&mcp.Implementation{Name: "t", Version: "0"}, nil)
	// Register infers input/output schemas for every tool; a bad schema panics.
	assert.NotPanics(t, func() { Register(srv, reg) })
}

func TestCallerMiddleware(t *testing.T) {
	var gotCtx context.Context
	next := func(ctx context.Context, _ string, _ mcp.Request) (mcp.Result, error) {
		gotCtx = ctx
		return &mcp.CallToolResult{}, nil
	}
	mw := callerMiddleware(next)

	t.Run("injects the caller from an HTTP request's auth header and verified token", func(t *testing.T) {
		hdr := http.Header{}
		hdr.Set("Authorization", "Bearer xyz")
		req := &mcp.ServerRequest[*mcp.CallToolParams]{
			Extra: &mcp.RequestExtra{Header: hdr, TokenInfo: &auth.TokenInfo{UserID: "dave"}},
		}
		_, err := mw(context.Background(), "tools/call", req)
		require.NoError(t, err)
		c, ok := credential.CallerFromContext(gotCtx)
		require.True(t, ok)
		assert.Equal(t, "Bearer xyz", c.AuthHeader)
		assert.Equal(t, "dave", c.VerifiedUser)
	})

	t.Run("opaque mode leaves the verified user empty", func(t *testing.T) {
		hdr := http.Header{}
		hdr.Set("Authorization", "Bearer opaque")
		req := &mcp.ServerRequest[*mcp.CallToolParams]{Extra: &mcp.RequestExtra{Header: hdr}}
		_, err := mw(context.Background(), "tools/call", req)
		require.NoError(t, err)
		c, ok := credential.CallerFromContext(gotCtx)
		require.True(t, ok)
		assert.Equal(t, "Bearer opaque", c.AuthHeader)
		assert.Empty(t, c.VerifiedUser)
	})

	t.Run("no transport header (stdio) does not inject a caller", func(t *testing.T) {
		gotCtx = context.Background()
		_, err := mw(context.Background(), "tools/call", &mcp.ServerRequest[*mcp.CallToolParams]{})
		require.NoError(t, err)
		_, ok := credential.CallerFromContext(gotCtx)
		assert.False(t, ok)
	})
}

func fakeEngineServer(tb testing.TB) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/statement", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sql := string(body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(sql, "SELECTOK"):
			fmt.Fprint(w, `{"columns":[{"name":"n","type":"bigint"}],"data":[[1],[2]]}`)
		case strings.Contains(sql, "SHOW CATALOGS"):
			fmt.Fprint(w, `{"columns":[{"name":"Catalog"}],"data":[["hive"]]}`)
		case strings.Contains(sql, "SHOW SCHEMAS"):
			fmt.Fprint(w, `{"columns":[{"name":"Schema"}],"data":[["default"]]}`)
		case strings.Contains(sql, "SHOW TABLES"):
			fmt.Fprint(w, `{"columns":[{"name":"Table"}],"data":[["t1"]]}`)
		case strings.Contains(sql, "DESCRIBE"):
			fmt.Fprint(w, `{"columns":[{"name":"Column"},{"name":"Type"}],"data":[["id","bigint"]]}`)
		case strings.Contains(sql, "SHOW STATS"):
			fmt.Fprint(w, `{"columns":[{"name":"column_name"},{"name":"row_count"}],"data":[[null,42.0]]}`)
		case strings.Contains(sql, "system.runtime.nodes"):
			fmt.Fprint(w, `{"columns":[{"name":"node_id"}],"data":[["n1"]]}`)
		default:
			fmt.Fprint(w, `{"columns":[],"data":[]}`)
		}
	})
	mux.HandleFunc("/v1/info", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"nodeVersion":{"version":"440"},"environment":"test","coordinator":true}`)
	})
	mux.HandleFunc("/v1/query", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `[{"queryId":"q1","state":"FINISHED","sessionUser":"alice","queryStats":{"createTime":"2026-06-24T10:00:00Z"}}]`)
	})
	mux.HandleFunc("/v1/query/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/missing") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fmt.Fprint(w, `{"queryId":"q1","state":"FINISHED","session":{"user":"alice"},"queryStats":{"elapsedTime":"1.00s"}}`)
	})
	s := httptest.NewServer(mux)
	tb.Cleanup(s.Close)
	return s
}

func testRegistry(tb testing.TB, url string, withHistory bool) *registry.Registry {
	ec := config.EngineConfig{
		ID: "e", Endpoint: url, Dialect: config.DialectTrino,
		Auth: config.AuthConfig{Mode: config.AuthStatic, User: "u"},
	}
	var hf registry.HistoryFactory
	if withHistory {
		ec.History = config.HistoryConfig{Enabled: true, Provider: "fake"}
		hf = func(config.EngineConfig) (history.Provider, error) { return fakeHistory{}, nil }
	}
	cfg := &config.Config{DeploymentMode: config.ModeLocal, Engines: []config.EngineConfig{ec}}
	r, err := registry.New(cfg, registry.DefaultCredentialFactory, hf)
	require.NoError(tb, err)
	return r
}

type fakeHistory struct {
	notFound bool
}

func (fakeHistory) Name() string { return "fake" }
func (f fakeHistory) ListQueries(context.Context, normalize.Filter) ([]normalize.QueryListItem, error) {
	return []normalize.QueryListItem{{QueryID: "h1", State: "FINISHED", User: "carol"}}, nil
}
func (f fakeHistory) GetQuery(_ context.Context, id string) (*normalize.QueryDetail, error) {
	if f.notFound {
		return nil, &history.ErrNotFound{QueryID: id}
	}
	return &normalize.QueryDetail{
		Summary:           normalize.QuerySummary{QueryID: id, State: "FINISHED"},
		Source:            normalize.SourceHistory,
		AvailableSections: []string{normalize.SectionSummary},
	}, nil
}

func ctx() context.Context { return context.Background() }

func TestListEngines(t *testing.T) {
	s := fakeEngineServer(t)
	reg := testRegistry(t, s.URL, false)
	_, out, err := listEngines(reg)(ctx(), nil, struct{}{})
	require.NoError(t, err)
	require.Len(t, out.Engines, 1)
	assert.Equal(t, "e", out.Engines[0].ID)
	assert.Equal(t, "trino", out.Engines[0].Dialect)
	assert.False(t, out.Engines[0].HistoryEnabled)
}

func TestMetadataTools(t *testing.T) {
	s := fakeEngineServer(t)
	reg := testRegistry(t, s.URL, false)

	_, cat, err := listCatalogs(reg)(ctx(), nil, EngineInput{Engine: "e"})
	require.NoError(t, err)
	assert.Equal(t, []string{"hive"}, cat.Catalogs)

	_, sc, err := listSchemas(reg)(ctx(), nil, catalogInput{Engine: "e", Catalog: "hive"})
	require.NoError(t, err)
	assert.Equal(t, []string{"default"}, sc.Schemas)

	_, tb, err := listTables(reg)(ctx(), nil, schemaInput{Engine: "e", Catalog: "hive", Schema: "default"})
	require.NoError(t, err)
	assert.Equal(t, []string{"t1"}, tb.Tables)

	_, dt, err := describeTable(reg)(ctx(), nil, tableInput{Engine: "e", Catalog: "hive", Schema: "default", Table: "t1"})
	require.NoError(t, err)
	require.Len(t, dt.Columns, 1)
	assert.Equal(t, "id", dt.Columns[0].Name)

	_, st, err := getTableStats(reg)(ctx(), nil, tableInput{Engine: "e", Catalog: "hive", Schema: "default", Table: "t1"})
	require.NoError(t, err)
	require.NotNil(t, st.Stats.RowCount)
	assert.EqualValues(t, 42, *st.Stats.RowCount)
	assert.Equal(t, "hive.default.t1", st.Table)

	_, ci, err := getClusterInfo(reg)(ctx(), nil, EngineInput{Engine: "e"})
	require.NoError(t, err)
	assert.Equal(t, "440", ci.Cluster.Version)
}

func TestListQueries_LiveAndHistory(t *testing.T) {
	s := fakeEngineServer(t)

	regLive := testRegistry(t, s.URL, false)
	_, live, err := listQueries(regLive)(ctx(), nil, listQueriesInput{Engine: "e"})
	require.NoError(t, err)
	assert.Equal(t, normalize.SourceLive, live.Source)
	require.Len(t, live.Queries, 1)
	assert.Equal(t, "alice", live.Queries[0].User)

	// filter by user that doesn't match -> empty
	_, none, err := listQueries(regLive)(ctx(), nil, listQueriesInput{Engine: "e", User: "nobody"})
	require.NoError(t, err)
	assert.Empty(t, none.Queries)

	regHist := testRegistry(t, s.URL, true)
	_, hist, err := listQueries(regHist)(ctx(), nil, listQueriesInput{Engine: "e"})
	require.NoError(t, err)
	assert.Equal(t, normalize.SourceHistory, hist.Source)
	require.Len(t, hist.Queries, 1)
	assert.Equal(t, "carol", hist.Queries[0].User)
}

func TestListQueries_BadTimeRange(t *testing.T) {
	s := fakeEngineServer(t)
	reg := testRegistry(t, s.URL, false)
	_, _, err := listQueries(reg)(ctx(), nil, listQueriesInput{Engine: "e", Since: "not-a-time"})
	assert.Error(t, err)
	_, _, err = listQueries(reg)(ctx(), nil, listQueriesInput{Engine: "e", Until: "nope"})
	assert.Error(t, err)
}

func TestGetQuery_LiveAndRawAndHistory(t *testing.T) {
	s := fakeEngineServer(t)

	regLive := testRegistry(t, s.URL, false)
	_, d, err := getQuery(regLive)(ctx(), nil, getQueryInput{Engine: "e", QueryID: "q1"})
	require.NoError(t, err)
	assert.Equal(t, normalize.SourceLive, d.Source)
	assert.Empty(t, d.Raw)

	_, dr, err := getQuery(regLive)(ctx(), nil, getQueryInput{Engine: "e", QueryID: "q1", Raw: true})
	require.NoError(t, err)
	assert.NotEmpty(t, dr.Raw)

	_, _, err = getQuery(regLive)(ctx(), nil, getQueryInput{Engine: "e", QueryID: "missing"})
	assert.Error(t, err)

	regHist := testRegistry(t, s.URL, true)
	_, dh, err := getQuery(regHist)(ctx(), nil, getQueryInput{Engine: "e", QueryID: "x"})
	require.NoError(t, err)
	assert.Equal(t, normalize.SourceHistory, dh.Source)
}

func TestRunQuery(t *testing.T) {
	s := fakeEngineServer(t)
	reg := testRegistry(t, s.URL, false)

	_, out, err := runQuery(reg)(ctx(), nil, runQueryInput{Engine: "e", SQL: "SELECT n /* SELECTOK */"})
	require.NoError(t, err)
	assert.Equal(t, "e", out.Engine)
	require.Len(t, out.Columns, 1)
	assert.Equal(t, "n", out.Columns[0].Name)
	assert.Equal(t, "bigint", out.Columns[0].Type)
	assert.Equal(t, 2, out.RowCount)
	require.Len(t, out.Rows, 2)
	assert.False(t, out.Truncated)
}

func TestRunQuery_Errors(t *testing.T) {
	s := fakeEngineServer(t)
	reg := testRegistry(t, s.URL, false)

	// unknown engine
	_, _, err := runQuery(reg)(ctx(), nil, runQueryInput{Engine: "nope", SQL: "SELECT 1"})
	assert.Error(t, err)

	// empty / blank sql
	_, _, err = runQuery(reg)(ctx(), nil, runQueryInput{Engine: "e", SQL: "   "})
	assert.Error(t, err)

	// write rejected by the read-only guard
	_, _, err = runQuery(reg)(ctx(), nil, runQueryInput{Engine: "e", SQL: "INSERT INTO t VALUES (1)"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")
}

func TestQueryTimeout(t *testing.T) {
	assert.Equal(t, defaultQueryTimeout, queryTimeout(0))
	assert.Equal(t, defaultQueryTimeout, queryTimeout(-3))
	assert.Equal(t, 10*time.Second, queryTimeout(10))
	assert.Equal(t, maxQueryTimeout, queryTimeout(99999))
}

func TestErrorsAndValidation(t *testing.T) {
	s := fakeEngineServer(t)
	reg := testRegistry(t, s.URL, false)

	// unknown / empty engine
	_, _, err := listCatalogs(reg)(ctx(), nil, EngineInput{Engine: "nope"})
	assert.Error(t, err)
	_, _, err = listCatalogs(reg)(ctx(), nil, EngineInput{})
	assert.Error(t, err)

	// required args
	_, _, err = listSchemas(reg)(ctx(), nil, catalogInput{Engine: "e"})
	assert.Error(t, err)
	_, _, err = listTables(reg)(ctx(), nil, schemaInput{Engine: "e", Catalog: "c"})
	assert.Error(t, err)
	_, _, err = describeTable(reg)(ctx(), nil, tableInput{Engine: "e", Catalog: "c", Schema: "s"})
	assert.Error(t, err)
	_, _, err = getQuery(reg)(ctx(), nil, getQueryInput{Engine: "e"})
	assert.Error(t, err)
}

func TestGetQuery_HistoryNotFound(t *testing.T) {
	s := fakeEngineServer(t)
	ec := config.EngineConfig{ID: "e", Endpoint: s.URL, Dialect: config.DialectTrino,
		Auth:    config.AuthConfig{Mode: config.AuthStatic, User: "u"},
		History: config.HistoryConfig{Enabled: true, Provider: "fake"}}
	cfg := &config.Config{Engines: []config.EngineConfig{ec}}
	reg, err := registry.New(cfg, registry.DefaultCredentialFactory, func(config.EngineConfig) (history.Provider, error) {
		return fakeHistory{notFound: true}, nil
	})
	require.NoError(t, err)
	_, _, err = getQuery(reg)(ctx(), nil, getQueryInput{Engine: "e", QueryID: "z"})
	assert.Error(t, err)
}
