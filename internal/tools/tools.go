// Package tools registers the curated read-only MCP tools and wires them to the
// engine registry. Every tool is stateless; all but list_engines take an engine
// id. Handlers return ordinary errors, which the SDK surfaces to the agent as
// tool errors (IsError) so the model can self-correct.
package tools

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/yabinma/presto-mcp/internal/credential"
	"github.com/yabinma/presto-mcp/internal/normalize"
	"github.com/yabinma/presto-mcp/internal/presto"
	"github.com/yabinma/presto-mcp/internal/registry"
)

// Register adds all read-only tools to the server, backed by reg, and installs
// the passthrough caller middleware so the enterprise (HTTP) shape can forward
// the caller's credential to the engine.
func Register(server *mcp.Server, reg *registry.Registry) {
	server.AddReceivingMiddleware(callerMiddleware)

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_engines",
		Description: "List the configured engines this server can reach.",
	}, listEngines(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_catalogs",
		Description: "List the catalogs available on an engine.",
	}, listCatalogs(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_schemas",
		Description: "List the schemas in a catalog on an engine.",
	}, listSchemas(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_tables",
		Description: "List the tables in a schema on an engine.",
	}, listTables(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "describe_table",
		Description: "Describe a table's columns and types.",
	}, describeTable(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_table_stats",
		Description: "Best-effort row count and per-column statistics for a table.",
	}, getTableStats(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_cluster_info",
		Description: "Engine version, environment, and node list.",
	}, getClusterInfo(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name: "run_query",
		Description: "Run a single read-only SQL query (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN/VALUES/TABLE) and return its rows. " +
			"Writes and DDL are rejected. Results are capped (default 1000 rows, max 10000); truncated=true means the engine had more rows than were returned.",
	}, runQuery(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_queries",
		Description: "List queries for auditing, optionally filtered by state, user, and time range. Reports which source answered (live coordinator or history).",
	}, listQueries(reg))

	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_query",
		Description: "Normalized performance detail for one query (summary, and stages/operators/plan when available). Set raw=true to also include the raw engine fragment.",
	}, getQuery(reg))
}

// callerMiddleware copies the transport-level credential signals — the incoming
// Authorization header and any verified token identity — from an HTTP request
// onto the context, where the passthrough credential provider reads them. It is
// a no-op for stdio (no transport header) and for the static strategy (which
// ignores the context), so it is safe to install unconditionally.
func callerMiddleware(next mcp.MethodHandler) mcp.MethodHandler {
	return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
		if extra := req.GetExtra(); extra != nil && extra.Header != nil {
			c := credential.Caller{AuthHeader: extra.Header.Get("Authorization")}
			if extra.TokenInfo != nil {
				c.VerifiedUser = extra.TokenInfo.UserID
			}
			ctx = credential.WithCaller(ctx, c)
		}
		return next(ctx, method, req)
	}
}

// --- inputs ---------------------------------------------------------------

// EngineInput is embedded by tools that target a single engine.
type EngineInput struct {
	Engine string `json:"engine" jsonschema:"the engine id from list_engines"`
}

type catalogInput struct {
	Engine  string `json:"engine"`
	Catalog string `json:"catalog"`
}

type schemaInput struct {
	Engine  string `json:"engine"`
	Catalog string `json:"catalog"`
	Schema  string `json:"schema"`
}

type tableInput struct {
	Engine  string `json:"engine"`
	Catalog string `json:"catalog"`
	Schema  string `json:"schema"`
	Table   string `json:"table"`
}

type listQueriesInput struct {
	Engine string `json:"engine"`
	State  string `json:"state,omitempty" jsonschema:"optional query state filter, e.g. RUNNING or FINISHED"`
	User   string `json:"user,omitempty" jsonschema:"optional submitting-user filter"`
	Since  string `json:"since,omitempty" jsonschema:"optional RFC3339 lower bound on query create time"`
	Until  string `json:"until,omitempty" jsonschema:"optional RFC3339 upper bound on query create time"`
}

type getQueryInput struct {
	Engine  string `json:"engine"`
	QueryID string `json:"query_id"`
	Raw     bool   `json:"raw,omitempty" jsonschema:"include the raw engine fragment as well"`
}

type runQueryInput struct {
	Engine         string `json:"engine"`
	SQL            string `json:"sql" jsonschema:"a single read-only statement (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN/VALUES/TABLE); writes and DDL are rejected"`
	Catalog        string `json:"catalog,omitempty" jsonschema:"optional session catalog for resolving unqualified names"`
	Schema         string `json:"schema,omitempty" jsonschema:"optional session schema for resolving unqualified names"`
	MaxRows        int    `json:"max_rows,omitempty" jsonschema:"row cap (default 1000, max 10000); extra rows are truncated"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty" jsonschema:"overall query timeout in seconds (default 60, max 300)"`
}

// --- outputs --------------------------------------------------------------

// EngineSummary describes one engine in list_engines.
type EngineSummary struct {
	ID             string `json:"id"`
	Endpoint       string `json:"endpoint"`
	Dialect        string `json:"dialect"`
	HistoryEnabled bool   `json:"history_enabled"`
}

type listEnginesOutput struct {
	Engines []EngineSummary `json:"engines"`
}

type listCatalogsOutput struct {
	Engine   string   `json:"engine"`
	Catalogs []string `json:"catalogs"`
}

type listSchemasOutput struct {
	Engine  string   `json:"engine"`
	Catalog string   `json:"catalog"`
	Schemas []string `json:"schemas"`
}

type listTablesOutput struct {
	Engine  string   `json:"engine"`
	Catalog string   `json:"catalog"`
	Schema  string   `json:"schema"`
	Tables  []string `json:"tables"`
}

type describeTableOutput struct {
	Engine  string              `json:"engine"`
	Catalog string              `json:"catalog"`
	Schema  string              `json:"schema"`
	Table   string              `json:"table"`
	Columns []presto.ColumnInfo `json:"columns"`
}

type getTableStatsOutput struct {
	Engine string             `json:"engine"`
	Table  string             `json:"table"`
	Stats  *presto.TableStats `json:"stats"`
}

type getClusterInfoOutput struct {
	Engine  string              `json:"engine"`
	Cluster *presto.ClusterInfo `json:"cluster"`
}

type listQueriesOutput struct {
	Engine  string                    `json:"engine"`
	Source  string                    `json:"source"`
	Queries []normalize.QueryListItem `json:"queries"`
}

type runQueryOutput struct {
	Engine    string          `json:"engine"`
	Columns   []presto.Column `json:"columns"`
	Rows      [][]any         `json:"rows"`
	RowCount  int             `json:"row_count"`
	Truncated bool            `json:"truncated"`
}

// --- handlers -------------------------------------------------------------

func listEngines(reg *registry.Registry) mcp.ToolHandlerFor[struct{}, listEnginesOutput] {
	return func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, listEnginesOutput, error) {
		var out listEnginesOutput
		for _, e := range reg.List() {
			out.Engines = append(out.Engines, EngineSummary{
				ID:             e.Config.ID,
				Endpoint:       e.Config.Endpoint,
				Dialect:        string(e.Config.Dialect),
				HistoryEnabled: e.Config.History.Enabled,
			})
		}
		return nil, out, nil
	}
}

func listCatalogs(reg *registry.Registry) mcp.ToolHandlerFor[EngineInput, listCatalogsOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in EngineInput) (*mcp.CallToolResult, listCatalogsOutput, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, listCatalogsOutput{}, err
		}
		cats, err := eng.Client.ListCatalogs(ctx)
		if err != nil {
			return nil, listCatalogsOutput{}, err
		}
		return nil, listCatalogsOutput{Engine: in.Engine, Catalogs: cats}, nil
	}
}

func listSchemas(reg *registry.Registry) mcp.ToolHandlerFor[catalogInput, listSchemasOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in catalogInput) (*mcp.CallToolResult, listSchemasOutput, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, listSchemasOutput{}, err
		}
		if in.Catalog == "" {
			return nil, listSchemasOutput{}, fmt.Errorf("catalog is required")
		}
		schemas, err := eng.Client.ListSchemas(ctx, in.Catalog)
		if err != nil {
			return nil, listSchemasOutput{}, err
		}
		return nil, listSchemasOutput{Engine: in.Engine, Catalog: in.Catalog, Schemas: schemas}, nil
	}
}

func listTables(reg *registry.Registry) mcp.ToolHandlerFor[schemaInput, listTablesOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in schemaInput) (*mcp.CallToolResult, listTablesOutput, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, listTablesOutput{}, err
		}
		if in.Catalog == "" || in.Schema == "" {
			return nil, listTablesOutput{}, fmt.Errorf("catalog and schema are required")
		}
		tables, err := eng.Client.ListTables(ctx, in.Catalog, in.Schema)
		if err != nil {
			return nil, listTablesOutput{}, err
		}
		return nil, listTablesOutput{Engine: in.Engine, Catalog: in.Catalog, Schema: in.Schema, Tables: tables}, nil
	}
}

func describeTable(reg *registry.Registry) mcp.ToolHandlerFor[tableInput, describeTableOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in tableInput) (*mcp.CallToolResult, describeTableOutput, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, describeTableOutput{}, err
		}
		if err := requireTable(in.Catalog, in.Schema, in.Table); err != nil {
			return nil, describeTableOutput{}, err
		}
		cols, err := eng.Client.DescribeTable(ctx, in.Catalog, in.Schema, in.Table)
		if err != nil {
			return nil, describeTableOutput{}, err
		}
		return nil, describeTableOutput{Engine: in.Engine, Catalog: in.Catalog, Schema: in.Schema, Table: in.Table, Columns: cols}, nil
	}
}

func getTableStats(reg *registry.Registry) mcp.ToolHandlerFor[tableInput, getTableStatsOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in tableInput) (*mcp.CallToolResult, getTableStatsOutput, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, getTableStatsOutput{}, err
		}
		if err := requireTable(in.Catalog, in.Schema, in.Table); err != nil {
			return nil, getTableStatsOutput{}, err
		}
		stats, err := eng.Client.GetTableStats(ctx, in.Catalog, in.Schema, in.Table)
		if err != nil {
			return nil, getTableStatsOutput{}, err
		}
		fq := fmt.Sprintf("%s.%s.%s", in.Catalog, in.Schema, in.Table)
		return nil, getTableStatsOutput{Engine: in.Engine, Table: fq, Stats: stats}, nil
	}
}

func getClusterInfo(reg *registry.Registry) mcp.ToolHandlerFor[EngineInput, getClusterInfoOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in EngineInput) (*mcp.CallToolResult, getClusterInfoOutput, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, getClusterInfoOutput{}, err
		}
		ci, err := eng.Client.GetClusterInfo(ctx)
		if err != nil {
			return nil, getClusterInfoOutput{}, err
		}
		return nil, getClusterInfoOutput{Engine: in.Engine, Cluster: ci}, nil
	}
}

// query timeout bounds the whole multi-page statement (the client's per-request
// http timeout is shorter); see runQuery.
const (
	defaultQueryTimeout = 60 * time.Second
	maxQueryTimeout     = 300 * time.Second
)

func queryTimeout(sec int) time.Duration {
	if sec <= 0 {
		return defaultQueryTimeout
	}
	if d := time.Duration(sec) * time.Second; d < maxQueryTimeout {
		return d
	}
	return maxQueryTimeout
}

func runQuery(reg *registry.Registry) mcp.ToolHandlerFor[runQueryInput, runQueryOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in runQueryInput) (*mcp.CallToolResult, runQueryOutput, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, runQueryOutput{}, err
		}
		if strings.TrimSpace(in.SQL) == "" {
			return nil, runQueryOutput{}, fmt.Errorf("sql is required")
		}
		ctx, cancel := context.WithTimeout(ctx, queryTimeout(in.TimeoutSeconds))
		defer cancel()
		res, err := eng.Client.RunReadQuery(ctx, in.SQL, in.Catalog, in.Schema, in.MaxRows)
		if err != nil {
			return nil, runQueryOutput{}, err
		}
		return nil, runQueryOutput{
			Engine:    in.Engine,
			Columns:   res.Columns,
			Rows:      res.Rows,
			RowCount:  len(res.Rows),
			Truncated: res.Truncated,
		}, nil
	}
}

func listQueries(reg *registry.Registry) mcp.ToolHandlerFor[listQueriesInput, listQueriesOutput] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in listQueriesInput) (*mcp.CallToolResult, listQueriesOutput, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, listQueriesOutput{}, err
		}
		filter, err := buildFilter(in)
		if err != nil {
			return nil, listQueriesOutput{}, err
		}
		if eng.History != nil {
			items, err := eng.History.ListQueries(ctx, filter)
			if err != nil {
				return nil, listQueriesOutput{}, err
			}
			return nil, listQueriesOutput{Engine: in.Engine, Source: normalize.SourceHistory, Queries: items}, nil
		}
		basic, err := eng.Client.ListQueries(ctx)
		if err != nil {
			return nil, listQueriesOutput{}, err
		}
		items := normalize.QueryListFromLive(basic, filter)
		return nil, listQueriesOutput{Engine: in.Engine, Source: normalize.SourceLive, Queries: items}, nil
	}
}

func getQuery(reg *registry.Registry) mcp.ToolHandlerFor[getQueryInput, normalize.QueryDetail] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in getQueryInput) (*mcp.CallToolResult, normalize.QueryDetail, error) {
		eng, err := engineOf(reg, in.Engine)
		if err != nil {
			return nil, normalize.QueryDetail{}, err
		}
		if in.QueryID == "" {
			return nil, normalize.QueryDetail{}, fmt.Errorf("query_id is required")
		}
		if eng.History != nil {
			d, err := eng.History.GetQuery(ctx, in.QueryID)
			if err != nil {
				return nil, normalize.QueryDetail{}, err
			}
			return nil, *d, nil
		}
		body, err := eng.Client.GetQueryRaw(ctx, in.QueryID)
		if err != nil {
			return nil, normalize.QueryDetail{}, err
		}
		qi, err := presto.DecodeQueryInfo(body)
		if err != nil {
			return nil, normalize.QueryDetail{}, err
		}
		return nil, normalize.QueryDetailFromLive(qi, in.Raw, body), nil
	}
}

// --- helpers --------------------------------------------------------------

func engineOf(reg *registry.Registry, id string) (*registry.Engine, error) {
	if id == "" {
		return nil, fmt.Errorf("engine is required")
	}
	e, ok := reg.Get(id)
	if !ok {
		return nil, fmt.Errorf("unknown engine %q (use list_engines)", id)
	}
	return e, nil
}

func requireTable(catalog, schema, table string) error {
	if catalog == "" || schema == "" || table == "" {
		return fmt.Errorf("catalog, schema, and table are required")
	}
	return nil
}

func buildFilter(in listQueriesInput) (normalize.Filter, error) {
	f := normalize.Filter{State: in.State, User: in.User}
	if in.Since != "" {
		t, err := time.Parse(time.RFC3339, in.Since)
		if err != nil {
			return f, fmt.Errorf("since: %w", err)
		}
		f.Since = &t
	}
	if in.Until != "" {
		t, err := time.Parse(time.RFC3339, in.Until)
		if err != nil {
			return f, fmt.Errorf("until: %w", err)
		}
		f.Until = &t
	}
	return f, nil
}
