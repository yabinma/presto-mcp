// Package presto is a read-only REST client for Presto/Trino engines: it drives
// the statement protocol for metadata queries and reads the coordinator's query
// endpoints, hiding dialect (header) differences behind a small abstraction.
package presto

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
)

const defaultSource = "presto-mcp"

// Client is a read-only REST client for one Presto/Trino engine. It is safe for
// concurrent use; all methods are stateless beyond the shared http.Client.
type Client struct {
	endpoint string
	dialect  dialect
	cred     credential.Provider
	http     *http.Client
	source   string
}

// Option customizes a Client.
type Option func(*Client)

// WithHTTPClient injects an http.Client (tests point this at httptest.Server).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithSource overrides the source name reported to the engine.
func WithSource(s string) Option { return func(c *Client) { c.source = s } }

// NewClient builds a client for an engine. It returns an error for unsupported
// dialects (currently wxd).
func NewClient(eng config.EngineConfig, cred credential.Provider, opts ...Option) (*Client, error) {
	d, err := dialectFor(eng.Dialect)
	if err != nil {
		return nil, err
	}
	if cred == nil {
		return nil, fmt.Errorf("credential provider is required")
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	if eng.TLSInsecureSkipVerify {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		// Dev/test only: trust self-signed certificates for this engine.
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
		httpClient.Transport = tr
	}
	c := &Client{
		endpoint: strings.TrimRight(eng.Endpoint, "/"),
		dialect:  d,
		cred:     cred,
		http:     httpClient,
		source:   defaultSource,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// --- statement protocol ---------------------------------------------------

type stmtColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type stmtError struct {
	Message   string `json:"message"`
	ErrorName string `json:"errorName"`
	ErrorCode int    `json:"errorCode"`
}

type stmtResponse struct {
	ID      string       `json:"id"`
	NextURI string       `json:"nextUri"`
	Columns []stmtColumn `json:"columns"`
	Data    [][]any      `json:"data"`
	Error   *stmtError   `json:"error"`
}

// ResultSet is the accumulated result of a read-only statement.
type ResultSet struct {
	Columns []string
	Rows    [][]any
}

func (rs *ResultSet) col(name string) int {
	for i, c := range rs.Columns {
		if strings.EqualFold(c, name) {
			return i
		}
	}
	return -1
}

// firstColumn returns the first column of every row as strings.
func (rs *ResultSet) firstColumn() []string {
	out := make([]string, 0, len(rs.Rows))
	for _, r := range rs.Rows {
		if len(r) > 0 {
			out = append(out, cellString(r[0]))
		}
	}
	return out
}

// runStatement drives POST /v1/statement and follows nextUri to completion,
// returning every row. It is the unbounded path used for fixed metadata
// statements; read queries use collect with a row cap via RunReadQuery.
func (c *Client) runStatement(ctx context.Context, sql, catalog, schema string) (*ResultSet, error) {
	cols, rows, _, err := c.collect(ctx, sql, catalog, schema, 0)
	if err != nil {
		return nil, err
	}
	rs := &ResultSet{Rows: rows, Columns: make([]string, len(cols))}
	for i, col := range cols {
		rs.Columns[i] = col.Name
	}
	return rs, nil
}

// collect drives POST /v1/statement and follows nextUri, accumulating columns and
// rows. When maxRows > 0 it stops once more than maxRows rows have arrived, trims
// to maxRows, reports truncated=true, and best-effort cancels the still-running
// statement on the coordinator (maxRows <= 0 means unbounded).
func (c *Client) collect(ctx context.Context, sql, catalog, schema string, maxRows int) (cols []stmtColumn, rows [][]any, truncated bool, err error) {
	cred, err := c.cred.Resolve(ctx)
	if err != nil {
		return nil, nil, false, fmt.Errorf("resolve credential: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/statement", strings.NewReader(sql))
	if err != nil {
		return nil, nil, false, err
	}
	req.Header.Set("Content-Type", "text/plain")
	c.dialect.apply(req, cred, catalog, schema, c.source)

	for {
		body, err := c.do(req)
		if err != nil {
			// Mid-stream failure (e.g. a timeout): free the server-side query.
			if req.Method == http.MethodGet {
				c.cancelStatement(ctx, req.URL.String(), cred)
			}
			return nil, nil, false, err
		}
		var sr stmtResponse
		if err := decodeJSON(body, &sr); err != nil {
			return nil, nil, false, err
		}
		if sr.Error != nil {
			return nil, nil, false, fmt.Errorf("engine error: %s (%s)", sr.Error.Message, sr.Error.ErrorName)
		}
		if len(cols) == 0 && len(sr.Columns) > 0 {
			cols = sr.Columns
		}
		rows = append(rows, sr.Data...)
		if maxRows > 0 && len(rows) > maxRows {
			rows = rows[:maxRows]
			if sr.NextURI != "" {
				c.cancelStatement(ctx, sr.NextURI, cred)
			}
			return cols, rows, true, nil
		}
		if sr.NextURI == "" {
			return cols, rows, false, nil
		}
		// Follow the next page. Trino requires consuming nextUri to completion.
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, sr.NextURI, nil)
		if err != nil {
			return nil, nil, false, err
		}
		c.dialect.apply(req, cred, catalog, schema, c.source)
	}
}

// do executes a request and returns the body on a 2xx, or an error otherwise.
func (c *Client) do(req *http.Request) ([]byte, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", req.Method, req.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s %s: status %d: %s", req.Method, req.URL, resp.StatusCode, snippet(body))
	}
	return body, nil
}

// getJSON performs a GET against an endpoint path and decodes JSON into v.
func (c *Client) getJSON(ctx context.Context, path string, v any) error {
	cred, err := c.cred.Resolve(ctx)
	if err != nil {
		return fmt.Errorf("resolve credential: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+path, nil)
	if err != nil {
		return err
	}
	c.dialect.apply(req, cred, "", "", c.source)
	body, err := c.do(req)
	if err != nil {
		return err
	}
	return decodeJSON(body, v)
}

// --- metadata -------------------------------------------------------------

// ColumnInfo describes one column from DESCRIBE.
type ColumnInfo struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Extra   string `json:"extra,omitempty"`
	Comment string `json:"comment,omitempty"`
}

// TableStats is the best-effort output of SHOW STATS FOR.
type TableStats struct {
	RowCount *int64        `json:"row_count,omitempty"`
	Columns  []ColumnStats `json:"columns"`
}

// ColumnStats is per-column statistics (any field may be absent).
type ColumnStats struct {
	Column         string   `json:"column"`
	DataSize       *int64   `json:"data_size,omitempty"`
	DistinctValues *float64 `json:"distinct_values,omitempty"`
	NullsFraction  *float64 `json:"nulls_fraction,omitempty"`
	LowValue       string   `json:"low_value,omitempty"`
	HighValue      string   `json:"high_value,omitempty"`
}

// NodeInfo is one row of system.runtime.nodes.
type NodeInfo struct {
	NodeID      string `json:"node_id"`
	HTTPURI     string `json:"http_uri,omitempty"`
	Version     string `json:"version,omitempty"`
	Coordinator bool   `json:"coordinator"`
	State       string `json:"state,omitempty"`
}

// ClusterInfo summarizes the coordinator and the cluster's nodes.
type ClusterInfo struct {
	Version     string     `json:"version"`
	Environment string     `json:"environment,omitempty"`
	Coordinator bool       `json:"coordinator"`
	Uptime      string     `json:"uptime,omitempty"`
	Nodes       []NodeInfo `json:"nodes"`
}

// ListCatalogs returns the catalogs visible to the credential.
func (c *Client) ListCatalogs(ctx context.Context) ([]string, error) {
	rs, err := c.runStatement(ctx, "SHOW CATALOGS", "", "")
	if err != nil {
		return nil, err
	}
	return rs.firstColumn(), nil
}

// ListSchemas returns the schemas in a catalog.
func (c *Client) ListSchemas(ctx context.Context, catalog string) ([]string, error) {
	rs, err := c.runStatement(ctx, "SHOW SCHEMAS FROM "+quoteIdent(catalog), "", "")
	if err != nil {
		return nil, err
	}
	return rs.firstColumn(), nil
}

// ListTables returns the tables in a schema.
func (c *Client) ListTables(ctx context.Context, catalog, schema string) ([]string, error) {
	rs, err := c.runStatement(ctx, "SHOW TABLES FROM "+quoteIdent(catalog)+"."+quoteIdent(schema), "", "")
	if err != nil {
		return nil, err
	}
	return rs.firstColumn(), nil
}

// DescribeTable returns the columns of a table.
func (c *Client) DescribeTable(ctx context.Context, catalog, schema, table string) ([]ColumnInfo, error) {
	rs, err := c.runStatement(ctx, "DESCRIBE "+fqTable(catalog, schema, table), "", "")
	if err != nil {
		return nil, err
	}
	ci, ct := rs.col("Column"), rs.col("Type")
	ce, cc := rs.col("Extra"), rs.col("Comment")
	cols := make([]ColumnInfo, 0, len(rs.Rows))
	for _, r := range rs.Rows {
		col := ColumnInfo{Name: cellAt(r, ci), Type: cellAt(r, ct), Extra: cellAt(r, ce), Comment: cellAt(r, cc)}
		cols = append(cols, col)
	}
	return cols, nil
}

// GetTableStats returns best-effort statistics from SHOW STATS FOR.
func (c *Client) GetTableStats(ctx context.Context, catalog, schema, table string) (*TableStats, error) {
	rs, err := c.runStatement(ctx, "SHOW STATS FOR "+fqTable(catalog, schema, table), "", "")
	if err != nil {
		return nil, err
	}
	colName := rs.col("column_name")
	dataSize := rs.col("data_size")
	distinct := rs.col("distinct_values_count")
	nulls := rs.col("nulls_fraction")
	rowCount := rs.col("row_count")
	low := rs.col("low_value")
	high := rs.col("high_value")

	out := &TableStats{}
	for _, r := range rs.Rows {
		name := cellAt(r, colName)
		if name == "" {
			// Summary row: total row count.
			if rc := parseIntPtr(cellAt(r, rowCount)); rc != nil {
				out.RowCount = rc
			}
			continue
		}
		cs := ColumnStats{
			Column:         name,
			DataSize:       parseIntPtr(cellAt(r, dataSize)),
			DistinctValues: parseFloatPtr(cellAt(r, distinct)),
			NullsFraction:  parseFloatPtr(cellAt(r, nulls)),
			LowValue:       cellAt(r, low),
			HighValue:      cellAt(r, high),
		}
		out.Columns = append(out.Columns, cs)
	}
	return out, nil
}

type infoResponse struct {
	NodeVersion struct {
		Version string `json:"version"`
	} `json:"nodeVersion"`
	Environment string `json:"environment"`
	Coordinator bool   `json:"coordinator"`
	Uptime      string `json:"uptime"`
}

// GetClusterInfo reads /v1/info and the system.runtime.nodes table.
func (c *Client) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	var info infoResponse
	if err := c.getJSON(ctx, "/v1/info", &info); err != nil {
		return nil, err
	}
	ci := &ClusterInfo{
		Version:     info.NodeVersion.Version,
		Environment: info.Environment,
		Coordinator: info.Coordinator,
		Uptime:      info.Uptime,
	}
	rs, err := c.runStatement(ctx,
		"SELECT node_id, http_uri, node_version, coordinator, state FROM system.runtime.nodes", "", "")
	if err != nil {
		// Node listing is best-effort; return what /v1/info gave us.
		return ci, nil //nolint:nilerr
	}
	id, uri := rs.col("node_id"), rs.col("http_uri")
	ver, coord, st := rs.col("node_version"), rs.col("coordinator"), rs.col("state")
	for _, r := range rs.Rows {
		ci.Nodes = append(ci.Nodes, NodeInfo{
			NodeID:      cellAt(r, id),
			HTTPURI:     cellAt(r, uri),
			Version:     cellAt(r, ver),
			Coordinator: cellAt(r, coord) == "true",
			State:       cellAt(r, st),
		})
	}
	return ci, nil
}

// --- query info -----------------------------------------------------------

// ListQueries returns the coordinator's in-memory queries (/v1/query).
func (c *Client) ListQueries(ctx context.Context) ([]BasicQueryInfo, error) {
	var qs []BasicQueryInfo
	if err := c.getJSON(ctx, "/v1/query", &qs); err != nil {
		return nil, err
	}
	return qs, nil
}

// GetQuery returns the full query info for one query (/v1/query/{id}).
func (c *Client) GetQuery(ctx context.Context, queryID string) (*QueryInfo, error) {
	body, err := c.GetQueryRaw(ctx, queryID)
	if err != nil {
		return nil, err
	}
	return DecodeQueryInfo(body)
}

// GetQueryRaw returns the raw /v1/query/{id} response body, used when the caller
// asked for the raw fragment (raw=true) so it can be both decoded and attached.
func (c *Client) GetQueryRaw(ctx context.Context, queryID string) ([]byte, error) {
	cred, err := c.cred.Resolve(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve credential: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.endpoint+"/v1/query/"+queryID, nil)
	if err != nil {
		return nil, err
	}
	c.dialect.apply(req, cred, "", "", c.source)
	return c.do(req)
}

// DecodeQueryInfo decodes a /v1/query/{id} response body into QueryInfo.
func DecodeQueryInfo(body []byte) (*QueryInfo, error) {
	var qi QueryInfo
	if err := decodeJSON(body, &qi); err != nil {
		return nil, err
	}
	return &qi, nil
}

// --- helpers --------------------------------------------------------------

func decodeJSON(body []byte, v any) error {
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("decode response: %w (body: %s)", err, snippet(body))
	}
	return nil
}

func snippet(b []byte) string {
	const maxLen = 256
	s := strings.TrimSpace(string(b))
	if len(s) > maxLen {
		return s[:maxLen] + "…"
	}
	return s
}

// quoteIdent double-quotes an SQL identifier, escaping embedded quotes. This is
// the only place caller-supplied names enter SQL, so quoting is mandatory.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func fqTable(catalog, schema, table string) string {
	return quoteIdent(catalog) + "." + quoteIdent(schema) + "." + quoteIdent(table)
}

// cellString renders a statement cell (string, number, bool, nil) as a string.
func cellString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case json.Number:
		return t.String()
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

func cellAt(row []any, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return cellString(row[idx])
}

func parseIntPtr(s string) *int64 {
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	v := int64(f)
	return &v
}

func parseFloatPtr(s string) *float64 {
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}
