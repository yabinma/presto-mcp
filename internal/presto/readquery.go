package presto

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/yabinma/presto-mcp/internal/credential"
)

// Read-query result caps. run_query is deliberately bounded so a result set can
// never blow up the agent's context: the row count is clamped to MaxResultRows
// and defaults to DefaultMaxRows when the caller does not ask for a specific cap.
const (
	DefaultMaxRows = 1000
	MaxResultRows  = 10000
)

// Column is one output column of a read query (name + engine type).
type Column struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

// QueryResult is the bounded result of a read-only query. Truncated is true when
// the engine had more rows than the cap allowed and the rest were dropped.
type QueryResult struct {
	Columns   []Column `json:"columns"`
	Rows      [][]any  `json:"rows"`
	Truncated bool     `json:"truncated"`
}

// readOnlyKeywords is the allowlist of leading statement keywords run_query will
// run. Everything else (writes, DDL, CALL, SET, USE, transaction control, ...) is
// rejected. EXPLAIN is handled separately so its inner statement is re-checked.
var readOnlyKeywords = map[string]bool{
	"SELECT":   true,
	"WITH":     true, // top-level WITH resolves to a query in Presto/Trino
	"VALUES":   true,
	"TABLE":    true, // "TABLE t" is shorthand for "SELECT * FROM t"
	"SHOW":     true,
	"DESCRIBE": true,
	"DESC":     true,
}

// ValidateReadOnly rejects any statement run_query must not execute. It is a
// best-effort guard layered on top of the engine's own access control: it allows
// only a single statement whose leading keyword is read-only (see readOnlyKeywords),
// recursing through EXPLAIN. It never interpolates or rewrites the SQL — the
// statement is sent to the engine verbatim once it passes.
func ValidateReadOnly(sql string) error {
	trimmed := strings.TrimSpace(sql)
	if trimmed == "" {
		return errors.New("run_query: empty statement")
	}
	// Reject multiple statements: anything but trivia after a top-level ';'.
	if idx := topLevelSemicolon(trimmed); idx >= 0 {
		if stripLeadingTrivia(trimmed[idx+1:]) != "" {
			return errors.New("run_query: only a single statement may be run")
		}
		trimmed = strings.TrimSpace(trimmed[:idx])
		if trimmed == "" {
			return errors.New("run_query: empty statement")
		}
	}
	return checkStatement(trimmed, 0)
}

// checkStatement verifies the leading keyword of stmt is read-only. For EXPLAIN it
// strips the EXPLAIN options and re-checks the inner statement (EXPLAIN ANALYSE
// actually executes it), guarding against nesting via depth.
func checkStatement(stmt string, depth int) error {
	kw, rest := leadingKeyword(stmt)
	if kw == "" {
		return errors.New("run_query: could not determine the statement type")
	}
	if kw == "EXPLAIN" {
		if depth > 0 {
			return errors.New("run_query: nested EXPLAIN is not allowed")
		}
		return checkStatement(stripExplainOptions(rest), depth+1)
	}
	if !readOnlyKeywords[kw] {
		return fmt.Errorf("run_query is read-only; %s statements are not allowed", kw)
	}
	return nil
}

// RunReadQuery validates sql as read-only, runs it with optional session
// catalog/schema, and returns at most maxRows rows (clamped to the result caps).
// Validation happens here, not just in the tool, so the client cannot be misused.
func (c *Client) RunReadQuery(ctx context.Context, sql, catalog, schema string, maxRows int) (*QueryResult, error) {
	if err := ValidateReadOnly(sql); err != nil {
		return nil, err
	}
	cols, rows, truncated, err := c.collect(ctx, sql, catalog, schema, clampMaxRows(maxRows))
	if err != nil {
		return nil, err
	}
	out := &QueryResult{Truncated: truncated, Rows: rows, Columns: make([]Column, len(cols))}
	for i, col := range cols {
		out.Columns[i] = Column(col)
	}
	if out.Rows == nil {
		out.Rows = [][]any{}
	}
	return out, nil
}

func clampMaxRows(n int) int {
	switch {
	case n <= 0:
		return DefaultMaxRows
	case n > MaxResultRows:
		return MaxResultRows
	default:
		return n
	}
}

// --- lexical helpers ------------------------------------------------------

// topLevelSemicolon returns the index of the first ';' that acts as a statement
// separator — one not inside a string literal, quoted identifier, or comment —
// or -1 if there is none.
func topLevelSemicolon(s string) int {
	for i := 0; i < len(s); {
		switch s[i] {
		case '\'':
			i = skipQuoted(s, i, '\'')
		case '"':
			i = skipQuoted(s, i, '"')
		case '-':
			if i+1 < len(s) && s[i+1] == '-' {
				i = skipLineComment(s, i)
			} else {
				i++
			}
		case '/':
			if i+1 < len(s) && s[i+1] == '*' {
				i = skipBlockComment(s, i)
			} else {
				i++
			}
		case ';':
			return i
		default:
			i++
		}
	}
	return -1
}

// stripLeadingTrivia removes leading whitespace and SQL comments (line and block).
func stripLeadingTrivia(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n\f\v")
		switch {
		case strings.HasPrefix(s, "--"):
			s = s[skipLineComment(s, 0):]
		case strings.HasPrefix(s, "/*"):
			s = s[skipBlockComment(s, 0):]
		default:
			return s
		}
	}
}

// leadingKeyword strips leading trivia and returns the first keyword (uppercased)
// together with the remainder of the string after it.
func leadingKeyword(s string) (kw, rest string) {
	s = stripLeadingTrivia(s)
	word, after := leadingWord(s)
	if word == "" {
		return "", s
	}
	return strings.ToUpper(word), after
}

// leadingWord returns the run of leading word bytes (letters/underscore) and the
// rest. It does not strip trivia; callers strip first when needed.
func leadingWord(s string) (word, rest string) {
	i := 0
	for i < len(s) && isWordByte(s[i]) {
		i++
	}
	return s[:i], s[i:]
}

// stripExplainOptions consumes the optional ANALYZE/VERBOSE words and a "(...)"
// option group that may follow EXPLAIN, returning the inner statement.
func stripExplainOptions(s string) string {
	for {
		s = stripLeadingTrivia(s)
		if strings.HasPrefix(s, "(") {
			s = skipParenGroup(s)
			continue
		}
		word, after := leadingWord(s)
		switch strings.ToUpper(word) {
		case "ANALYZE", "VERBOSE":
			s = after
			continue
		}
		return s
	}
}

func isWordByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// skipQuoted returns the index just past a string/identifier opened at i with the
// quote byte q, treating a doubled quote byte as an escaped (literal) quote.
func skipQuoted(s string, i int, q byte) int {
	i++ // past the opening quote
	for i < len(s) {
		if s[i] == q {
			if i+1 < len(s) && s[i+1] == q {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i // unterminated: treat the rest as consumed
}

func skipLineComment(s string, i int) int {
	for i < len(s) && s[i] != '\n' {
		i++
	}
	return i
}

func skipBlockComment(s string, i int) int {
	i += 2 // past "/*"
	for i+1 < len(s) {
		if s[i] == '*' && s[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return len(s) // unterminated
}

// skipParenGroup returns the substring after the balanced parenthesis group that
// s begins with, respecting single-quoted strings inside. Returns "" if unbalanced.
func skipParenGroup(s string) string {
	depth := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '\'':
			i = skipQuoted(s, i, '\'')
		case '(':
			depth++
			i++
		case ')':
			depth--
			i++
			if depth == 0 {
				return s[i:]
			}
		default:
			i++
		}
	}
	return ""
}

// cancelStatement best-effort frees a running statement on the coordinator by
// DELETE-ing its nextUri (Presto/Trino abandon the query). Errors are ignored, and
// a detached context ensures cancellation still runs after a timeout/cancel.
func (c *Client) cancelStatement(ctx context.Context, nextURI string, cred credential.Credential) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodDelete, nextURI, nil)
	if err != nil {
		return
	}
	c.dialect.apply(req, cred, "", "", c.source)
	resp, err := c.http.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}
