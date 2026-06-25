package presto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateReadOnly(t *testing.T) {
	accept := []struct {
		name string
		sql  string
	}{
		{"select", "SELECT 1"},
		{"select lower", "select * from t"},
		{"leading whitespace", "   \n\t SELECT 1"},
		{"line comment", "-- a comment\nSELECT 1"},
		{"block comment", "/* hello */ SELECT 1"},
		{"nested-ish block comment then select", "/* multi\nline */\nSELECT count(*) FROM x"},
		{"cte", "WITH t AS (SELECT 1) SELECT * FROM t"},
		{"values", "VALUES (1), (2)"},
		{"table shorthand", "TABLE tpch.tiny.nation"},
		{"show", "SHOW SCHEMAS FROM tpch"},
		{"describe", "DESCRIBE tpch.tiny.nation"},
		{"desc", "DESC tpch.tiny.nation"},
		{"explain", "EXPLAIN SELECT 1"},
		{"explain analyze select", "EXPLAIN ANALYZE SELECT 1"},
		{"explain analyze verbose select", "EXPLAIN ANALYZE VERBOSE SELECT 1"},
		{"explain with options", "EXPLAIN (TYPE LOGICAL) SELECT 1"},
		{"trailing semicolon", "SELECT 1;"},
		{"trailing semicolon and whitespace", "SELECT 1 ;  \n"},
		{"trailing semicolon and comment", "SELECT 1; -- done"},
		{"semicolon inside string literal", "SELECT 'a;b' AS s"},
		{"semicolon inside quoted identifier", "SELECT 1 AS \"a;b\""},
		{"keyword case mixed", "SeLeCt 1"},
	}
	for _, tc := range accept {
		t.Run("accept/"+tc.name, func(t *testing.T) {
			assert.NoError(t, ValidateReadOnly(tc.sql))
		})
	}

	reject := []struct {
		name string
		sql  string
	}{
		{"empty", ""},
		{"whitespace only", "   \n\t "},
		{"comment only", "-- just a comment"},
		{"insert", "INSERT INTO t VALUES (1)"},
		{"update", "UPDATE t SET a = 1"},
		{"delete", "DELETE FROM t"},
		{"merge", "MERGE INTO t USING s ON t.id = s.id"},
		{"create table", "CREATE TABLE t (a int)"},
		{"create table as select", "CREATE TABLE t AS SELECT 1"},
		{"drop", "DROP TABLE t"},
		{"alter", "ALTER TABLE t ADD COLUMN b int"},
		{"truncate", "TRUNCATE TABLE t"},
		{"call", "CALL system.runtime.kill_query('q')"},
		{"grant", "GRANT SELECT ON t TO alice"},
		{"set session", "SET SESSION foo = 'bar'"},
		{"use", "USE tpch.tiny"},
		{"start transaction", "START TRANSACTION"},
		{"commit", "COMMIT"},
		{"two statements", "SELECT 1; SELECT 2"},
		{"two statements with write", "SELECT 1; DROP TABLE t"},
		{"write hidden after select comment", "SELECT 1; /* x */ DELETE FROM t"},
		{"explain analyze insert", "EXPLAIN ANALYZE INSERT INTO t VALUES (1)"},
		{"explain options then write", "EXPLAIN (TYPE DISTRIBUTED) DELETE FROM t"},
		{"leading comment then write", "/* read please */ DROP TABLE t"},
	}
	for _, tc := range reject {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			assert.Error(t, ValidateReadOnly(tc.sql))
		})
	}
}

func TestValidateReadOnly_ErrorMentionsKeyword(t *testing.T) {
	err := ValidateReadOnly("DELETE FROM t")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DELETE")
}

func TestClampMaxRows(t *testing.T) {
	assert.Equal(t, DefaultMaxRows, clampMaxRows(0))
	assert.Equal(t, DefaultMaxRows, clampMaxRows(-5))
	assert.Equal(t, 42, clampMaxRows(42))
	assert.Equal(t, MaxResultRows, clampMaxRows(MaxResultRows+1))
	assert.Equal(t, MaxResultRows, clampMaxRows(1<<20))
}
