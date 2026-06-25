package presto

import (
	"context"
	"testing"

	"github.com/yabinma/presto-mcp/internal/config"
)

func BenchmarkParseDurationMillis(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseDurationMillis("12.34s"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseDataSizeBytes(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := ParseDataSizeBytes("1.50GB"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRunStatement(b *testing.B) {
	fe := newFakeEngine(b)
	c := newTestClient(b, fe, config.DialectTrino, staticCred())
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.ListCatalogs(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateReadOnly(b *testing.B) {
	const sql = "/* report */ WITH t AS (SELECT id, name FROM tpch.tiny.nation) SELECT * FROM t WHERE name <> 'a;b'"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := ValidateReadOnly(sql); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRunReadQuery(b *testing.B) {
	fe := newFakeEngine(b)
	c := newTestClient(b, fe, config.DialectTrino, staticCred())
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := c.RunReadQuery(ctx, "SELECT n, label FROM t /* SELECTOK */", "", "", 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeQueryInfo(b *testing.B) {
	body := []byte(`{"queryId":"q1","state":"FINISHED","session":{"user":"alice"},
		"queryStats":{"elapsedTime":"2.00s","totalCpuTime":"1.00s","operatorSummaries":[
		{"operatorType":"ScanFilter","addInputCpu":"0.5s"}]}}`)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeQueryInfo(body); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClientParallel hits a single shared *Client from many goroutines so
// `go test -race -bench` surfaces data races / contention in the request path.
func BenchmarkClientParallel(b *testing.B) {
	fe := newFakeEngine(b)
	c := newTestClient(b, fe, config.DialectTrino, staticCred())
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, err := c.ListCatalogs(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}
