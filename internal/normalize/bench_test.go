package normalize

import (
	"encoding/json"
	"testing"

	"github.com/yabinma/presto-mcp/internal/presto"
)

func BenchmarkQueryDetailFromLive(b *testing.B) {
	qi, err := presto.DecodeQueryInfo([]byte(fullQuery))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = QueryDetailFromLive(qi, false, nil)
	}
}

func BenchmarkQueryListFromLive(b *testing.B) {
	var items []presto.BasicQueryInfo
	raw := `[{"queryId":"q1","state":"FINISHED","sessionUser":"alice","query":"a","queryStats":{"createTime":"2026-06-24T10:00:00Z","elapsedTime":"1.00s"}},
		{"queryId":"q2","state":"RUNNING","sessionUser":"bob","query":"b","queryStats":{"createTime":"2026-06-24T12:00:00Z","elapsedTime":"2.00s"}}]`
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		b.Fatal(err)
	}
	f := Filter{State: "FINISHED"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = QueryListFromLive(items, f)
	}
}
