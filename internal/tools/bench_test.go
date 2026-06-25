package tools

import (
	"testing"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
	"github.com/yabinma/presto-mcp/internal/registry"
)

func BenchmarkGetQueryLive(b *testing.B) {
	s := fakeEngineServer(b)
	reg := testRegistry(b, s.URL, false)
	h := getQuery(reg)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := h(ctx(), nil, getQueryInput{Engine: "e", QueryID: "q1"}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkListCatalogsParallel drives the tool handler from many goroutines
// over a shared registry + client so `go test -race -bench` catches contention.
func BenchmarkListCatalogsParallel(b *testing.B) {
	s := fakeEngineServer(b)
	reg := testRegistry(b, s.URL, false)
	h := listCatalogs(reg)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := h(ctx(), nil, EngineInput{Engine: "e"}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkRunQuery(b *testing.B) {
	s := fakeEngineServer(b)
	reg := testRegistry(b, s.URL, false)
	h := runQuery(reg)
	in := runQueryInput{Engine: "e", SQL: "SELECT n /* SELECTOK */"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := h(ctx(), nil, in); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPassthroughParallel drives a handler backed by a passthrough engine
// from many goroutines, each carrying a caller credential on the context, so
// `go test -race -bench` surfaces contention on the per-request credential path
// (caller context -> PassthroughProvider.Resolve -> dialect.apply).
func BenchmarkPassthroughParallel(b *testing.B) {
	s := fakeEngineServer(b)
	ec := config.EngineConfig{
		ID: "e", Endpoint: s.URL, Dialect: config.DialectTrino,
		Auth: config.AuthConfig{Mode: config.AuthPassthrough},
	}
	cfg := &config.Config{DeploymentMode: config.ModeEnterprise, Engines: []config.EngineConfig{ec}}
	reg, err := registry.New(cfg, registry.DefaultCredentialFactory, nil)
	if err != nil {
		b.Fatal(err)
	}
	h := listCatalogs(reg)
	pctx := credential.WithCaller(ctx(), credential.Caller{AuthHeader: "Bearer bench.jwt"})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := h(pctx, nil, EngineInput{Engine: "e"}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRunQueryParallel drives run_query from many goroutines over a shared
// registry + client so `go test -race -bench` surfaces contention on the read path.
func BenchmarkRunQueryParallel(b *testing.B) {
	s := fakeEngineServer(b)
	reg := testRegistry(b, s.URL, false)
	h := runQuery(reg)
	in := runQueryInput{Engine: "e", SQL: "SELECT n /* SELECTOK */"}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := h(ctx(), nil, in); err != nil {
				b.Fatal(err)
			}
		}
	})
}
