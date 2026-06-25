// Package server wires the registry and tools onto an MCP server and runs it
// over the configured transport. Only the transport is shape-specific; the core
// (registry + tools) is shared. Phase 1 implements stdio; http arrives in Phase 2.
package server

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/registry"
	"github.com/yabinma/presto-mcp/internal/tools"
)

// Version is the server version reported to clients. It is a var so a release
// build can stamp it via -ldflags "-X .../internal/server.Version=v1.2.3".
var Version = "0.1.0-dev"

const instructions = "Read-only access to Presto/Trino engines: list catalogs/schemas/tables, " +
	"describe tables, get table stats, inspect the cluster, and audit queries. " +
	"Start with list_engines to discover engine ids. No arbitrary SQL is exposed."

// New builds an MCP server with all read-only tools registered against reg.
func New(reg *registry.Registry) *mcp.Server {
	s := mcp.NewServer(
		&mcp.Implementation{Name: "presto-mcp", Version: Version},
		&mcp.ServerOptions{Instructions: instructions},
	)
	tools.Register(s, reg)
	return s
}

// Run builds the registry from cfg and serves over the configured transport.
func Run(ctx context.Context, cfg *config.Config) error {
	reg, err := registry.New(cfg, registry.DefaultCredentialFactory, nil)
	if err != nil {
		return err
	}
	s := New(reg)

	switch cfg.Server.Transport {
	case config.TransportStdio:
		return s.Run(ctx, &mcp.StdioTransport{})
	case config.TransportHTTP:
		return runHTTP(ctx, cfg.Server.HTTP, s)
	default:
		return fmt.Errorf("unknown transport %q", cfg.Server.Transport)
	}
}
