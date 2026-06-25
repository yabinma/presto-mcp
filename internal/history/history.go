// Package history defines the pluggable query-history provider interface.
//
// History is a secondary, read-only data source: the server does not hook the
// engine's event-listener plugin, it reads whatever sink the listener wrote to.
// Phase 1 ships the interface only — with no configured sink, queries come from
// the coordinator's memory via the Presto client. Concrete providers
// (mysql_event_listener, qpmm, ...) arrive in Phase 2.
package history

import (
	"context"

	"github.com/yabinma/presto-mcp/internal/normalize"
)

// Provider reads query history from a configured sink. Returned records use the
// same normalized shapes as the live path so tools are source-agnostic.
type Provider interface {
	// Name identifies the provider (matches the config "provider" value).
	Name() string
	// ListQueries returns history records matching the filter.
	ListQueries(ctx context.Context, f normalize.Filter) ([]normalize.QueryListItem, error)
	// GetQuery returns one query's detail, or ErrNotFound if absent. The detail's
	// Source is normalize.SourceHistory and AvailableSections reflects what the
	// sink persisted (commonly only "summary").
	GetQuery(ctx context.Context, queryID string) (*normalize.QueryDetail, error)
}

// ErrNotFound is returned by GetQuery when the query is not in the sink.
type ErrNotFound struct{ QueryID string }

func (e *ErrNotFound) Error() string { return "query not found in history: " + e.QueryID }
