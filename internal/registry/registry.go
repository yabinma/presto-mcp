// Package registry builds the set of engines from config: a Presto client and
// an optional history provider per engine, addressed by the engine's logical id.
package registry

import (
	"fmt"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
	"github.com/yabinma/presto-mcp/internal/history"
	"github.com/yabinma/presto-mcp/internal/presto"
)

// Engine is one configured engine and its constructed dependencies.
type Engine struct {
	Config  config.EngineConfig
	Client  *presto.Client
	History history.Provider // nil when no sink is configured
}

// CredentialFactory builds a credential provider for an engine. Injectable so
// the passthrough strategy can be swapped in for the enterprise shape (Phase 2).
type CredentialFactory func(config.EngineConfig) (credential.Provider, error)

// HistoryFactory builds a history provider for an engine, or returns nil when
// history is disabled. Injectable so Phase 2 can register concrete providers.
type HistoryFactory func(config.EngineConfig) (history.Provider, error)

// Registry holds engines in config order, indexed by id.
type Registry struct {
	order []string
	byID  map[string]*Engine
}

// New constructs a registry. credFactory is required; historyFactory may be nil,
// in which case a sink-enabled engine is an error (Phase 1 has no providers).
func New(cfg *config.Config, credFactory CredentialFactory, historyFactory HistoryFactory) (*Registry, error) {
	if credFactory == nil {
		return nil, fmt.Errorf("credential factory is required")
	}
	if historyFactory == nil {
		historyFactory = noHistory
	}
	r := &Registry{byID: make(map[string]*Engine, len(cfg.Engines))}
	for _, ec := range cfg.Engines {
		cred, err := credFactory(ec)
		if err != nil {
			return nil, fmt.Errorf("engine %q: credentials: %w", ec.ID, err)
		}
		client, err := presto.NewClient(ec, cred)
		if err != nil {
			return nil, fmt.Errorf("engine %q: client: %w", ec.ID, err)
		}
		hist, err := historyFactory(ec)
		if err != nil {
			return nil, fmt.Errorf("engine %q: history: %w", ec.ID, err)
		}
		r.byID[ec.ID] = &Engine{Config: ec, Client: client, History: hist}
		r.order = append(r.order, ec.ID)
	}
	return r, nil
}

// Get returns the engine with the given id.
func (r *Registry) Get(id string) (*Engine, bool) {
	e, ok := r.byID[id]
	return e, ok
}

// IDs returns the engine ids in config order.
func (r *Registry) IDs() []string {
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// List returns the engines in config order.
func (r *Registry) List() []*Engine {
	out := make([]*Engine, 0, len(r.order))
	for _, id := range r.order {
		out = append(out, r.byID[id])
	}
	return out
}

// DefaultCredentialFactory builds providers for both shapes: static mode
// resolves config refs (bearer token or basic password by scheme); passthrough
// mode forwards the caller's credential, extracted per request from the context.
func DefaultCredentialFactory(ec config.EngineConfig) (credential.Provider, error) {
	switch ec.Auth.Mode {
	case config.AuthStatic:
		switch ec.Auth.Scheme {
		case config.SchemeBasic:
			return credential.NewBasic(ec.Auth.User, ec.Auth.PasswordRef, nil)
		case config.SchemeBearer, "":
			return credential.NewStatic(ec.Auth.User, ec.Auth.CredentialRef, nil)
		default:
			return nil, fmt.Errorf("unknown auth scheme %q", ec.Auth.Scheme)
		}
	case config.AuthPassthrough:
		return credential.NewPassthrough(ec.Auth.User), nil
	default:
		return nil, fmt.Errorf("unknown auth mode %q", ec.Auth.Mode)
	}
}

// noHistory rejects sink-enabled engines (no concrete providers in Phase 1).
func noHistory(ec config.EngineConfig) (history.Provider, error) {
	if ec.History.Enabled {
		return nil, fmt.Errorf("history provider %q is not available in this build (Phase 2); set history.enabled=false to use coordinator memory", ec.History.Provider)
	}
	return nil, nil
}
