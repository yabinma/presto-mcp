package registry

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yabinma/presto-mcp/internal/config"
	"github.com/yabinma/presto-mcp/internal/credential"
	"github.com/yabinma/presto-mcp/internal/history"
	"github.com/yabinma/presto-mcp/internal/normalize"
)

func staticCfg(engines ...config.EngineConfig) *config.Config {
	return &config.Config{DeploymentMode: config.ModeLocal, Engines: engines}
}

func eng(id string, d config.Dialect) config.EngineConfig {
	return config.EngineConfig{ID: id, Endpoint: "http://h:8080", Dialect: d, Auth: config.AuthConfig{Mode: config.AuthStatic, User: "u"}}
}

func TestNew_BuildsEnginesInOrder(t *testing.T) {
	cfg := staticCfg(eng("a", config.DialectPresto), eng("b", config.DialectTrino))
	r, err := New(cfg, DefaultCredentialFactory, nil)
	require.NoError(t, err)

	assert.Equal(t, []string{"a", "b"}, r.IDs())
	assert.Len(t, r.List(), 2)

	e, ok := r.Get("a")
	require.True(t, ok)
	assert.Equal(t, config.DialectPresto, e.Config.Dialect)
	assert.NotNil(t, e.Client)
	assert.Nil(t, e.History)

	_, ok = r.Get("missing")
	assert.False(t, ok)
}

func TestNew_RequiresCredFactory(t *testing.T) {
	_, err := New(staticCfg(eng("a", config.DialectPresto)), nil, nil)
	assert.Error(t, err)
}

func TestNew_CredFactoryError(t *testing.T) {
	cfg := staticCfg(eng("a", config.DialectPresto))
	_, err := New(cfg, func(config.EngineConfig) (credential.Provider, error) {
		return nil, fmt.Errorf("nope")
	}, nil)
	assert.Error(t, err)
}

func TestNew_ClientBuildError(t *testing.T) {
	// wxd is rejected by the presto client.
	cfg := staticCfg(eng("a", config.DialectWxd))
	_, err := New(cfg, DefaultCredentialFactory, nil)
	assert.Error(t, err)
}

func TestNew_HistoryEnabledWithoutProviderFails(t *testing.T) {
	e := eng("a", config.DialectPresto)
	e.History = config.HistoryConfig{Enabled: true, Provider: "mysql_event_listener"}
	_, err := New(staticCfg(e), DefaultCredentialFactory, nil)
	assert.Error(t, err)
}

type fakeHistory struct{}

func (fakeHistory) Name() string { return "fake" }
func (fakeHistory) ListQueries(context.Context, normalize.Filter) ([]normalize.QueryListItem, error) {
	return nil, nil
}
func (fakeHistory) GetQuery(context.Context, string) (*normalize.QueryDetail, error) { return nil, nil }

func TestNew_CustomHistoryFactory(t *testing.T) {
	e := eng("a", config.DialectPresto)
	e.History = config.HistoryConfig{Enabled: true, Provider: "fake"}
	r, err := New(staticCfg(e), DefaultCredentialFactory, func(config.EngineConfig) (history.Provider, error) {
		return fakeHistory{}, nil
	})
	require.NoError(t, err)
	got, _ := r.Get("a")
	assert.NotNil(t, got.History)
}

func TestNew_HistoryFactoryError(t *testing.T) {
	e := eng("a", config.DialectPresto)
	_, err := New(staticCfg(e), DefaultCredentialFactory, func(config.EngineConfig) (history.Provider, error) {
		return nil, fmt.Errorf("boom")
	})
	assert.Error(t, err)
}

func TestDefaultCredentialFactory(t *testing.T) {
	t.Setenv("REG_TOK", "x")
	ec := eng("a", config.DialectPresto)
	ec.Auth.CredentialRef = "env://REG_TOK"
	p, err := DefaultCredentialFactory(ec)
	require.NoError(t, err)
	assert.NotNil(t, p)

	ec.Auth.Mode = config.AuthPassthrough
	p, err = DefaultCredentialFactory(ec)
	require.NoError(t, err, "passthrough is available in the enterprise build")
	assert.NotNil(t, p)

	ec.Auth.Mode = "weird"
	_, err = DefaultCredentialFactory(ec)
	assert.Error(t, err)
}

func TestDefaultCredentialFactory_Basic(t *testing.T) {
	t.Setenv("REG_PW", "pw")
	ec := eng("a", config.DialectTrino)
	ec.Auth.Scheme = config.SchemeBasic
	ec.Auth.PasswordRef = "env://REG_PW"
	p, err := DefaultCredentialFactory(ec)
	require.NoError(t, err)
	assert.NotNil(t, p)

	ec.Auth.PasswordRef = "" // basic without a password ref fails
	_, err = DefaultCredentialFactory(ec)
	assert.Error(t, err)

	ec.Auth.Scheme = "weird"
	_, err = DefaultCredentialFactory(ec)
	assert.Error(t, err)
}
