package main

import (
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withArgs resets the global flag state so run() can be exercised repeatedly.
func withArgs(args ...string) func() {
	oldArgs, oldFS := os.Args, flag.CommandLine
	os.Args = append([]string{"presto-mcp"}, args...)
	flag.CommandLine = flag.NewFlagSet("presto-mcp", flag.ContinueOnError)
	return func() { os.Args, flag.CommandLine = oldArgs, oldFS }
}

func TestRun_Version(t *testing.T) {
	defer withArgs("--version")()
	assert.NoError(t, run())
}

func TestRun_BadConfigPath(t *testing.T) {
	defer withArgs("--config", "/no/such/file.yaml")()
	assert.Error(t, run())
}

func TestRun_InvalidConfig(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/c.yaml"
	require.NoError(t, os.WriteFile(path, []byte("deployment_mode: bogus\nengines: []\n"), 0o600))
	defer withArgs("--config", path)()
	assert.Error(t, run())
}
