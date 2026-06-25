package credential

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultResolver_Env(t *testing.T) {
	t.Setenv("MY_TOKEN", "s3cr3t")
	v, err := DefaultResolver("env://MY_TOKEN")
	require.NoError(t, err)
	assert.Equal(t, "s3cr3t", v)

	_, err = DefaultResolver("env://NOT_SET_VAR_XYZ")
	assert.Error(t, err)

	_, err = DefaultResolver("env://")
	assert.Error(t, err)
}

func TestDefaultResolver_File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tok")
	require.NoError(t, os.WriteFile(path, []byte("  filetoken\n"), 0o600))

	v, err := DefaultResolver("file://" + path)
	require.NoError(t, err)
	assert.Equal(t, "filetoken", v, "trimmed")

	_, err = DefaultResolver("file://" + filepath.Join(dir, "nope"))
	assert.Error(t, err)

	_, err = DefaultResolver("file://")
	assert.Error(t, err)
}

func TestDefaultResolver_Unsupported(t *testing.T) {
	_, err := DefaultResolver("vault://secret/x")
	assert.Error(t, err)

	_, err = DefaultResolver("plain-value")
	assert.Error(t, err)
}

func TestNewStatic(t *testing.T) {
	t.Run("requires user", func(t *testing.T) {
		_, err := NewStatic("", "env://X", nil)
		assert.Error(t, err)
	})

	t.Run("no ref means no token", func(t *testing.T) {
		p, err := NewStatic("alice", "", nil)
		require.NoError(t, err)
		c, err := p.Resolve(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "alice", c.User)
		assert.Empty(t, c.Token)
	})

	t.Run("resolves ref via injected resolver", func(t *testing.T) {
		p, err := NewStatic("bob", "custom://thing", func(ref string) (string, error) {
			assert.Equal(t, "custom://thing", ref)
			return "tok123", nil
		})
		require.NoError(t, err)
		c, _ := p.Resolve(context.Background())
		assert.Equal(t, "bob", c.User)
		assert.Equal(t, "tok123", c.Token)
	})

	t.Run("resolver error fails construction", func(t *testing.T) {
		_, err := NewStatic("bob", "env://MISSING_XYZ", nil)
		assert.Error(t, err)
	})

	t.Run("default resolver used when nil", func(t *testing.T) {
		t.Setenv("DEF_TOK", "abc")
		p, err := NewStatic("u", "env://DEF_TOK", nil)
		require.NoError(t, err)
		c, _ := p.Resolve(context.Background())
		assert.Equal(t, "abc", c.Token)
	})
}

func TestNewBasic(t *testing.T) {
	t.Run("requires user", func(t *testing.T) {
		_, err := NewBasic("", "env://P", nil)
		assert.Error(t, err)
	})

	t.Run("requires password ref", func(t *testing.T) {
		_, err := NewBasic("alice", "", nil)
		assert.Error(t, err)
	})

	t.Run("resolves password via injected resolver", func(t *testing.T) {
		p, err := NewBasic("alice", "custom://pw", func(ref string) (string, error) {
			assert.Equal(t, "custom://pw", ref)
			return "s3cret", nil
		})
		require.NoError(t, err)
		c, _ := p.Resolve(context.Background())
		assert.Equal(t, "alice", c.User)
		assert.Equal(t, "s3cret", c.Password)
		assert.Empty(t, c.Token, "basic must not set a bearer token")
	})

	t.Run("resolver error fails construction", func(t *testing.T) {
		_, err := NewBasic("alice", "env://MISSING_PW_XYZ", nil)
		assert.Error(t, err)
	})

	t.Run("default resolver used when nil", func(t *testing.T) {
		t.Setenv("BASIC_PW", "pw1")
		p, err := NewBasic("alice", "env://BASIC_PW", nil)
		require.NoError(t, err)
		c, _ := p.Resolve(context.Background())
		assert.Equal(t, "pw1", c.Password)
	})
}

func TestPassthroughProvider(t *testing.T) {
	t.Run("forwards the caller's authorization header verbatim", func(t *testing.T) {
		p := NewPassthrough("")
		ctx := WithCaller(context.Background(), Caller{AuthHeader: "Bearer abc.def.ghi"})
		c, err := p.Resolve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "Bearer abc.def.ghi", c.AuthHeader)
		assert.Empty(t, c.Token, "passthrough must not populate Token")
		assert.Empty(t, c.Password)
		assert.Empty(t, c.User, "no verified or fallback user means the engine derives identity")
	})

	t.Run("preserves a basic authorization header verbatim", func(t *testing.T) {
		p := NewPassthrough("")
		ctx := WithCaller(context.Background(), Caller{AuthHeader: "Basic dXNlcjpwYXNz"})
		c, err := p.Resolve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "Basic dXNlcjpwYXNz", c.AuthHeader)
	})

	t.Run("verified user takes precedence over the configured fallback", func(t *testing.T) {
		p := NewPassthrough("svc-account")
		ctx := WithCaller(context.Background(), Caller{AuthHeader: "Bearer t", VerifiedUser: "alice"})
		c, err := p.Resolve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "alice", c.User)
	})

	t.Run("falls back to the configured user when none is verified", func(t *testing.T) {
		p := NewPassthrough("svc-account")
		ctx := WithCaller(context.Background(), Caller{AuthHeader: "Bearer t"})
		c, err := p.Resolve(ctx)
		require.NoError(t, err)
		assert.Equal(t, "svc-account", c.User)
	})

	t.Run("errors when the context carries no caller", func(t *testing.T) {
		p := NewPassthrough("")
		_, err := p.Resolve(context.Background())
		assert.Error(t, err)
	})

	t.Run("errors when the caller has no authorization header", func(t *testing.T) {
		p := NewPassthrough("")
		ctx := WithCaller(context.Background(), Caller{})
		_, err := p.Resolve(ctx)
		assert.Error(t, err)
	})
}

func TestCallerFromContext(t *testing.T) {
	_, ok := CallerFromContext(context.Background())
	assert.False(t, ok, "no caller on a bare context")

	want := Caller{AuthHeader: "Bearer x", VerifiedUser: "bob"}
	got, ok := CallerFromContext(WithCaller(context.Background(), want))
	require.True(t, ok)
	assert.Equal(t, want, got)
}

// ensure both providers satisfy Provider.
var (
	_ Provider = (*StaticProvider)(nil)
	_ Provider = (*PassthroughProvider)(nil)
)
