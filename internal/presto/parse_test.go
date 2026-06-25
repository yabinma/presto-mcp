package presto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDurationMillis(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"", 0},
		{"0.00ns", 0},
		{"1.00ms", 1},
		{"2.50s", 2500},
		{"1.00m", 60000},
		{"1.00h", 3600000},
		{"1.00d", 86400000},
		{"1000us", 1},
	}
	for _, c := range cases {
		got, err := ParseDurationMillis(c.in)
		require.NoError(t, err, c.in)
		assert.InDelta(t, c.want, got, 0.001, c.in)
	}
}

func TestParseDurationMillis_Bad(t *testing.T) {
	_, err := ParseDurationMillis("1.0lightyears")
	assert.Error(t, err)
	_, err = ParseDurationMillis("abcs")
	assert.Error(t, err)
	_, err = ParseDurationMillis("123")
	assert.Error(t, err, "missing unit")
}

func TestParseDataSizeBytes(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"", 0},
		{"0B", 0},
		{"512B", 512},
		{"1kB", 1024},
		{"1.00MB", 1024 * 1024},
		{"2.00GB", 2 * 1024 * 1024 * 1024},
		{"1TB", 1 << 40},
		{"1PB", 1 << 50},
	}
	for _, c := range cases {
		got, err := ParseDataSizeBytes(c.in)
		require.NoError(t, err, c.in)
		assert.Equal(t, c.want, got, c.in)
	}
}

func TestParseDataSizeBytes_Bad(t *testing.T) {
	_, err := ParseDataSizeBytes("10ZB")
	assert.Error(t, err)
	_, err = ParseDataSizeBytes("xxGB")
	assert.Error(t, err)
}
