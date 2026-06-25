package presto

import (
	"fmt"
	"strconv"
	"strings"
)

// Presto and Trino serialize io.airlift Duration and DataSize values as
// human-readable strings (for example "1.23s", "4.50ms", "2.00GB"). These
// helpers parse them back into machine units.

var durationUnitsNanos = map[string]float64{
	"ns": 1,
	"us": 1e3,
	"ms": 1e6,
	"s":  1e9,
	"m":  60e9,
	"h":  3600e9,
	"d":  86400e9,
}

var dataSizeUnitsBytes = map[string]float64{
	"B":  1,
	"kB": 1 << 10,
	"MB": 1 << 20,
	"GB": 1 << 30,
	"TB": 1 << 40,
	"PB": 1 << 50,
}

// splitValueUnit separates the leading numeric part from the trailing unit
// (the longest run of trailing ASCII letters).
func splitValueUnit(s string) (float64, string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, "", nil
	}
	i := len(s)
	for i > 0 {
		c := s[i-1]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
			i--
			continue
		}
		break
	}
	num, unit := s[:i], s[i:]
	if unit == "" {
		return 0, "", fmt.Errorf("missing unit in %q", s)
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid numeric part in %q: %w", s, err)
	}
	return v, unit, nil
}

// ParseDurationMillis parses a Presto/Trino duration string into milliseconds.
// An empty string yields 0 with no error (the field was simply absent).
func ParseDurationMillis(s string) (float64, error) {
	v, unit, err := splitValueUnit(s)
	if err != nil || unit == "" {
		return 0, err
	}
	factor, ok := durationUnitsNanos[unit]
	if !ok {
		return 0, fmt.Errorf("unknown duration unit %q", unit)
	}
	return v * factor / 1e6, nil
}

// ParseDataSizeBytes parses a Presto/Trino data-size string into bytes.
// An empty string yields 0 with no error.
func ParseDataSizeBytes(s string) (int64, error) {
	v, unit, err := splitValueUnit(s)
	if err != nil || unit == "" {
		return 0, err
	}
	factor, ok := dataSizeUnitsBytes[unit]
	if !ok {
		return 0, fmt.Errorf("unknown data-size unit %q", unit)
	}
	return int64(v * factor), nil
}
