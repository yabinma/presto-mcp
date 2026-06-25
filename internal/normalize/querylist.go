package normalize

import (
	"strings"
	"time"

	"github.com/yabinma/presto-mcp/internal/presto"
)

// Filter narrows a query listing. Zero-value fields are not applied. It lives
// here (not in history) so both the live and history paths share it without an
// import cycle.
type Filter struct {
	State string
	User  string
	Since *time.Time
	Until *time.Time
}

// QueryListItem is one row of list_queries.
type QueryListItem struct {
	QueryID       string  `json:"query_id"`
	State         string  `json:"state"`
	User          string  `json:"user,omitempty"`
	Query         string  `json:"query,omitempty"`
	CreateTime    string  `json:"create_time,omitempty"`
	EndTime       string  `json:"end_time,omitempty"`
	ElapsedMillis float64 `json:"elapsed_ms,omitempty"`
}

// QueryListFromLive normalizes and filters coordinator query records.
func QueryListFromLive(items []presto.BasicQueryInfo, f Filter) []QueryListItem {
	out := make([]QueryListItem, 0, len(items))
	for _, q := range items {
		if !f.matches(q.State, q.User(), q.QueryStats.CreateTime) {
			continue
		}
		out = append(out, QueryListItem{
			QueryID:       q.QueryID,
			State:         q.State,
			User:          q.User(),
			Query:         q.Query,
			CreateTime:    q.QueryStats.CreateTime,
			EndTime:       q.QueryStats.EndTime,
			ElapsedMillis: millis(q.QueryStats.ElapsedTime),
		})
	}
	return out
}

func (f Filter) matches(state, user, createTime string) bool {
	if f.State != "" && !strings.EqualFold(f.State, state) {
		return false
	}
	if f.User != "" && f.User != user {
		return false
	}
	if f.Since != nil || f.Until != nil {
		t, ok := parseEngineTime(createTime)
		if !ok {
			// Cannot place this record in time; don't exclude it.
			return true
		}
		if f.Since != nil && t.Before(*f.Since) {
			return false
		}
		if f.Until != nil && t.After(*f.Until) {
			return false
		}
	}
	return true
}

// parseEngineTime tries the timestamp layouts Presto/Trino emit.
func parseEngineTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}
