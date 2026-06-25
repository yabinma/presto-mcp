// Package normalize converts the engine's verbose query info into the compact
// QueryDetail model returned by get_query, so raw QueryInfo JSON never reaches
// the agent's context. The raw fragment is included only on explicit request.
package normalize

import (
	"encoding/json"
	"sort"

	"github.com/yabinma/presto-mcp/internal/presto"
)

// Source identifies where a query record came from.
const (
	SourceLive    = "live"
	SourceHistory = "history"
)

// Section names reported in QueryDetail.AvailableSections.
const (
	SectionSummary   = "summary"
	SectionStages    = "stages"
	SectionOperators = "operators"
	SectionPlan      = "plan"
)

// QueryDetail is the normalized result of get_query.
type QueryDetail struct {
	Summary           QuerySummary     `json:"summary"`
	Stages            []StageDetail    `json:"stages,omitempty"`
	Operators         []OperatorDetail `json:"operators,omitempty"`
	Plan              []PlanNode       `json:"plan,omitempty"`
	Source            string           `json:"source"`
	AvailableSections []string         `json:"available_sections"`
	// Raw holds the unmodified engine fragment; populated only when raw=true.
	Raw string `json:"raw,omitempty"`
}

// QuerySummary is always present.
type QuerySummary struct {
	QueryID         string      `json:"query_id"`
	State           string      `json:"state"`
	User            string      `json:"user,omitempty"`
	Query           string      `json:"query,omitempty"`
	CreateTime      string      `json:"create_time,omitempty"`
	EndTime         string      `json:"end_time,omitempty"`
	ElapsedMillis   float64     `json:"elapsed_ms,omitempty"`
	CPUMillis       float64     `json:"cpu_ms,omitempty"`
	PeakMemoryBytes int64       `json:"peak_memory_bytes,omitempty"`
	ScannedRows     int64       `json:"scanned_rows,omitempty"`
	ScannedBytes    int64       `json:"scanned_bytes,omitempty"`
	OutputRows      int64       `json:"output_rows,omitempty"`
	OutputBytes     int64       `json:"output_bytes,omitempty"`
	Error           *QueryError `json:"error,omitempty"`
}

// QueryError carries failure details when a query did not finish cleanly.
type QueryError struct {
	Code    int    `json:"code,omitempty"`
	Name    string `json:"name,omitempty"`
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
}

// StageDetail is per-stage cost (when stage info is available).
type StageDetail struct {
	StageID       string  `json:"stage_id"`
	State         string  `json:"state,omitempty"`
	CPUMillis     float64 `json:"cpu_ms,omitempty"`
	WallMillis    float64 `json:"wall_ms,omitempty"`
	BlockedMillis float64 `json:"blocked_ms,omitempty"`
	InputRows     int64   `json:"input_rows,omitempty"`
	InputBytes    int64   `json:"input_bytes,omitempty"`
	OutputRows    int64   `json:"output_rows,omitempty"`
	OutputBytes   int64   `json:"output_bytes,omitempty"`
}

// OperatorDetail is per-operator step cost (the "cost of each step").
type OperatorDetail struct {
	StageID       int     `json:"stage_id"`
	PipelineID    int     `json:"pipeline_id"`
	OperatorID    int     `json:"operator_id"`
	OperatorType  string  `json:"operator_type"`
	TotalDrivers  int64   `json:"total_drivers,omitempty"`
	CPUMillis     float64 `json:"cpu_ms,omitempty"`
	WallMillis    float64 `json:"wall_ms,omitempty"`
	BlockedMillis float64 `json:"blocked_ms,omitempty"`
	InputRows     int64   `json:"input_rows,omitempty"`
	InputBytes    int64   `json:"input_bytes,omitempty"`
	OutputRows    int64   `json:"output_rows,omitempty"`
	OutputBytes   int64   `json:"output_bytes,omitempty"`
}

// PlanNode is one node of the flattened plan tree (parent referenced by ID).
type PlanNode struct {
	ID       string `json:"id"`
	ParentID string `json:"parent_id,omitempty"`
	StageID  string `json:"stage_id,omitempty"`
	Name     string `json:"name"`
	Detail   string `json:"detail,omitempty"`
}

// QueryDetailFromLive normalizes a live coordinator QueryInfo. When includeRaw
// is true, rawBody is attached verbatim.
func QueryDetailFromLive(qi *presto.QueryInfo, includeRaw bool, rawBody []byte) QueryDetail {
	d := QueryDetail{Source: SourceLive}
	d.Summary = summaryFromLive(qi)
	sections := []string{SectionSummary}

	if qi.OutputStage != nil {
		d.Stages = stagesFromLive(qi.OutputStage)
		if len(d.Stages) > 0 {
			sections = append(sections, SectionStages)
		}
		d.Plan = planFromLive(qi.OutputStage)
		if len(d.Plan) > 0 {
			sections = append(sections, SectionPlan)
		}
	}
	if ops := operatorsFromLive(qi); len(ops) > 0 {
		d.Operators = ops
		sections = append(sections, SectionOperators)
	}
	d.AvailableSections = sections
	if includeRaw {
		d.Raw = string(rawBody)
	}
	return d
}

func summaryFromLive(qi *presto.QueryInfo) QuerySummary {
	s := QuerySummary{
		QueryID:       qi.QueryID,
		State:         qi.State,
		User:          qi.Session.User,
		Query:         qi.Query,
		CreateTime:    qi.QueryStats.CreateTime,
		EndTime:       qi.QueryStats.EndTime,
		ElapsedMillis: millis(qi.QueryStats.ElapsedTime),
		CPUMillis:     millis(qi.QueryStats.TotalCPUTime),
		ScannedRows:   qi.QueryStats.RawInputPositions,
		ScannedBytes:  bytesOf(qi.QueryStats.RawInputDataSize),
		OutputRows:    qi.QueryStats.OutputPositions,
		OutputBytes:   bytesOf(qi.QueryStats.OutputDataSize),
	}
	if peak := bytesOf(qi.QueryStats.PeakMemoryReservation); peak > 0 {
		s.PeakMemoryBytes = peak
	} else {
		s.PeakMemoryBytes = bytesOf(qi.QueryStats.PeakUserMemoryReservation)
	}
	s.Error = errorOf(qi)
	return s
}

func errorOf(qi *presto.QueryInfo) *QueryError {
	if qi.ErrorCode == nil && qi.FailureInfo == nil {
		return nil
	}
	e := &QueryError{}
	if qi.ErrorCode != nil {
		e.Code = qi.ErrorCode.Code
		e.Name = qi.ErrorCode.Name
		e.Type = qi.ErrorCode.Type
	}
	if qi.FailureInfo != nil {
		e.Message = qi.FailureInfo.Message
		if e.Type == "" {
			e.Type = qi.FailureInfo.Type
		}
	}
	return e
}

func stagesFromLive(root *presto.StageInfo) []StageDetail {
	var out []StageDetail
	var walk func(s *presto.StageInfo)
	walk = func(s *presto.StageInfo) {
		out = append(out, StageDetail{
			StageID:       s.StageID,
			State:         s.State,
			CPUMillis:     millis(s.StageStats.TotalCPUTime),
			WallMillis:    millis(s.StageStats.TotalScheduledTime),
			BlockedMillis: millis(s.StageStats.TotalBlockedTime),
			InputRows:     s.StageStats.RawInputPositions,
			InputBytes:    bytesOf(s.StageStats.RawInputDataSize),
			OutputRows:    s.StageStats.OutputPositions,
			OutputBytes:   bytesOf(s.StageStats.OutputDataSize),
		})
		for i := range s.SubStages {
			walk(&s.SubStages[i])
		}
	}
	walk(root)
	return out
}

func operatorsFromLive(qi *presto.QueryInfo) []OperatorDetail {
	ops := qi.QueryStats.OperatorSummaries
	if len(ops) == 0 {
		return nil
	}
	out := make([]OperatorDetail, 0, len(ops))
	for _, op := range ops {
		out = append(out, OperatorDetail{
			StageID:       op.StageID,
			PipelineID:    op.PipelineID,
			OperatorID:    op.OperatorID,
			OperatorType:  op.OperatorType,
			TotalDrivers:  op.TotalDrivers,
			CPUMillis:     millis(op.AddInputCPU) + millis(op.GetOutputCPU),
			WallMillis:    millis(op.AddInputWall) + millis(op.GetOutputWall),
			BlockedMillis: millis(op.BlockedWall),
			InputRows:     op.InputPositions,
			InputBytes:    bytesOf(op.InputDataSize),
			OutputRows:    op.OutputPositions,
			OutputBytes:   bytesOf(op.OutputDataSize),
		})
	}
	return out
}

func planFromLive(root *presto.StageInfo) []PlanNode {
	var out []PlanNode
	var walkStage func(s *presto.StageInfo)
	walkStage = func(s *presto.StageInfo) {
		if s.Plan != nil && s.Plan.JSONRepresentation != "" {
			var node presto.RenderedPlanNode
			if err := json.Unmarshal([]byte(s.Plan.JSONRepresentation), &node); err == nil {
				flattenPlan(node, "", s.StageID, &out)
			}
		}
		for i := range s.SubStages {
			walkStage(&s.SubStages[i])
		}
	}
	walkStage(root)
	return out
}

func flattenPlan(n presto.RenderedPlanNode, parentID, stageID string, out *[]PlanNode) {
	*out = append(*out, PlanNode{
		ID:       n.ID,
		ParentID: parentID,
		StageID:  stageID,
		Name:     n.Name,
		Detail:   describePlan(n.Descriptor),
	})
	for _, c := range n.Children {
		flattenPlan(c, n.ID, stageID, out)
	}
}

// describePlan renders a descriptor map deterministically as "k=v, k=v".
func describePlan(desc map[string]string) string {
	if len(desc) == 0 {
		return ""
	}
	keys := make([]string, 0, len(desc))
	for k := range desc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for i, k := range keys {
		if i > 0 {
			b = append(b, ", "...)
		}
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, desc[k]...)
	}
	return string(b)
}

func millis(s string) float64 {
	v, _ := presto.ParseDurationMillis(s)
	return v
}

func bytesOf(s string) int64 {
	v, _ := presto.ParseDataSizeBytes(s)
	return v
}
