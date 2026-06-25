package normalize

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yabinma/presto-mcp/internal/presto"
)

const fullQuery = `{
  "queryId":"q1","state":"FINISHED","query":"SELECT 1","session":{"user":"alice"},
  "queryStats":{
    "createTime":"t0","endTime":"t1","elapsedTime":"2.00s","totalCpuTime":"1.00s",
    "peakMemoryReservation":"10.00MB","rawInputDataSize":"1.00kB","rawInputPositions":5,
    "outputDataSize":"512B","outputPositions":1,
    "operatorSummaries":[{"stageId":0,"pipelineId":0,"operatorId":1,"operatorType":"ScanFilter",
      "totalDrivers":4,"addInputCpu":"0.50s","getOutputCpu":"0.20s","addInputWall":"0.60s",
      "getOutputWall":"0.30s","blockedWall":"0.10s","inputDataSize":"1.00kB","inputPositions":5,
      "outputDataSize":"512B","outputPositions":1}]
  },
  "outputStage":{
    "stageId":"q1.0","state":"FINISHED",
    "stageStats":{"totalCpuTime":"1.00s","totalScheduledTime":"1.50s","totalBlockedTime":"0.10s",
      "rawInputDataSize":"1.00kB","rawInputPositions":5,"outputDataSize":"512B","outputPositions":1},
    "plan":{"jsonRepresentation":"{\"id\":\"1\",\"name\":\"Output\",\"descriptor\":{\"columns\":\"x\"},\"children\":[{\"id\":\"2\",\"name\":\"TableScan\",\"children\":[]}]}"},
    "subStages":[{"stageId":"q1.1","state":"FINISHED","stageStats":{"totalCpuTime":"0.20s"}}]
  }
}`

func mustQueryInfo(t *testing.T, s string) *presto.QueryInfo {
	t.Helper()
	qi, err := presto.DecodeQueryInfo([]byte(s))
	require.NoError(t, err)
	return qi
}

func TestQueryDetailFromLive_Full(t *testing.T) {
	qi := mustQueryInfo(t, fullQuery)
	d := QueryDetailFromLive(qi, false, nil)

	assert.Equal(t, SourceLive, d.Source)
	assert.ElementsMatch(t, []string{SectionSummary, SectionStages, SectionOperators, SectionPlan}, d.AvailableSections)

	// summary
	assert.Equal(t, "q1", d.Summary.QueryID)
	assert.Equal(t, "alice", d.Summary.User)
	assert.InDelta(t, 2000, d.Summary.ElapsedMillis, 0.01)
	assert.InDelta(t, 1000, d.Summary.CPUMillis, 0.01)
	assert.EqualValues(t, 10*1024*1024, d.Summary.PeakMemoryBytes)
	assert.EqualValues(t, 5, d.Summary.ScannedRows)
	assert.EqualValues(t, 1024, d.Summary.ScannedBytes)
	assert.EqualValues(t, 512, d.Summary.OutputBytes)
	assert.Nil(t, d.Summary.Error)

	// stages flattened (root + substage)
	require.Len(t, d.Stages, 2)
	assert.Equal(t, "q1.0", d.Stages[0].StageID)
	assert.Equal(t, "q1.1", d.Stages[1].StageID)
	assert.InDelta(t, 1500, d.Stages[0].WallMillis, 0.01)

	// operators
	require.Len(t, d.Operators, 1)
	assert.Equal(t, "ScanFilter", d.Operators[0].OperatorType)
	assert.InDelta(t, 700, d.Operators[0].CPUMillis, 0.01, "addInputCpu+getOutputCpu")
	assert.InDelta(t, 900, d.Operators[0].WallMillis, 0.01)

	// plan flattened with parent links
	require.Len(t, d.Plan, 2)
	assert.Equal(t, "Output", d.Plan[0].Name)
	assert.Equal(t, "columns=x", d.Plan[0].Detail)
	assert.Equal(t, "1", d.Plan[1].ParentID)
	assert.Equal(t, "q1.0", d.Plan[1].StageID)

	assert.Empty(t, d.Raw)
}

func TestQueryDetailFromLive_SummaryOnly(t *testing.T) {
	qi := mustQueryInfo(t, `{"queryId":"q9","state":"RUNNING","session":{"user":"u"},"queryStats":{"elapsedTime":"1.00s"}}`)
	d := QueryDetailFromLive(qi, false, nil)
	assert.Equal(t, []string{SectionSummary}, d.AvailableSections)
	assert.Empty(t, d.Stages)
	assert.Empty(t, d.Operators)
	assert.Empty(t, d.Plan)
}

func TestQueryDetailFromLive_Raw(t *testing.T) {
	raw := []byte(`{"queryId":"q1","state":"FINISHED"}`)
	qi := mustQueryInfo(t, string(raw))
	d := QueryDetailFromLive(qi, true, raw)
	assert.JSONEq(t, string(raw), d.Raw)
}

func TestQueryDetailFromLive_Error(t *testing.T) {
	qi := mustQueryInfo(t, `{"queryId":"qf","state":"FAILED","errorCode":{"code":7,"name":"USER_ERROR","type":"USER_ERROR"},"failureInfo":{"type":"X","message":"bad"}}`)
	d := QueryDetailFromLive(qi, false, nil)
	require.NotNil(t, d.Summary.Error)
	assert.Equal(t, 7, d.Summary.Error.Code)
	assert.Equal(t, "USER_ERROR", d.Summary.Error.Name)
	assert.Equal(t, "bad", d.Summary.Error.Message)
}

func TestQueryDetailFromLive_PeakFallback(t *testing.T) {
	qi := mustQueryInfo(t, `{"queryId":"q","queryStats":{"peakUserMemoryReservation":"2.00MB"}}`)
	d := QueryDetailFromLive(qi, false, nil)
	assert.EqualValues(t, 2*1024*1024, d.Summary.PeakMemoryBytes)
}

func TestQueryDetailFromLive_BadPlanJSONIgnored(t *testing.T) {
	qi := mustQueryInfo(t, `{"queryId":"q","outputStage":{"stageId":"0","plan":{"jsonRepresentation":"{bad"}}}`)
	d := QueryDetailFromLive(qi, false, nil)
	assert.NotContains(t, d.AvailableSections, SectionPlan)
	assert.Contains(t, d.AvailableSections, SectionStages)
}

func TestQueryListFromLive_Filtering(t *testing.T) {
	var items []presto.BasicQueryInfo
	require.NoError(t, json.Unmarshal([]byte(`[
		{"queryId":"q1","state":"FINISHED","sessionUser":"alice","query":"a","queryStats":{"createTime":"2026-06-24T10:00:00Z","elapsedTime":"1.00s"}},
		{"queryId":"q2","state":"RUNNING","sessionUser":"bob","query":"b","queryStats":{"createTime":"2026-06-24T12:00:00Z","elapsedTime":"2.00s"}}
	]`), &items))

	all := QueryListFromLive(items, Filter{})
	assert.Len(t, all, 2)
	assert.InDelta(t, 1000, all[0].ElapsedMillis, 0.01)

	byState := QueryListFromLive(items, Filter{State: "running"})
	require.Len(t, byState, 1)
	assert.Equal(t, "q2", byState[0].QueryID)

	byUser := QueryListFromLive(items, Filter{User: "alice"})
	require.Len(t, byUser, 1)
	assert.Equal(t, "q1", byUser[0].QueryID)

	since := time.Date(2026, 6, 24, 11, 0, 0, 0, time.UTC)
	until := time.Date(2026, 6, 24, 13, 0, 0, 0, time.UTC)
	byTime := QueryListFromLive(items, Filter{Since: &since, Until: &until})
	require.Len(t, byTime, 1)
	assert.Equal(t, "q2", byTime[0].QueryID)
}

func TestFilter_UnparseableTimeNotExcluded(t *testing.T) {
	since := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := Filter{Since: &since}
	assert.True(t, f.matches("FINISHED", "u", "not-a-time"))
	assert.True(t, f.matches("FINISHED", "u", ""))
}

func TestDescribePlan_Deterministic(t *testing.T) {
	got := describePlan(map[string]string{"b": "2", "a": "1"})
	assert.Equal(t, "a=1, b=2", got)
	assert.Empty(t, describePlan(nil))
}
