package presto

// These types decode the engine's /v1/query and /v1/query/{id} JSON. They are
// intentionally tolerant: only the fields the normalizer needs are declared,
// and durations / data sizes are kept as their raw strings (parsed later with
// ParseDurationMillis / ParseDataSizeBytes). Fields the engine omits stay zero.

// BasicQueryInfo is one element of the /v1/query list endpoint.
type BasicQueryInfo struct {
	QueryID     string          `json:"queryId"`
	State       string          `json:"state"`
	Query       string          `json:"query"`
	SessionUser string          `json:"sessionUser"`
	Session     *sessionInfo    `json:"session"`
	QueryStats  basicQueryStats `json:"queryStats"`
	ErrorCode   *ErrorCode      `json:"errorCode"`
	ErrorType   string          `json:"errorType"`
}

// User returns the submitting user from whichever field the engine populated.
func (b BasicQueryInfo) User() string {
	if b.SessionUser != "" {
		return b.SessionUser
	}
	if b.Session != nil {
		return b.Session.User
	}
	return ""
}

type sessionInfo struct {
	User string `json:"user"`
}

type basicQueryStats struct {
	CreateTime   string `json:"createTime"`
	EndTime      string `json:"endTime"`
	ElapsedTime  string `json:"elapsedTime"`
	TotalCPUTime string `json:"totalCpuTime"`
}

// ErrorCode mirrors the engine's {code,name,type} error descriptor.
type ErrorCode struct {
	Code int    `json:"code"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// QueryInfo is the full /v1/query/{id} document.
type QueryInfo struct {
	QueryID     string       `json:"queryId"`
	State       string       `json:"state"`
	Query       string       `json:"query"`
	Session     sessionInfo  `json:"session"`
	QueryStats  queryStats   `json:"queryStats"`
	OutputStage *StageInfo   `json:"outputStage"`
	ErrorCode   *ErrorCode   `json:"errorCode"`
	ErrorType   string       `json:"errorType"`
	FailureInfo *failureInfo `json:"failureInfo"`
}

type failureInfo struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type queryStats struct {
	CreateTime                string          `json:"createTime"`
	EndTime                   string          `json:"endTime"`
	ElapsedTime               string          `json:"elapsedTime"`
	TotalCPUTime              string          `json:"totalCpuTime"`
	TotalScheduledTime        string          `json:"totalScheduledTime"`
	PeakUserMemoryReservation string          `json:"peakUserMemoryReservation"`
	PeakMemoryReservation     string          `json:"peakMemoryReservation"`
	RawInputDataSize          string          `json:"rawInputDataSize"`
	RawInputPositions         int64           `json:"rawInputPositions"`
	OutputDataSize            string          `json:"outputDataSize"`
	OutputPositions           int64           `json:"outputPositions"`
	OperatorSummaries         []operatorStats `json:"operatorSummaries"`
}

type operatorStats struct {
	StageID         int    `json:"stageId"`
	PipelineID      int    `json:"pipelineId"`
	OperatorID      int    `json:"operatorId"`
	OperatorType    string `json:"operatorType"`
	TotalDrivers    int64  `json:"totalDrivers"`
	AddInputCPU     string `json:"addInputCpu"`
	AddInputWall    string `json:"addInputWall"`
	GetOutputCPU    string `json:"getOutputCpu"`
	GetOutputWall   string `json:"getOutputWall"`
	BlockedWall     string `json:"blockedWall"`
	InputDataSize   string `json:"inputDataSize"`
	InputPositions  int64  `json:"inputPositions"`
	OutputDataSize  string `json:"outputDataSize"`
	OutputPositions int64  `json:"outputPositions"`
}

// StageInfo is one node of the output-stage tree (recursive via SubStages).
type StageInfo struct {
	StageID    string        `json:"stageId"`
	State      string        `json:"state"`
	StageStats stageStats    `json:"stageStats"`
	Plan       *planFragment `json:"plan"`
	SubStages  []StageInfo   `json:"subStages"`
}

type stageStats struct {
	TotalCPUTime       string `json:"totalCpuTime"`
	TotalScheduledTime string `json:"totalScheduledTime"`
	TotalBlockedTime   string `json:"totalBlockedTime"`
	RawInputDataSize   string `json:"rawInputDataSize"`
	RawInputPositions  int64  `json:"rawInputPositions"`
	OutputDataSize     string `json:"outputDataSize"`
	OutputPositions    int64  `json:"outputPositions"`
}

// planFragment carries Trino's rendered-plan JSON string for a stage.
type planFragment struct {
	JSONRepresentation string `json:"jsonRepresentation"`
}

// RenderedPlanNode matches Trino's JsonRenderedNode tree inside a stage's
// plan.jsonRepresentation string.
type RenderedPlanNode struct {
	ID         string             `json:"id"`
	Name       string             `json:"name"`
	Descriptor map[string]string  `json:"descriptor"`
	Children   []RenderedPlanNode `json:"children"`
}
