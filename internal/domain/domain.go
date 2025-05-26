package domain

type Label struct {
	Name  string
	Value string
}

type Sample struct {
	Value     float64
	Timestamp int64
}

type TimeSeries struct {
	Labels  []Label
	Samples []Sample // asc sorted by timestamp
}

type LabelMatcherType int32

const (
	EQ  = 0
	NEQ = 1
)

type LabelMatcher struct {
	Type  LabelMatcherType
	Name  string
	Value string
}
