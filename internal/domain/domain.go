package domain

import "github.com/prometheus/prometheus/prompb"

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

// ToProto method converts TimeSeries to prometheus protobuf compliant type.
func (ts TimeSeries) ToProto() *prompb.TimeSeries {
	labels := make([]prompb.Label, 0, len(ts.Labels))
	for _, l := range ts.Labels {
		labels = append(labels, prompb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}

	samples := make([]prompb.Sample, 0, len(ts.Samples))
	for _, s := range ts.Samples {
		samples = append(samples, prompb.Sample{
			Value:     s.Value,
			Timestamp: s.Timestamp,
		})
	}

	return &prompb.TimeSeries{
		Labels:  labels,
		Samples: samples,
	}
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
