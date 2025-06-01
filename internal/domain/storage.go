package domain

type Storage interface {
	Write(labels []Label, samples []Sample)
	WriteMultiple(series []TimeSeries)
	Read(fromMs, toMs int64, labelMatchers []LabelMatcher) []TimeSeries
}
