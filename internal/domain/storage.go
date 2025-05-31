package domain

type Storage interface {
	Write(labels []Label, samples []Sample) error
	Read(fromMs, toMs int64, labelMatchers []LabelMatcher) ([]TimeSeries, error)
}
