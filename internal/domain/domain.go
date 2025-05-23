package domain

type Label struct {
	Name  string
	Value string
}

type Sample struct {
	Value     float64
	Timestamp int64
}
