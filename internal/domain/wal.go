package domain

type WalEntity struct {
	Timestamp  int64
	TimeSeries []TimeSeries
}

type Wal interface {
	Append(entry WalEntity) error
}
