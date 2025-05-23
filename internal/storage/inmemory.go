package storage

import (
	"errors"
	"sync"

	"github.com/dstdfx/mini-tsdb/internal/domain"
)

type TimeSeriesInMemory struct {
	mu   sync.Mutex
	data map[string][]domain.Sample
}

func NewTimeSeriesInMemory() *TimeSeriesInMemory {
	return &TimeSeriesInMemory{
		data: make(map[string][]domain.Sample),
	}
}

func (s *TimeSeriesInMemory) Write(labels []domain.Label, samples []domain.Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := buildKey(labels)
	if _, exists := s.data[key]; !exists {
		s.data[key] = []domain.Sample{}
	}

	s.data[key] = append(s.data[key], samples...)
}

func (s *TimeSeriesInMemory) Read(labels []domain.Label) ([]domain.Sample, error) {
	return nil, errors.New("not implemented")
}

func buildKey(labels []domain.Label) string {
	key := ""
	for _, label := range labels {
		key += label.Name + "=" + label.Value + ","
	}

	return key
}
