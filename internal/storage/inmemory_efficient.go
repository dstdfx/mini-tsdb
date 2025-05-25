package storage

import (
	"errors"
	"sort"
	"sync"

	"github.com/dstdfx/mini-tsdb/internal/domain"

	"hash/fnv"
)

type (
	SeriesID uint64

	LableName string

	LabelValue string

	LabelsHash uint64
)

type InMemoryEfficient struct {
	mu            sync.RWMutex
	lastSeriesID  SeriesID                                // id of the last used series identifier
	series        map[SeriesID][]domain.Sample            // used to map series identifier to a slice of samples
	invertedIndex map[LableName]map[LabelValue][]SeriesID // used to map series with specific labels and names
	labelsByID    map[SeriesID]map[LableName]LabelValue   // do we really need to store the values?
	seriesHash    map[LabelsHash]SeriesID                 // to check if we already had a sequence of labels before
}

func NewInMemoryEfficient() *InMemoryEfficient {
	return &InMemoryEfficient{
		series:        make(map[SeriesID][]domain.Sample),
		invertedIndex: make(map[LableName]map[LabelValue][]SeriesID),
		labelsByID:    make(map[SeriesID]map[LableName]LabelValue),
		seriesHash:    make(map[LabelsHash]SeriesID),
	}
}

// TODO: add tests and basic benchmarks to verify this approach

func (s *InMemoryEfficient) Write(labels []domain.Label, samples []domain.Sample) error {
	// TODO: return an error on invalid params

	s.mu.Lock()
	defer s.mu.Unlock()

	currentHash := s.buildLabelsHash(labels)

	// Check if we already had this sequence of labels
	seriesID, isKnownHash := s.seriesHash[currentHash]
	if !isKnownHash {
		// New sequence, get next series id
		s.lastSeriesID++
		seriesID = s.lastSeriesID

		// Map hash to the series id
		s.seriesHash[currentHash] = seriesID
	}

	// Update the samples
	s.series[seriesID] = append(s.series[seriesID], samples...)

	if isKnownHash {
		// Fast path: no need to update inverted index for existing labels sequence
		return nil
	}

	// Slow path: build the inverted index and labels map for a new labels sequence
	for _, l := range labels {
		// Make sure the index is initialized
		if s.invertedIndex[LableName(l.Name)] == nil {
			s.invertedIndex[LableName(l.Name)] = make(map[LabelValue][]SeriesID, len(labels))
		}
		if s.invertedIndex[LableName(l.Name)][LabelValue(l.Value)] == nil {
			s.invertedIndex[LableName(l.Name)][LabelValue(l.Value)] = make([]SeriesID, 0, 256)
		}

		// Map label name, label value to the series id
		s.invertedIndex[LableName(l.Name)][LabelValue(l.Value)] =
			append(s.invertedIndex[LableName(l.Name)][LabelValue(l.Value)], seriesID)

		// Build lables map
		if s.labelsByID[seriesID] == nil {
			s.labelsByID[seriesID] = make(map[LableName]LabelValue, len(labels))
		}
		s.labelsByID[seriesID][LableName(l.Name)] = LabelValue(l.Value)
	}

	return nil
}

func (s *InMemoryEfficient) buildLabelsHash(labels []domain.Label) LabelsHash {
	// Sort labels' names alphabetically
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Name < labels[j].Name
	})

	h := fnv.New64a()

	for idx, l := range labels {
		h.Write([]byte(l.Name))
		h.Write([]byte("="))
		h.Write([]byte(l.Value))

		if idx != len(labels)-1 {
			h.Write([]byte(","))
		}
	}

	return LabelsHash(h.Sum64())
}

// TODO: change the interface to match the request
func (s *InMemoryEfficient) Read(labels []domain.Label) ([]domain.Sample, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	currentHash := s.buildLabelsHash(labels)

	// Check if we already had this sequence of labels
	seriesID, isKnownHash := s.seriesHash[currentHash]
	if !isKnownHash {
		return nil, errors.New("no data for these labels")
	}

	// TODO: for now - just return all we have
	return s.series[seriesID], nil
}
