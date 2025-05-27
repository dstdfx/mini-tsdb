package storage

import (
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
	labelsByID    map[SeriesID]map[LableName]LabelValue   // series id to the labels and values
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

func (s *InMemoryEfficient) Read(
	fromMs,
	toMs int64,
	labelMatchers []domain.LabelMatcher) (timeSeries []domain.TimeSeries, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find values that match with all given lables
	seriesIDs := make([]SeriesID, 0)
	for _, l := range labelMatchers {
		// Skip non EQ types
		if l.Type != domain.EQ {
			continue
		}

		values, ok := s.invertedIndex[LableName(l.Name)]
		if !ok {
			// No matching values - abort further checking
			return
		}

		ids, ok := values[LabelValue(l.Value)]
		if !ok {
			// No matching values - abort further checking
			return
		}

		if len(seriesIDs) == 0 {
			// Init case: when it's the very first slice so
			// there's nothing to intersect it with
			seriesIDs = ids
		} else {
			// Otherwise just find intersection to leave only the
			// ids that matched
			seriesIDs = FindIntersection(seriesIDs, ids)
			if len(seriesIDs) == 0 {
				// No matching values - abort further checking
				return
			}
		}
	}

	// Collect labels and samples for matching ids
	for _, id := range seriesIDs {
		ts := domain.TimeSeries{}
		ts.Samples = s.series[id]

		for k, v := range s.labelsByID[id] {
			ts.Labels = append(ts.Labels, domain.Label{
				Name:  string(k),
				Value: string(v),
			})
		}

		timeSeries = append(timeSeries, ts)
	}

	// TODO:
	// 1. go over all labels with EQ in request +
	// 2. check inverted index for which series id have these exact labels/values +
	// 3. do set intersection on each step +
	// 4. check for NEQ

	return timeSeries, nil
}

// FindIntersection returns a slice of common elements that are both
// in a and b slices.
func FindIntersection[T comparable](a, b []T) []T {
	if len(a) > len(b) {
		return FindIntersection(b, a)
	}

	result := make([]T, 0)

	// Build a map from the longer slice so we
	// can check which elements are in A and B slices
	mappingA := make(map[T]struct{})
	for _, el := range a {
		mappingA[el] = struct{}{}
	}

	// Traverse each element in the shortest slice and
	// check wether it contains in A mapping
	for _, el := range b {
		_, ok := mappingA[el]
		if ok {
			// Add common element to the result
			result = append(result, el)
		}
	}

	return result
}
