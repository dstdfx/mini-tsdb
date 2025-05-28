package storage

import (
	"sort"
	"sync"

	"github.com/dstdfx/mini-tsdb/internal/domain"

	"hash/fnv"
)

type (
	seriesID   uint64 // Unique identifier for a series
	lableName  string // Represents the name of a label
	labelValue string // Represents the value of a label
	labelsHash uint64 // Hash value for a set of labels
)

type InMemory struct {
	mu            sync.RWMutex
	lastSeriesID  seriesID                                // id of the last used series identifier
	series        map[seriesID][]domain.Sample            // used to map series identifier to a slice of samples
	invertedIndex map[lableName]map[labelValue][]seriesID // used to map series with specific labels and names
	labelsByID    map[seriesID]map[lableName]labelValue   // series id to the labels and values
	seriesHash    map[labelsHash]seriesID                 // to check if we already had a sequence of labels before
}

func NewInMemoryEfficient() *InMemory {
	return &InMemory{
		series:        make(map[seriesID][]domain.Sample),
		invertedIndex: make(map[lableName]map[labelValue][]seriesID),
		labelsByID:    make(map[seriesID]map[lableName]labelValue),
		seriesHash:    make(map[labelsHash]seriesID),
	}
}

func (s *InMemory) Write(labels []domain.Label, samples []domain.Sample) error {
	// TODO: return an error on invalid params

	s.mu.Lock()
	defer s.mu.Unlock()

	currentHash := s.buildLabelsHash(labels)

	// Check if we already had this sequence of labels
	existingSeriesID, isKnownHash := s.seriesHash[currentHash]
	if !isKnownHash {
		// New sequence, get next series id
		s.lastSeriesID++
		existingSeriesID = s.lastSeriesID

		// Map hash to the series id
		s.seriesHash[currentHash] = existingSeriesID
	}

	// Update the samples
	s.series[existingSeriesID] = append(s.series[existingSeriesID], samples...)

	if isKnownHash {
		// Fast path: no need to update inverted index for existing labels sequence
		return nil
	}

	// Slow path: build the inverted index and labels map for a new labels sequence
	for _, l := range labels {
		// Make sure the index is initialized
		if s.invertedIndex[lableName(l.Name)] == nil {
			s.invertedIndex[lableName(l.Name)] = make(map[labelValue][]seriesID, len(labels))
		}
		if s.invertedIndex[lableName(l.Name)][labelValue(l.Value)] == nil {
			s.invertedIndex[lableName(l.Name)][labelValue(l.Value)] = make([]seriesID, 0, 256)
		}

		// Map label name, label value to the series id
		s.invertedIndex[lableName(l.Name)][labelValue(l.Value)] =
			append(s.invertedIndex[lableName(l.Name)][labelValue(l.Value)], existingSeriesID)

		// Build lables map
		if s.labelsByID[existingSeriesID] == nil {
			s.labelsByID[existingSeriesID] = make(map[lableName]labelValue, len(labels))
		}
		s.labelsByID[existingSeriesID][lableName(l.Name)] = labelValue(l.Value)
	}

	return nil
}

func (s *InMemory) buildLabelsHash(labels []domain.Label) labelsHash {
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

	return labelsHash(h.Sum64())
}

func (s *InMemory) Read(
	fromMs,
	toMs int64,
	labelMatchers []domain.LabelMatcher) (timeSeries []domain.TimeSeries, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	neqLabels := make([]domain.LabelMatcher, 0)

	// Find values that match with all EQ labels
	seriesIDs := make([]seriesID, 0)
	for _, l := range labelMatchers {
		// Skip non EQ types
		if l.Type != domain.EQ {
			// TODO: fix me when regexp type is supported
			// Collect all NEQ labels for further filtering
			neqLabels = append(neqLabels, l)

			continue
		}

		values, ok := s.invertedIndex[lableName(l.Name)]
		if !ok {
			// No matching values - abort further checking
			return
		}

		ids, ok := values[labelValue(l.Value)]
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

	// Filter ids by remaining NEQ labels and
	// collect matching time series
	for _, id := range seriesIDs {
		// Check if we need to skip current id
		var skip bool
		for _, l := range neqLabels {
			if v, ok := s.labelsByID[id][lableName(l.Name)]; ok && v == labelValue(l.Value) {
				skip = true

				break
			}
		}
		if skip {
			// Skip current id as NEQ label mismatched
			continue
		}

		var ts domain.TimeSeries

		// Collect time series and filter samples by from/to range
		ts.Samples = filterSamples(s.series[id], fromMs, toMs)
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
	// 4. check for NEQ +
	// 5. filter by FROM/TO +

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

func filterSamples(samples []domain.Sample, fromMs, toMs int64) []domain.Sample {
	// Find leftmost index first
	leftmost := sort.Search(len(samples), func(i int) bool {
		return samples[i].Timestamp >= fromMs
	})

	if leftmost == len(samples) {
		// no values within the range
		return []domain.Sample{}
	}

	rightmost := sort.Search(len(samples), func(i int) bool {
		return samples[i].Timestamp > toMs
	})

	if rightmost == 0 {
		// no values within the range
		return []domain.Sample{}
	}

	return samples[leftmost:rightmost]
}
