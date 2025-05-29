package storage

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/dstdfx/mini-tsdb/internal/domain"
)

func TestInMemory_BuildHash(t *testing.T) {
	s := NewInMemoryEfficient()

	labels := []domain.Label{
		{
			Name:  "test",
			Value: "123",
		},
		{
			Name:  "namespace",
			Value: "jobs",
		},
		{
			Name:  "state",
			Value: "stable",
		},
		{
			Name:  "abc",
			Value: "hello",
		},
	}

	got := s.buildLabelsHash(labels)
	expected := labelsHash(uint64(8341061335512845696))

	if got != expected {
		t.Errorf("expected '%v' but got '%v'", expected, got)
		t.Fail()
	}
}

func TestFindIntersection(t *testing.T) {
	tableTest := []struct {
		msg      string
		a        []int
		b        []int
		expected []int
	}{
		{
			msg:      "empty values",
			a:        []int{},
			b:        []int{},
			expected: []int{},
		},
		{
			msg:      "equal slices",
			a:        []int{1, 2, 3},
			b:        []int{1, 2, 3},
			expected: []int{1, 2, 3},
		},
		{
			msg:      "with intersection, len(a) > len(b)",
			a:        []int{1, 2, 3, 4},
			b:        []int{1, 2, 3},
			expected: []int{1, 2, 3},
		},
		{
			msg:      "with intersection, len(a) < len(b)",
			a:        []int{1, 2, 3},
			b:        []int{1, 2, 3, 4},
			expected: []int{1, 2, 3},
		},
		{
			msg:      "no intersection, len(a) > len(b)",
			a:        []int{5, 6, 7, 8},
			b:        []int{1, 2, 3},
			expected: []int{},
		},
		{
			msg:      "no intersection, len(a) < len(b)",
			a:        []int{1, 2, 3},
			b:        []int{5, 6, 7, 8},
			expected: []int{},
		},
	}

	for _, test := range tableTest {
		t.Run(test.msg, func(t *testing.T) {
			got := FindIntersection(test.a, test.b)
			if !reflect.DeepEqual(got, test.expected) {
				t.Errorf("got '%v' but expected '%v'", got, test.expected)
			}
		})
	}
}

func TestInMemory_Write_Read(t *testing.T) {
	s := NewInMemoryEfficient()

	labels1 := []domain.Label{
		{
			Name:  "test",
			Value: "123",
		},
		{
			Name:  "namespace",
			Value: "jobs",
		},
		{
			Name:  "state",
			Value: "stable",
		},
		{
			Name:  "abc",
			Value: "hello",
		},
	}

	labels2 := []domain.Label{
		{
			Name:  "namespace",
			Value: "jobs",
		},
	}

	labels3 := []domain.Label{
		{
			Name:  "namespace",
			Value: "jobs",
		},
		{
			Name:  "test",
			Value: "123",
		},
	}

	tNow := time.Now().Unix()

	samples := []domain.Sample{
		{
			Timestamp: tNow,
			Value:     123,
		},
		{
			Timestamp: tNow + 1,
			Value:     124,
		},
		{
			Timestamp: tNow + 2,
			Value:     125,
		},
		{
			Timestamp: tNow + 3,
			Value:     126,
		},
	}

	for _, v := range [][]domain.Label{labels1, labels2, labels3} {
		s.Write(v, samples)
	}

	requestLabels := []domain.LabelMatcher{
		{
			Type:  domain.EQ,
			Name:  "namespace",
			Value: "jobs",
		},
		{
			Type:  domain.NEQ,
			Name:  "test",
			Value: "123",
		},
	}

	got, err := s.Read(tNow, tNow+2, requestLabels)
	if err != nil {
		t.Errorf("unexpected error: '%v'", err)
	}

	// TODO: refactor tests
	fmt.Println("matching ts: ")
	for _, v := range got {
		fmt.Println(v)
	}

	// got, err := s.Read(labels)
	// if err != nil {
	// 	t.Errorf("unexpected error: '%v'", err)
	// 	t.Fail()
	// }

	// if !reflect.DeepEqual(got, samples) {
	// 	t.Errorf("expected values to be equal: '%v' to '%v'", got, samples)
	// }
}

// TODO: add tests and basic benchmarks to verify this approach

func TestFilterSamples(t *testing.T) {
	tableTest := []struct {
		msg      string
		from, to int64
		samples  []domain.Sample
		expected []domain.Sample
	}{
		{
			msg:      "empty",
			from:     1,
			to:       5,
			samples:  []domain.Sample{},
			expected: []domain.Sample{},
		},
		{
			msg:  "normal case",
			from: 2,
			to:   5,
			samples: []domain.Sample{
				{
					Timestamp: 1,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 3,
				},
				{
					Timestamp: 5,
				},
				{
					Timestamp: 8,
				},
			},
			expected: []domain.Sample{
				{
					Timestamp: 2,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 3,
				},
				{
					Timestamp: 5,
				},
			},
		},
		{
			msg:  "all samples",
			from: 1,
			to:   10,
			samples: []domain.Sample{
				{
					Timestamp: 1,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 3,
				},
				{
					Timestamp: 5,
				},
				{
					Timestamp: 8,
				},
			},
			expected: []domain.Sample{
				{
					Timestamp: 1,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 3,
				},
				{
					Timestamp: 5,
				},
				{
					Timestamp: 8,
				},
			},
		},
		{
			msg:  "no samples in the range, right side",
			from: 9,
			to:   15,
			samples: []domain.Sample{
				{
					Timestamp: 1,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 3,
				},
				{
					Timestamp: 5,
				},
				{
					Timestamp: 8,
				},
			},
			expected: []domain.Sample{},
		},
		{
			msg:  "no samples in the range, left side",
			from: -3,
			to:   0,
			samples: []domain.Sample{
				{
					Timestamp: 1,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 2,
				},
				{
					Timestamp: 3,
				},
				{
					Timestamp: 5,
				},
				{
					Timestamp: 8,
				},
			},
			expected: []domain.Sample{},
		},
	}

	for _, test := range tableTest {
		t.Run(test.msg, func(t *testing.T) {
			got := filterSamples(test.expected, test.from, test.to)
			if !reflect.DeepEqual(test.expected, got) {
				t.Errorf("filterSamples: got '%v' but expected '%v'", got, test.expected)
			}
		})
	}
}

func BenchmarkFilterSamples(b *testing.B) {
	const total = 1_000_000
	samples := make([]domain.Sample, total)
	for i := 0; i < total; i++ {
		samples[i] = domain.Sample{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}

	from := int64(300_000_000)
	to := int64(600_000_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = filterSamples(samples, from, to)
	}
}
