package storage

import (
	"testing"
	"time"

	"github.com/dstdfx/mini-tsdb/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestInMemory_BuildHash(t *testing.T) {
	s := NewInMemory()

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
	assert.Equal(t, expected, got)
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
			assert.Equal(t, test.expected, findIntersection(test.a, test.b))
		})
	}
}

func TestInMemory_Write_Read(t *testing.T) {
	type (
		expected struct {
			timeSeries []domain.TimeSeries
		}
		readOpts struct {
			from, to      int64
			labelsMatcher []domain.LabelMatcher
		}
	)

	// Fixtures
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

	tableTest := []struct {
		msg      string
		writes   []domain.TimeSeries
		reads    []readOpts
		expected []expected
	}{
		{
			msg:    "no data",
			writes: []domain.TimeSeries{},
			reads: []readOpts{
				{
					from:          1,
					to:            10,
					labelsMatcher: []domain.LabelMatcher{},
				},
			},
			expected: []expected{
				{},
			},
		},
		{
			msg: "writes/reads",
			writes: []domain.TimeSeries{
				{
					Labels: []domain.Label{
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
					},
					Samples: samples,
				},
				{
					Labels: []domain.Label{
						{
							Name:  "namespace",
							Value: "jobs",
						},
					},
					Samples: samples,
				},
				{
					Labels: []domain.Label{
						{
							Name:  "namespace",
							Value: "jobs",
						},
						{
							Name:  "test",
							Value: "123",
						},
					},
					Samples: samples,
				},
			},
			reads: []readOpts{
				{
					from: tNow,
					to:   tNow + 2,
					labelsMatcher: []domain.LabelMatcher{
						{
							Type:  domain.EQ,
							Name:  "namespace",
							Value: "jobs",
						},
						{
							Type:  domain.EQ,
							Name:  "test",
							Value: "123",
						},
					},
				},
				{
					from: tNow,
					to:   tNow + 2,
					labelsMatcher: []domain.LabelMatcher{
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
					},
				},
				// Samples are out of range
				{
					from: tNow + 10,
					to:   tNow + 15,
					labelsMatcher: []domain.LabelMatcher{
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
					},
				},
			},
			expected: []expected{
				{
					timeSeries: []domain.TimeSeries{
						{
							Labels: []domain.Label{
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
							},
							Samples: []domain.Sample{
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
							},
						},
						{
							Labels: []domain.Label{
								{
									Name:  "namespace",
									Value: "jobs",
								},
								{
									Name:  "test",
									Value: "123",
								},
							},
							Samples: []domain.Sample{
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
							},
						},
					},
				},
				{
					timeSeries: []domain.TimeSeries{
						{
							Labels: []domain.Label{
								{
									Name:  "namespace",
									Value: "jobs",
								},
							},
							Samples: []domain.Sample{
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
							},
						},
					},
				},
				{
					timeSeries: []domain.TimeSeries{
						{
							Labels: []domain.Label{
								{
									Name:  "namespace",
									Value: "jobs",
								},
							},
							Samples: []domain.Sample{},
						},
					},
				},
			},
		},
	}

	for _, test := range tableTest {
		t.Run(test.msg, func(t *testing.T) {
			s := NewInMemory()

			// Write data
			for _, w := range test.writes {
				s.Write(w.Labels, w.Samples)
			}

			// Read data
			for i, r := range test.reads {
				got := s.Read(r.from, r.to, r.labelsMatcher)

				// A bit hacky way to assert non-determenistic order in slice-fields
				if assert.Equal(t, len(test.expected[i].timeSeries), len(got)) {
					for idx := 0; idx < len(test.expected[i].timeSeries); idx++ {
						// Assert labels
						assert.ElementsMatch(t,
							test.expected[i].timeSeries[idx].Labels, got[idx].Labels)
						// Assert samples
						assert.ElementsMatch(t,
							test.expected[i].timeSeries[idx].Samples, got[idx].Samples)
					}
				}
			}
		})
	}
}

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
			assert.Equal(t, test.expected, got)
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
