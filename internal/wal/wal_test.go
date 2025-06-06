package wal

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/dstdfx/mini-tsdb/internal/domain"
	"github.com/stretchr/testify/assert"
)

func TestWal_listWalFiles(t *testing.T) {
	walDir, err := os.MkdirTemp(os.TempDir(), "waltest")
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(walDir))
	})

	files := []string{
		"123.wal",
		"321.wal",
		"124.wal",
		"2.wal",
		"asdfsf.wal",
		"999ggg.wal",
		"adsfasdf_1.wal",
	}

	for _, f := range files {
		ff, err := os.Create(walDir + "/" + f)
		assert.NoError(t, err)

		ff.Close()
	}

	w := New(nil, Opts{
		PartitionsPath: walDir,
	})

	expected := []walFile{
		{
			name: "2.wal",
			ts:   2,
		},
		{
			name: "123.wal",
			ts:   123,
		},
		{
			name: "124.wal",
			ts:   124,
		},
		{
			name: "321.wal",
			ts:   321,
		},
	}

	got, err := w.listWalFiles()
	assert.NoError(t, err)
	assert.Equal(t, expected, got)

}

func TestWal(t *testing.T) {
	walDir, err := os.MkdirTemp(os.TempDir(), "waltest")
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(walDir))
	})

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	w := New(log, Opts{
		PartitionsPath:     walDir,
		PartitionSizeInSec: 2, // every 2 seconds - new wal partition
		TimeNow:            time.Now,
	})

	expectedTimeSeries := []domain.WalEntity{
		domain.WalEntity{
			Timestamp: time.Now().Unix(),
			TimeSeries: []domain.TimeSeries{
				{
					Labels: []domain.Label{
						{
							Name:  "abc",
							Value: "value",
						},
					},
					Samples: []domain.Sample{
						{
							Value:     132,
							Timestamp: 321,
						},
						{
							Value:     134,
							Timestamp: 322,
						},
					},
				},
			},
		},
		domain.WalEntity{
			Timestamp: time.Now().Unix(),
			TimeSeries: []domain.TimeSeries{
				{
					Labels: []domain.Label{
						{
							Name:  "abc2",
							Value: "value2",
						},
					},
					Samples: []domain.Sample{
						{
							Value:     133,
							Timestamp: 323,
						},
						{
							Value:     135,
							Timestamp: 326,
						},
					},
				},
			},
		},
		domain.WalEntity{
			Timestamp: time.Now().Unix(),
			TimeSeries: []domain.TimeSeries{
				{
					Labels: []domain.Label{
						{
							Name:  "abc3",
							Value: "value3",
						},
					},
					Samples: []domain.Sample{
						{
							Value:     134,
							Timestamp: 325,
						},
						{
							Value:     136,
							Timestamp: 327,
						},
					},
				},
			},
		},
		domain.WalEntity{
			Timestamp: time.Now().Unix(),
			TimeSeries: []domain.TimeSeries{
				{
					Labels: []domain.Label{
						{
							Name:  "abc4",
							Value: "value4",
						},
					},
					Samples: []domain.Sample{
						{
							Value:     135,
							Timestamp: 326,
						},
						{
							Value:     137,
							Timestamp: 328,
						},
					},
				},
			},
		},
	}

	// Append WAL entries
	for i, s := range expectedTimeSeries {
		assert.NoError(t, w.Append(s))

		if i%2 == 0 {
			time.Sleep(2 * time.Second)
		}
	}

	// 3 wal partitions are expected
	files, err := w.listWalFiles()
	assert.NoError(t, err)
	assert.Len(t, files, 3)

	// Get data to replay
	walEntries, err := w.Replay()
	assert.NoError(t, err)
	assert.Equal(t, expectedTimeSeries, walEntries)
}
