package wal

import (
	"fmt"
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

// TODO: rewrite the test
func TestWal_Append(t *testing.T) {
	// walDir, err := os.MkdirTemp(".", "waltest")
	// assert.NoError(t, err)

	// t.Cleanup(func() {
	// 	assert.NoError(t, os.RemoveAll(walDir))
	// })

	w := New(nil, Opts{
		PartitionsPath:     ".",
		PartitionSizeInSec: 60,
		TimeNow:            time.Now,
	})

	err := w.Append(domain.WalEntity{
		Timestamp: time.Now().Unix(),
		TimeSeries: []domain.TimeSeries{
			{
				Labels: []domain.Label{
					{
						Name:  "test2",
						Value: "qewafsdfasdf",
					},
				},
				Samples: []domain.Sample{
					{
						Value:     555,
						Timestamp: 666,
					},
				},
			},
		},
	})
	assert.NoError(t, err)

	f, err := os.Open("1749157590.wal")
	if err != nil {
		panic(err)
	}

	// Read all records
	allRecords, err := readNRecords(f, -1)
	assert.NoError(t, err)

	for _, r := range allRecords {
		fmt.Println(r)
	}
}
