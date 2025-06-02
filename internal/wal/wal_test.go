package wal

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: finish the test
func TestWal(t *testing.T) {
	walDir, err := os.MkdirTemp(os.TempDir(), "waltest")
	assert.NoError(t, err)

	t.Cleanup(func() {
		assert.NoError(t, os.RemoveAll(walDir))
	})

	files := []string{
		"wal_123",
		"wal_321",
		"wal_124",
		"wal_2",
		"wal_9999",
		"wal_9991",
		"adsfasdf_1",
	}

	for _, f := range files {
		ff, err := os.Create(walDir + "/" + f)
		assert.NoError(t, err)

		ff.Close()
	}

	w := New(nil, Opts{
		PartitionsPath: walDir,
	})

	fmt.Println(w.listWalFiles())
}
