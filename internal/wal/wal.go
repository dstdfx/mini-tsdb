package wal

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/dstdfx/mini-tsdb/internal/domain"
)

// TODO: on start of the service:
// 1. run replay that will load N latest partitions so we can apply them in storage
// 2. we need to know what was the last wal file -> check wal directory, get latest one

// TODO: append
// 1. writes data to current wal file + fsync

const (
	partitionPrefix = "wal_"
)

type wal struct {
	log                *slog.Logger
	partitionSizeInSec int64
	partitionsPath     string

	currentFile *os.File
}

type Opts struct {
	PartitionSizeInSec int64
	PartitionsPath     string
}

func New(log *slog.Logger, opts Opts) *wal {
	return &wal{
		log:                log,
		partitionSizeInSec: opts.PartitionSizeInSec,
		partitionsPath:     opts.PartitionsPath,
	}
}

// listWalFiles returns a sorted list (asc) of wal partition files.
func (l *wal) listWalFiles() ([]string, error) {
	entries, err := os.ReadDir(l.partitionsPath)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), partitionPrefix) {
			files = append(files, e.Name())
		}
	}

	sort.Slice(files, func(i, j int) bool {
		a, _ := strings.CutPrefix(files[i], partitionPrefix)
		tsA, _ := strconv.Atoi(a)

		b, _ := strings.CutPrefix(files[j], partitionPrefix)
		tsB, _ := strconv.Atoi(b)

		return tsA < tsB
	})

	return files, nil
}

func (l *wal) Append(entry domain.WalEntity) error {
	// TODO: write new entity to the current wal file (current minute/hour partition)
	// do fsync
	// consider doing fsync every N writes or every N seconds?

	if l.currentFile == nil {
		files, err := l.listWalFiles()
		if err != nil {
			return fmt.Errorf("failed to list wal files: %w", err)
		}

		if len(files) > 0 {
			// TODO:
			// 1. use the lates file files[len(files)-1] as they are already sorted by timestamp
			// 2. check if we need to create a new partition file:
			// (latest partitition + partition size > time.Now())
		}

		// TODO: create new partition file here
	}

	// TODO: write wal entry to the file + do fsync

	return nil
}

func (l *wal) Replay() ([]domain.WalEntity, error) {
	// TODO: get files for the last N minutes/hours and replay them so inmemory storage
	// has all needed data
	return nil, nil
}
