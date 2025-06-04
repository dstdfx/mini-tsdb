package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dstdfx/mini-tsdb/internal/domain"
)

// TODO: on start of the service:
// 1. run replay that will load N latest partitions so we can apply them in storage
// 2. we need to know what was the last wal file -> check wal directory, get latest one

// TODO: append
// 1. writes data to current wal file + fsync

const partitionSuffix = ".wal"

type wal struct {
	log                *slog.Logger
	partitionSizeInSec int64
	partitionsPath     string
	currentFile        *os.File
	timeFn             func() time.Time
}

type Opts struct {
	PartitionSizeInSec int64
	PartitionsPath     string
	TimeNow            func() time.Time
}

func New(log *slog.Logger, opts Opts) *wal {
	return &wal{
		log:                log,
		partitionSizeInSec: opts.PartitionSizeInSec,
		partitionsPath:     opts.PartitionsPath,
		timeFn:             opts.TimeNow,
	}
}

type walFile struct {
	name string
	ts   int64
}

// listWalFiles returns a sorted list (asc) of wal partition files.
func (l *wal) listWalFiles() ([]walFile, error) {
	entries, err := os.ReadDir(l.partitionsPath)
	if err != nil {
		return nil, err
	}

	files := make([]walFile, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), partitionSuffix) {
			// Get partition timestamp
			withoutPrefix, _ := strings.CutSuffix(e.Name(), partitionSuffix)
			ts, err := strconv.ParseInt(withoutPrefix, 10, 64)
			if err != nil {
				// Skip invalid files
				continue
			}

			files = append(files, walFile{
				name: e.Name(),
				ts:   ts,
			})
		}
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].ts < files[j].ts
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

		// Check if we have wal files and if can use the latest partition
		if len(files) > 0 && files[len(files)-1].ts < l.timeFn().Unix() {
			// TODO:
			// 1. use the lates file files[len(files)-1] as they are already sorted by timestamp
			// 2. check if we need to create a new partition file:
			// (latest partitition + partition size > time.Now())

			l.currentFile, err = l.openFile(files[len(files)-1].name)
			if err != nil {
				// TODO: write it to a temporary file anyway or keep a local state and then flush it?
				return fmt.Errorf("failed to open a wal file: %w", err)
			}
		} else {
			// Create new partiton
			l.currentFile, err = l.openFile(strconv.FormatInt(l.getNextPartitionTs(), 10) + partitionSuffix)
			if err != nil {
				return fmt.Errorf("failed to create new wal partition: %w", err)
			}
		}
	}

	if err := writeEntity(l.currentFile, entry); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	return l.currentFile.Sync()
}

func (l *wal) getNextPartitionTs() int64 {
	return l.timeFn().UTC().
		Truncate(time.Duration(l.partitionSizeInSec) * time.Second).
		Add(time.Duration(l.partitionSizeInSec) * time.Second).Unix()
}

func (l *wal) openFile(filename string) (*os.File, error) {
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (l *wal) Replay() ([]domain.WalEntity, error) {
	// TODO: get files for the last N minutes/hours and replay them so inmemory storage
	// has all needed data
	return nil, nil
}

func writeEntity(w io.Writer, data any) error {
	// for the sake of simplicity encode to json for now
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// <4-bytes-length><N-bytes-json-string>
	length := uint32(len(jsonBytes))
	err = binary.Write(w, binary.LittleEndian, length)
	if err != nil {
		return err
	}

	_, err = w.Write(jsonBytes)

	return err
}

func readEntity(r io.Reader, v any) error {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return err
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}

	return json.Unmarshal(buf, v)
}
