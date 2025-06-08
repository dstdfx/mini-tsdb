package wal

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	log                  *slog.Logger
	partitionSizeInSec   int64
	partitionsPath       string
	currentFileTimestamp int64
	currentFile          *os.File
	timeFn               func() time.Time
	mutex                sync.RWMutex
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

func (l *wal) Run(ctx context.Context) {
	<-ctx.Done()

	l.closeCurrentFile()
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
		if e.IsDir() || !strings.HasSuffix(e.Name(), partitionSuffix) {
			// Skip dirs or not-WAL files
			continue
		}

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

	sort.Slice(files, func(i, j int) bool {
		return files[i].ts < files[j].ts
	})

	return files, nil
}

// Append persists WAL entry to the disk.
func (l *wal) Append(entry domain.WalEntity) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	tNow := l.timeFn().Unix()
	var (
		currentFilename string
		err             error
	)

	if l.currentFile == nil {
		files, err := l.listWalFiles()
		if err != nil {
			return fmt.Errorf("failed to list wal files: %w", err)
		}

		// Check if we have latest wal partition
		if len(files) > 0 {
			currentFilename = files[len(files)-1].name
			l.currentFileTimestamp = files[len(files)-1].ts

			// Open latest wal partition if it's still active
			if files[len(files)-1].ts+l.partitionSizeInSec < tNow {
				l.currentFile, err = l.openFile(currentFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
				if err != nil {
					return fmt.Errorf("failed to create new wal partition: %w", err)
				}
			}
		}
	}

	// Check if we should create a new wal partition
	if l.currentFileTimestamp+l.partitionSizeInSec >= tNow || l.currentFileTimestamp == 0 {
		// Build next wal file name
		l.currentFileTimestamp = l.getNextPartitionTs()
		currentFilename = strconv.FormatInt(l.currentFileTimestamp, 10) + partitionSuffix

		// Close previous wal file
		l.closeCurrentFile()

		l.currentFile, err = l.openFile(currentFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
		if err != nil {
			return fmt.Errorf("failed to create new wal partition: %w", err)
		}
	}

	if err := writeEntity(l.currentFile, entry); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	// TODO: consider doing fsync every N writes or every N seconds
	return l.currentFile.Sync()
}

func (l *wal) closeCurrentFile() {
	// Close previous wal file
	if l.currentFile != nil {
		err := l.currentFile.Close()
		if err != nil {
			l.log.Error("failed to close wal file", slog.Any("err", err))
		}
	}
}

func (l *wal) getNextPartitionTs() int64 {
	return l.timeFn().UTC().
		Truncate(time.Duration(l.partitionSizeInSec) * time.Second).
		Add(time.Duration(l.partitionSizeInSec) * time.Second).Unix()
}

func (l *wal) openFile(filename string, mode int) (*os.File, error) {
	f, err := os.OpenFile(l.partitionsPath+"/"+filename, mode, 0644)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (l *wal) Replay() ([]domain.WalEntity, error) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// TODO: for now read all available files
	files, err := l.listWalFiles()
	if err != nil {
		return nil, fmt.Errorf("failed to list wal files: %w", err)
	}

	// TODO: read files in parallel
	result := make([]domain.WalEntity, 0)
	for _, file := range files {
		entries, err := l.readWalFile(file.name)
		if err != nil {
			l.log.Error("failed to read wal file",
				slog.String("file", file.name),
				slog.Any("error", err))
		} else {
			result = append(result, entries...)
		}
	}

	return result, nil
}

func writeEntity(w io.Writer, data domain.WalEntity) error {
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

func (l *wal) readWalFile(filename string) ([]domain.WalEntity, error) {
	f, err := l.openFile(filename, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return readNRecords(f, -1)
}

func readNRecords(r io.Reader, n int) ([]domain.WalEntity, error) {
	var result []domain.WalEntity

	for i := 0; n <= 0 || i < n; i++ {
		var length uint32
		if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
			if errors.Is(err, io.EOF) {
				break // reached end
			}
			return nil, fmt.Errorf("read length: %w", err)
		}

		buf := make([]byte, length)

		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read data: %w", err)
		}

		var record domain.WalEntity
		if err := json.Unmarshal(buf, &record); err != nil {
			return nil, fmt.Errorf("decode json: %w", err)
		}

		result = append(result, record)
	}

	return result, nil
}
