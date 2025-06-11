package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/caarlos0/env/v11"
	"github.com/dstdfx/mini-tsdb/internal/api"
	"github.com/dstdfx/mini-tsdb/internal/storage"
	"github.com/dstdfx/mini-tsdb/internal/wal"
)

type config struct {
	PartitionSizeInSec int64  `env:"PARTITION_SIZE_IN_SEC" envDefault:"30"`
	WALPartitionsPath  string `env:"WAL_PARTITIONS_PATH" envDefault:"waldata"`
	Addr               string `env:"PORT" envDefault:":9201"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Parse app configuration
	var cfg config
	err := env.Parse(&cfg)
	if err != nil {
		logger.Error("failed to parse app config", slog.String("error", err.Error()))
		os.Exit(1)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)

	storage := storage.NewInMemory()
	w := wal.New(logger, wal.Opts{
		PartitionSizeInSec: cfg.PartitionSizeInSec,
		PartitionsPath:     cfg.WALPartitionsPath,
		TimeNow:            time.Now,
	})

	// Init storage state
	logger.Info("init state from WAL")
	entities, err := w.Replay()
	if err != nil {
		panic(err)
	}
	for _, e := range entities {
		storage.WriteMultiple(e.TimeSeries)
	}

	r := http.NewServeMux()

	api.InitRoutesV1(r, logger, storage, w)

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &http.Server{
		Addr:    cfg.Addr,
		Handler: r,
		BaseContext: func(net.Listener) context.Context {
			return baseCtx
		},
	}

	go func() {
		logger.Info("Starting server", slog.String("address", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	<-rootCtx.Done()
	stop()
	cancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown failed", slog.String("error", err.Error()))
	} else {
		logger.Info("Server shutdown gracefully")
	}
}
