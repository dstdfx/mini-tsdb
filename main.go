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

	"github.com/dstdfx/mini-tsdb/internal/api"
	"github.com/dstdfx/mini-tsdb/internal/storage"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)

	storage := storage.NewInMemory()
	r := http.NewServeMux()

	api.InitRoutesV1(r, logger, storage)

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv := &http.Server{
		Addr:    ":9201",
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
