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
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	rootCtx, stop := signal.NotifyContext(context.Background())

	r := http.NewServeMux()
	r.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		logger.Info("Received request",
			slog.String("method", r.Method),
			slog.String("url", r.URL.String()))

		w.Write([]byte("Hello, world!"))
	}))

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
		BaseContext: func(net.Listener) context.Context {
			return rootCtx
		},
	}

	go func() {
		logger.Info("Starting server", slog.String("address", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	<-rootCtx.Done()
	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown failed", slog.String("error", err.Error()))
	} else {
		logger.Info("Server shutdown gracefully")
	}
}
