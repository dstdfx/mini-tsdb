package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/dstdfx/mini-tsdb/internal/domain"
	"github.com/dstdfx/mini-tsdb/internal/storage"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)

	storage := storage.NewTimeSeriesInMemory()

	r := http.NewServeMux()
	r.Handle("/api/v1/write", handleWrite(logger, storage))
	r.Handle("/api/v1/read", handleRead(logger, storage))

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

func handleWrite(log *slog.Logger, s *storage.TimeSeriesInMemory) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Info("Received write request",
			slog.String("method", r.Method),
			slog.String("url", r.URL.String()))

		// Read the payload
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Error("Failed to read request body", slog.String("error", err.Error()))
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)

			return
		}
		defer r.Body.Close()

		// Decode it
		decoded, err := snappy.Decode(nil, body)
		if err != nil {
			log.Error("Failed to decode snappy", slog.String("error", err.Error()))
			http.Error(w, "cannot decode snappy", http.StatusBadRequest)

			return
		}

		// Unmarshal the request
		var request prompb.WriteRequest
		if err := proto.Unmarshal(decoded, &request); err != nil {
			log.Error("Failed to unmarshal protobuf", slog.String("error", err.Error()))
			http.Error(w, "cannot unmarshal protobuf", http.StatusBadRequest)

			return
		}

		// Process the request: read the timeseries and write them to storage
		for _, ts := range request.Timeseries {
			labels := make([]domain.Label, len(ts.Labels))
			for i, label := range ts.Labels {
				labels[i] = domain.Label{
					Name:  string(label.Name),
					Value: string(label.Value),
				}
			}

			samples := make([]domain.Sample, len(ts.Samples))
			for i, sample := range ts.Samples {
				samples[i] = domain.Sample{
					Timestamp: sample.Timestamp,
					Value:     sample.Value,
				}
			}

			if len(labels) == 0 || len(samples) == 0 {
				continue
			}

			log.Info("Writing timeseries to storage",
				slog.Any("labels", labels),
				slog.Any("samples", samples))

			s.Write(labels, samples)
		}

		w.WriteHeader(http.StatusOK)
	}
}

// {"time":"2025-05-23T22:19:20.871392+02:00","level":"INFO","msg":"Writing timeseries to storage","labels":[
// 	{"Name":"__name__","Value":"process_network_transmit_bytes_total"},
// 	{"Name":"instance","Value":"host.docker.internal:8080"},
// 	{"Name":"job","Value":"test-scraper"}],
// 	"samples":[{"Value":165163,"Timestamp":1748031559050}]}

func handleRead(log *slog.Logger, s *storage.TimeSeriesInMemory) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: implement read request handling
		_ = prompb.ReadRequest{}

		log.Info("Received read request",
			slog.String("method", r.Method),
			slog.String("url", r.URL.String()))

		_ = s

		w.WriteHeader(http.StatusOK)
	}
}
