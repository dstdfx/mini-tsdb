package v1

import (
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/dstdfx/mini-tsdb/internal/domain"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type handler struct {
	log     *slog.Logger
	storage domain.Storage
	wal     domain.Wal
}

func NewHandler(log *slog.Logger, s domain.Storage, w domain.Wal) *handler {
	return &handler{
		log:     log,
		storage: s,
		wal:     w,
	}
}

func (h *handler) RemoteWrite() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.log.Info("Received write request",
			slog.String("method", r.Method),
			slog.String("url", r.URL.String()))

		// Read the payload
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.log.Error("Failed to read request body", slog.String("error", err.Error()))
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)

			return
		}
		defer r.Body.Close()

		// Decode it
		decoded, err := snappy.Decode(nil, body)
		if err != nil {
			h.log.Error("Failed to decode snappy", slog.String("error", err.Error()))
			http.Error(w, "cannot decode snappy", http.StatusBadRequest)

			return
		}

		// Unmarshal the request
		var request prompb.WriteRequest
		if err := proto.Unmarshal(decoded, &request); err != nil {
			h.log.Error("Failed to unmarshal protobuf", slog.String("error", err.Error()))
			http.Error(w, "cannot unmarshal protobuf", http.StatusBadRequest)

			return
		}

		// Parse time series
		timeSeries := make([]domain.TimeSeries, 0, len(request.Timeseries))
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

			timeSeries = append(timeSeries, domain.TimeSeries{
				Labels:  labels,
				Samples: samples,
			})
		}

		// Write data to WAL first
		err = h.wal.Append(domain.WalEntity{
			Timestamp:  time.Now().Unix(),
			TimeSeries: timeSeries,
		})
		if err != nil {
			h.log.Error("failed to append data to wal", slog.Any("error", err))
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		// Write data to in memory storage
		h.storage.WriteMultiple(timeSeries)

		w.WriteHeader(http.StatusOK)
	}
}

func (h *handler) RemoteRead() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h.log.Info("Received read request",
			slog.String("method", r.Method),
			slog.String("url", r.URL.String()))

		// Read the payload
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.log.Error("Failed to read request body", slog.String("error", err.Error()))
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)

			return
		}
		defer r.Body.Close()

		// Decode it
		decoded, err := snappy.Decode(nil, body)
		if err != nil {
			h.log.Error("Failed to decode snappy", slog.String("error", err.Error()))
			http.Error(w, "cannot decode snappy", http.StatusBadRequest)

			return
		}

		// Unmarshal the request
		var request prompb.ReadRequest
		if err := proto.Unmarshal(decoded, &request); err != nil {
			h.log.Error("Failed to unmarshal protobuf", slog.String("error", err.Error()))
			http.Error(w, "cannot unmarshal protobuf", http.StatusBadRequest)

			return
		}

		// Handle read queries
		response := prompb.ReadResponse{
			Results: make([]*prompb.QueryResult, 0, len(request.Queries)),
		}
		for _, q := range request.Queries {
			if q.Hints != nil {
				h.log.Warn("got read hints in the read request, ignoring", slog.Any("hints", q.Hints))
			}

			// Collect matchers
			matchers := make([]domain.LabelMatcher, 0, len(q.Matchers))
			for _, m := range q.Matchers {
				matchers = append(matchers, domain.LabelMatcher{
					Type:  domain.LabelMatcherType(m.Type),
					Name:  m.Name,
					Value: m.Value,
				})
			}

			// Handle query
			result := h.storage.Read(q.StartTimestampMs, q.EndTimestampMs, matchers)

			h.log.Debug("got result from storage", slog.Any("result", result))

			// Collect result
			queryResult := make([]*prompb.TimeSeries, 0, len(result))
			for _, r := range result {
				queryResult = append(queryResult, r.ToProto())
			}

			response.Results = append(response.Results, &prompb.QueryResult{
				Timeseries: queryResult,
			})
		}

		// Encode resposne
		data, err := proto.Marshal(&response)
		if err != nil {
			http.Error(w, "failed to marshal response", http.StatusInternalServerError)
			return
		}
		compressed := snappy.Encode(nil, data)

		// Write response
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		_, err = w.Write(compressed)
		if err != nil {
			h.log.Error("failed to write response", slog.Any("error", err))

			return
		}
	}
}
