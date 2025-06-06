package api

import (
	"log/slog"
	"net/http"

	v1 "github.com/dstdfx/mini-tsdb/internal/api/v1"
	"github.com/dstdfx/mini-tsdb/internal/domain"
)

// InitRoutesV1 initializes HTTP routes for v1 API.
func InitRoutesV1(r *http.ServeMux, log *slog.Logger, s domain.Storage, w domain.Wal) {
	h := v1.NewHandler(log, s, w)
	r.Handle("/api/v1/write", h.RemoteWrite())
	r.Handle("/api/v1/read", h.RemoteRead())
}
