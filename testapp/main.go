package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var httpRequests = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total number of HTTP requests",
	},
	[]string{"method", "handler"},
)

func main() {
	prometheus.MustRegister(httpRequests)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		httpRequests.WithLabelValues(r.Method, "/").Inc()
		w.Write([]byte("Hello, metrics!\n"))
	})

	http.Handle("/metrics", promhttp.Handler())

	http.ListenAndServe(":8080", nil)
}
