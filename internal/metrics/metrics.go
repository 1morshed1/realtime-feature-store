// Package metrics holds the Prometheus instrumentation for the feature store.
//
// DESIGN NOTE — what to measure in a feature store, and why
// ----------------------------------------------------------
// You cannot operate infrastructure you cannot see. For a feature store the four vital
// signs are:
//
//   - serving latency: features sit on the inference critical path; p99 latency is a
//     hard SLO. (histogram -> you can compute p50/p95/p99)
//   - freshness: how stale is each feature? A silently stale feature degrades the model
//     without any error. (gauge per feature)
//   - cache hit rate: what fraction of requested features were actually present? Misses
//     mean cold starts or upstream lag. (counters -> ratio)
//   - skew detected: how often does online disagree with a batch recomputation? This is
//     the direct signal of train-serve skew. (counter)
//
// We deliberately give each Metrics instance its OWN prometheus.Registry rather than using
// the global default registry. That keeps tests isolated (no "duplicate metrics
// registration" panics when several services spin up in one test binary) and makes the
// dependency explicit.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles every collector plus the registry they live in.
type Metrics struct {
	reg *prometheus.Registry

	ServingLatency prometheus.Histogram
	Freshness      *prometheus.GaugeVec
	Requests       prometheus.Counter
	Hits           prometheus.Counter
	Misses         prometheus.Counter
	SkewDetected   *prometheus.CounterVec
}

// New builds and registers all collectors on a fresh registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		reg: reg,
		ServingLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "feature_serving_latency_seconds",
			Help: "Latency of GetFeatures calls.",
			// Buckets tuned for a sub-millisecond-to-tens-of-ms serving path.
			Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1},
		}),
		Freshness: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "feature_freshness_seconds",
			Help: "Age (now - event_time) of the most recently served value, per feature.",
		}, []string{"feature"}),
		Requests: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "feature_requests_total",
			Help: "Total feature values requested.",
		}),
		Hits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "feature_cache_hits_total",
			Help: "Requested feature values that were present in the online store.",
		}),
		Misses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "feature_cache_misses_total",
			Help: "Requested feature values that were absent (cold start / lag).",
		}),
		SkewDetected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "skew_detected_total",
			Help: "Times a feature's online value disagreed with the batch recomputation.",
		}, []string{"feature"}),
	}
	reg.MustRegister(m.ServingLatency, m.Freshness, m.Requests, m.Hits, m.Misses, m.SkewDetected)
	return m
}

// Handler exposes the metrics over HTTP for Prometheus to scrape (mount at /metrics).
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Registry exposes the underlying registry (used by tests to gather values).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }
