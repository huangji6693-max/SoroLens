// Package metrics holds SoroLens's Prometheus instrumentation.
//
// Every method is safe on a nil *Metrics, so call sites can hold the
// pointer without nil checks and tests can pass nil. A nil Metrics records
// nothing — instrumentation is never load-bearing.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the process-wide metric set. Construct with New; serve with
// Handler.
type Metrics struct {
	registry *prometheus.Registry

	pollsTotal   *prometheus.CounterVec
	pollDuration prometheus.Histogram
	pollLag      prometheus.Gauge

	eventsIngested prometheus.Counter
	sincePollSec   prometheus.Gauge
	httpDuration   *prometheus.HistogramVec
}

// New returns a Metrics with its own registry, so multiple instances (e.g.
// in tests) never collide on the default registry.
func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),

		pollsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sorolens_polls_total",
			Help: "Ingest polls, by outcome (ok|error). Standalone mode only.",
		}, []string{"outcome"}),

		pollDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "sorolens_poll_duration_seconds",
			Help:    "Wall-clock duration of a single ingest poll cycle.",
			Buckets: prometheus.DefBuckets,
		}),

		pollLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sorolens_ingest_lag_ledgers",
			Help: "How far the stored resume point trails the chain tip, in ledgers.",
		}),

		eventsIngested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sorolens_events_ingested_total",
			Help: "Contract events ingested from the RPC and stored. Standalone mode only.",
		}),

		sincePollSec: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sorolens_seconds_since_last_poll",
			Help: "Seconds since the ingester last completed a cycle. Grows without bound when polling has stopped.",
		}),

		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "sorolens_http_request_duration_seconds",
			Help:    "HTTP request duration by route pattern, method and status.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method", "status"}),
	}
	m.registry.MustRegister(m.pollsTotal, m.pollDuration, m.pollLag,
		m.eventsIngested, m.sincePollSec, m.httpDuration)
	return m
}

// Handler serves the metrics registry in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// RecordPoll observes one completed ingest cycle.
func (m *Metrics) RecordPoll(ok bool, took time.Duration) {
	if m == nil {
		return
	}
	outcome := "ok"
	if !ok {
		outcome = "error"
	}
	m.pollsTotal.WithLabelValues(outcome).Inc()
	m.pollDuration.Observe(took.Seconds())
	m.sincePollSec.Set(0)
}

// SetLag records how far the stored resume point trails the chain tip.
func (m *Metrics) SetLag(ledgersBehind int64) {
	if m == nil {
		return
	}
	m.pollLag.Set(float64(ledgersBehind))
}

// TickPollAge advances the seconds-since-last-poll gauge; called on a timer
// so the gauge climbs visibly when polling has stalled.
func (m *Metrics) TickPollAge(secondsSince float64) {
	if m == nil {
		return
	}
	m.sincePollSec.Set(secondsSince)
}

// RecordEvents counts events stored by the cycle that just ran.
func (m *Metrics) RecordEvents(count int64) {
	if m == nil {
		return
	}
	m.eventsIngested.Add(float64(count))
}

// statusRecorder captures the status code a handler wrote, for the HTTP
// duration metric.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Flush forwards http.Flusher so Server-Sent Events handlers behind Middleware
// can flush each frame to the client immediately.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying ResponseWriter for http.ResponseController.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// Middleware wraps a handler with per-route HTTP duration instrumentation.
// route should be the matched URL pattern, not the raw path, so label
// cardinality stays bounded; use RoutePattern(r) to get it.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m == nil {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := r.URL.Path
		if p := RoutePattern(r); p != "" {
			route = p
		}
		m.httpDuration.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).
			Observe(time.Since(start).Seconds())
	})
}

// RoutePattern extracts the matched route pattern from the request
// context. It is a function variable so this package does not import chi;
// cmd/sorolens wires it to chi's route context.
var RoutePattern func(*http.Request) string
