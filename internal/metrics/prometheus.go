// Package metrics adapts event.Metrics to Prometheus.
//
// Metric names, all carrying a database label (the namespace, when set, is
// prefixed with an underscore):
//
//	events_total          counter    events delivered to the sink
//	events_dropped_total  counter    events removed by a filter (also op, table)
//	events_skipped_total  counter    events skipped on a permanent sink error
//	sink_latency_seconds  histogram  duration of each sink Send attempt
//	slot_lag_bytes        gauge      pg_current_wal_lsn() minus confirmed_flush_lsn
//	wal_status            gauge      reserved 0, extended 1, unreserved 2, lost 3, unknown -1
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type spec struct {
	help   string
	labels []string
}

var (
	counters = map[string]spec{
		"events_total":         {"Events delivered to the sink.", []string{"database"}},
		"events_dropped_total": {"Events removed by a filter.", []string{"database", "op", "table"}},
		"events_skipped_total": {"Events skipped after a permanent sink error.", []string{"database"}},
	}
	gauges = map[string]spec{
		"slot_lag_bytes": {"Bytes of WAL between the server position and the slot's confirmed flush LSN.", []string{"database"}},
		"wal_status":     {"Slot wal_status: reserved 0, extended 1, unreserved 2, lost 3, unknown -1.", []string{"database"}},
	}
	histograms = map[string]spec{
		"sink_latency_seconds": {"Duration of each sink Send attempt.", []string{"database"}},
	}
	latencyBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
)

// Prometheus implements event.Metrics on a registry the caller owns. Names it
// does not know are ignored, as are labels a metric does not declare; a
// declared label that is missing is empty.
type Prometheus struct {
	counters   map[string]*prometheus.CounterVec
	gauges     map[string]*prometheus.GaugeVec
	histograms map[string]*prometheus.HistogramVec
	order      map[string][]string
}

// NewPrometheus registers every walx metric on reg under namespace (may be
// empty). It panics if reg already holds a clashing metric.
func NewPrometheus(reg prometheus.Registerer, namespace string) *Prometheus {
	p := &Prometheus{
		counters:   map[string]*prometheus.CounterVec{},
		gauges:     map[string]*prometheus.GaugeVec{},
		histograms: map[string]*prometheus.HistogramVec{},
		order:      map[string][]string{},
	}
	for name, s := range counters {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: namespace, Name: name, Help: s.help}, s.labels)
		reg.MustRegister(v)
		p.counters[name], p.order[name] = v, s.labels
	}
	for name, s := range gauges {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: namespace, Name: name, Help: s.help}, s.labels)
		reg.MustRegister(v)
		p.gauges[name], p.order[name] = v, s.labels
	}
	for name, s := range histograms {
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: namespace, Name: name, Help: s.help, Buckets: latencyBuckets}, s.labels)
		reg.MustRegister(v)
		p.histograms[name], p.order[name] = v, s.labels
	}
	return p
}

// Count adds delta to a counter. A negative delta is ignored.
func (p *Prometheus) Count(name string, delta float64, labels ...string) {
	if v, ok := p.counters[name]; ok && delta >= 0 {
		v.WithLabelValues(p.values(name, labels)...).Add(delta)
	}
}

// Gauge sets a gauge.
func (p *Prometheus) Gauge(name string, value float64, labels ...string) {
	if v, ok := p.gauges[name]; ok {
		v.WithLabelValues(p.values(name, labels)...).Set(value)
	}
}

// Observe records a histogram observation.
func (p *Prometheus) Observe(name string, value float64, labels ...string) {
	if v, ok := p.histograms[name]; ok {
		v.WithLabelValues(p.values(name, labels)...).Observe(value)
	}
}

// values orders the key/value pairs by the metric's declared labels.
func (p *Prometheus) values(name string, kv []string) []string {
	declared := p.order[name]
	out := make([]string, len(declared))
	for i, l := range declared {
		for j := 0; j+1 < len(kv); j += 2 {
			if kv[j] == l {
				out[i] = kv[j+1]
			}
		}
	}
	return out
}

// Handler serves reg in the Prometheus text format.
func Handler(reg prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
