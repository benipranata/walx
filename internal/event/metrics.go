package event

// Metrics records counters, gauges and histogram observations. Labels are key/value pairs.
type Metrics interface {
	Count(name string, delta float64, labels ...string)
	Gauge(name string, value float64, labels ...string)
	Observe(name string, value float64, labels ...string)
}

// NopMetrics is a Metrics that discards everything.
type NopMetrics struct{}

// Count does nothing.
func (NopMetrics) Count(string, float64, ...string) {}

// Gauge does nothing.
func (NopMetrics) Gauge(string, float64, ...string) {}

// Observe does nothing.
func (NopMetrics) Observe(string, float64, ...string) {}
