package event

// Metrics records counters and gauges. Labels are key/value pairs.
type Metrics interface {
	Count(name string, delta float64, labels ...string)
	Gauge(name string, value float64, labels ...string)
}

// NopMetrics is a Metrics that discards everything.
type NopMetrics struct{}

// Count does nothing.
func (NopMetrics) Count(string, float64, ...string) {}

// Gauge does nothing.
func (NopMetrics) Gauge(string, float64, ...string) {}
