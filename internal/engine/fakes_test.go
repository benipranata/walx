package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/mock"

	evmock "github.com/benipranata/walx/internal/event/mock"

	"github.com/benipranata/walx/internal/event"
)

// fakeSource is a Source the test feeds: Run blocks until the context ends or
// an error is sent on runErr; when atFinal is set it then receives the number
// of confirms seen at that point (what Standby's final status update carries).
type fakeSource struct {
	out       chan pglogrepl.XLogData
	ka        chan pglogrepl.LSN
	runErr    chan error
	atFinal   chan int
	mu        sync.Mutex
	confirmed []pglogrepl.LSN
}

func newFakeSource() *fakeSource {
	return &fakeSource{out: make(chan pglogrepl.XLogData), ka: make(chan pglogrepl.LSN, 1), runErr: make(chan error, 1)}
}

func newFinalSource() *fakeSource {
	s := newFakeSource()
	s.atFinal = make(chan int, 1)
	return s
}

func (f *fakeSource) Run(ctx context.Context) error {
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-f.runErr:
	}
	if f.atFinal != nil {
		f.atFinal <- len(f.Confirmed())
	}
	return err
}

func (f *fakeSource) Messages() <-chan pglogrepl.XLogData { return f.out }

func (f *fakeSource) Keepalives() <-chan pglogrepl.LSN { return f.ka }

func (f *fakeSource) SetConfirmed(l pglogrepl.LSN) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmed = append(f.confirmed, l)
}

// Confirmed returns the LSNs passed to SetConfirmed so far.
func (f *fakeSource) Confirmed() []pglogrepl.LSN {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pglogrepl.LSN(nil), f.confirmed...)
}

// recMetrics is a MockMetrics that records every call.
type recMetrics struct {
	*evmock.MockMetrics
	mu     sync.Mutex
	counts map[string]float64
	labels map[string][]string
	obs    map[string][]float64
}

func newRecMetrics(t *testing.T) *recMetrics {
	m := &recMetrics{
		MockMetrics: evmock.NewMockMetrics(t),
		counts:      map[string]float64{},
		labels:      map[string][]string{},
		obs:         map[string][]float64{},
	}
	m.EXPECT().Count(mock.Anything, mock.Anything).Run(func(name string, d float64, _ ...string) {
		m.count(name, d, nil)
	}).Maybe()
	m.EXPECT().Count(mock.Anything, mock.Anything, mock.Anything).Run(func(name string, d float64, l ...string) {
		m.count(name, d, l)
	}).Maybe()
	m.EXPECT().Gauge(mock.Anything, mock.Anything).Return().Maybe()
	m.EXPECT().Gauge(mock.Anything, mock.Anything, mock.Anything).Return().Maybe()
	m.EXPECT().Observe(mock.Anything, mock.Anything).Run(func(name string, v float64, _ ...string) {
		m.observe(name, v)
	}).Maybe()
	m.EXPECT().Observe(mock.Anything, mock.Anything, mock.Anything).Run(func(name string, v float64, _ ...string) {
		m.observe(name, v)
	}).Maybe()
	return m
}

func (m *recMetrics) count(name string, d float64, l []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[name] += d
	m.labels[name] = l
}

func (m *recMetrics) observe(name string, v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.obs[name] = append(m.obs[name], v)
}

func (m *recMetrics) observed(name string) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]float64(nil), m.obs[name]...)
}

func (m *recMetrics) get(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[name]
}

// closeSink is a MockSink whose Send delegates to send and whose Close is counted.
type closeSink struct {
	*evmock.MockSink
	closed atomic.Int32
}

func newCloseSink(t *testing.T, send func(ctx context.Context, b event.Batch) error) *closeSink {
	s := &closeSink{MockSink: evmock.NewMockSink(t)}
	s.EXPECT().Send(mock.Anything, mock.Anything).RunAndReturn(send).Maybe()
	s.EXPECT().Close().RunAndReturn(func() error { s.closed.Add(1); return nil }).Maybe()
	return s
}

// newCloseFuncSink is a MockSink with the given Send and Close behaviour.
func newCloseFuncSink(t *testing.T, send func(ctx context.Context, b event.Batch) error, closeFn func() error) *evmock.MockSink {
	s := evmock.NewMockSink(t)
	s.EXPECT().Send(mock.Anything, mock.Anything).RunAndReturn(send).Maybe()
	s.EXPECT().Close().RunAndReturn(closeFn).Maybe()
	return s
}

// memSink is a MockSink that records batches and can block until gate closes.
type memSink struct {
	*evmock.MockSink
	mu      sync.Mutex
	batches []event.Batch
	gate    chan struct{}
}

func newMemSink(t *testing.T) *memSink {
	s := &memSink{MockSink: evmock.NewMockSink(t)}
	s.EXPECT().Send(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, b event.Batch) error {
		if s.gate != nil {
			select {
			case <-s.gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.batches = append(s.batches, b)
		return nil
	}).Maybe()
	s.EXPECT().Close().Return(nil).Maybe()
	return s
}

func newGatedMemSink(t *testing.T) *memSink {
	s := newMemSink(t)
	s.gate = make(chan struct{})
	return s
}

// Batches returns the batches recorded so far.
func (s *memSink) Batches() []event.Batch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]event.Batch(nil), s.batches...)
}

// dlRecorder is a MockDeadLetter that records SendFailed calls; errs are
// returned in order, then nil. When gate is set SendFailed waits for it.
type dlRecorder struct {
	*evmock.MockDeadLetter
	mu      sync.Mutex
	errs    []error
	gate    chan struct{}
	batches []event.Batch
	causes  []error
	calls   int
	closed  int
}

func newDLRecorder(t *testing.T, errs []error, gated bool) *dlRecorder {
	f := &dlRecorder{MockDeadLetter: evmock.NewMockDeadLetter(t), errs: errs}
	if gated {
		f.gate = make(chan struct{})
	}
	f.EXPECT().SendFailed(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, b event.Batch, cause error) error {
			if f.gate != nil {
				select {
				case <-f.gate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			f.calls++
			var err error
			if len(f.errs) > 0 {
				err, f.errs = f.errs[0], f.errs[1:]
			}
			if err == nil {
				f.batches = append(f.batches, b)
				f.causes = append(f.causes, cause)
			}
			return err
		}).Maybe()
	f.EXPECT().Close().RunAndReturn(func() error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.closed++
		return nil
	}).Maybe()
	return f
}

func (f *dlRecorder) snapshot() (calls, closed int, batches []event.Batch, causes []error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.closed, append([]event.Batch(nil), f.batches...), append([]error(nil), f.causes...)
}
