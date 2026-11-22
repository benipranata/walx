package sinktest_test

import (
	"testing"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/sinktest"
)

func TestFakeContract(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"fake"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sinktest.Run(t, func(t *testing.T) sinktest.Target {
				f := sinktest.NewFake()
				return sinktest.Target{
					Sink:       f,
					Written:    f.Written,
					Break:      f.Break,
					Unwritable: &event.Batch{Events: []*event.Event{{ID: sinktest.PoisonID}}},
				}
			})
		})
	}
}
