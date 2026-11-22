package sink_test

import (
	"errors"
	"testing"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink"
	"github.com/benipranata/walx/internal/sink/sinktest"
	"github.com/stretchr/testify/require"
)

func TestRegistryBuild(t *testing.T) {
	built := sinktest.NewFake()
	boom := errors.New("boom")
	tests := []struct {
		name     string
		register map[string]sink.Factory
		cfg      config.SinkConfig
		want     event.Sink
		wantErr  error
	}{
		{
			name: "registered type",
			register: map[string]sink.Factory{
				"stdout": func(db string, c config.SinkConfig) (event.Sink, error) { return built, nil },
			},
			cfg:  config.SinkConfig{Type: "stdout"},
			want: built,
		},
		{
			name:    "unknown type",
			cfg:     config.SinkConfig{Type: "kafka"},
			wantErr: sink.ErrUnknownType,
		},
		{
			name:    "empty type",
			cfg:     config.SinkConfig{},
			wantErr: sink.ErrUnknownType,
		},
		{
			name: "factory error",
			register: map[string]sink.Factory{
				"file": func(db string, c config.SinkConfig) (event.Sink, error) { return nil, boom },
			},
			cfg:     config.SinkConfig{Type: "file"},
			wantErr: boom,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := sink.NewRegistry()
			for typ, f := range tt.register {
				r.Register(typ, f)
			}
			got, err := r.Build("orders", tt.cfg)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Same(t, tt.want, got)
		})
	}
}

func TestRegistryPassesDatabaseAndConfig(t *testing.T) {
	tests := []struct {
		name string
		db   string
		cfg  config.SinkConfig
	}{
		{"file", "orders", config.SinkConfig{Type: "file", Path: "/tmp/x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotDB string
			var gotCfg config.SinkConfig
			r := sink.NewRegistry()
			r.Register(tt.cfg.Type, func(db string, c config.SinkConfig) (event.Sink, error) {
				gotDB, gotCfg = db, c
				return sinktest.NewFake(), nil
			})
			_, err := r.Build(tt.db, tt.cfg)
			require.NoError(t, err)
			require.Equal(t, tt.db, gotDB)
			require.Equal(t, tt.cfg, gotCfg)
		})
	}
}

func TestRegisterPanics(t *testing.T) {
	f := func(string, config.SinkConfig) (event.Sink, error) { return nil, nil }
	tests := []struct {
		name string
		typ  string
		f    sink.Factory
		pre  bool
	}{
		{"duplicate", "stdout", f, true},
		{"empty type", "", f, false},
		{"nil factory", "stdout", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := sink.NewRegistry()
			if tt.pre {
				r.Register(tt.typ, f)
			}
			require.Panics(t, func() { r.Register(tt.typ, tt.f) })
		})
	}
}
