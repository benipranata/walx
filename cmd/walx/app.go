package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/engine"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
	"github.com/benipranata/walx/internal/sink"
)

// app wires the config into one pipeline per database, the optional metrics
// endpoint and the supervisor.
type app struct {
	cfg *config.Config
	reg *sink.Registry
	log *slog.Logger
}

func newApp(cfg *config.Config, reg *sink.Registry, log *slog.Logger) *app {
	return &app{cfg: cfg, reg: reg, log: log}
}

// run checks every database, then streams until ctx ends or a pipeline fails.
// It returns ctx.Err() on a requested stop.
func (a *app) run(ctx context.Context) (err error) {
	var m event.Metrics = event.NopMetrics{}
	var promReg *prometheus.Registry
	var ln net.Listener
	if addr := a.cfg.Metrics.Listen; addr != "" {
		if ln, err = net.Listen("tcp", addr); err != nil {
			return fmt.Errorf("metrics listener: %w", err)
		}
		promReg = prometheus.NewRegistry()
		m = newMetrics(promReg)
	}

	var pipes []*pipeline
	defer func() {
		for _, p := range pipes {
			p.close(ctx)
		}
		if ln != nil {
			_ = ln.Close() // a no-op once the server owns it
		}
	}()
	for _, db := range a.cfg.Databases {
		p, err := preparePipeline(ctx, db, a.reg, a.log)
		if err != nil {
			return err
		}
		pipes = append(pipes, p)
	}
	for _, p := range pipes {
		if err := p.start(ctx, a.log, m, a.cfg.ShutdownTimeout); err != nil {
			return err
		}
	}

	auxCtx, stopAux := context.WithCancel(ctx)
	var aux sync.WaitGroup
	defer func() {
		stopAux()
		aux.Wait()
	}()
	if ln != nil {
		done := serveMetrics(auxCtx, ln, promReg, a.log)
		aux.Go(func() { <-done })
		a.log.Info("metrics listening", "addr", ln.Addr().String())
		for _, p := range pipes {
			poller := postgres.NewLagPoller(p.db.Postgres.URL, p.db.Name, p.db.Postgres.Slot, a.cfg.Metrics.LagInterval, m, a.log)
			aux.Go(func() {
				_ = poller.Run(auxCtx)
				poller.Close(context.WithoutCancel(auxCtx))
			})
		}
	}

	sup := &engine.Supervisor{Logger: a.log}
	for _, p := range pipes {
		p.running = true
		sup.Engines = append(sup.Engines, engine.Named{Name: p.db.Name, Run: p.eng.Run})
	}
	a.log.Info("walx started", "version", version, "databases", len(pipes))
	return sup.Run(ctx)
}
