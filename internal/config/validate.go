package config

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/benipranata/walx/internal/event"
)

var (
	sinkTypes       = []string{"stdout", "file"}
	retryPolicies   = []string{"forever", "max_attempts", "stop"}
	permanentErrors = []string{"stop", "skip"}
	ops             = []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate}
)

// maxIdentLen is the longest Postgres identifier, which also bounds slot names.
const maxIdentLen = 63

var (
	// A name is embedded in Event.ID and the default slot and publication, so
	// it is limited to what a slot name allows, minus upper case.
	namePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	slotPattern = regexp.MustCompile(`^[a-z0-9_]+$`)
)

type reporter func(key, format string, args ...any)

// validate checks the decoded config and reports every problem it finds.
func (c *Config) validate() error {
	var errs []error
	bad := func(key, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
	}

	if c.ShutdownTimeout <= 0 {
		bad("shutdown_timeout", "must be positive")
	}
	if len(c.Databases) == 0 {
		bad("databases", "at least one entry is required")
	}

	names := map[string]bool{}
	paths := map[string]bool{}
	slots := map[[2]string]bool{}
	for i := range c.Databases {
		d := &c.Databases[i]
		key := func(k string) string { return fmt.Sprintf("databases[%d].%s", i, k) }

		switch {
		case d.Name == "":
			bad(key("name"), "required")
		case !namePattern.MatchString(d.Name):
			bad(key("name"), "must match %s, got %q", namePattern, d.Name)
		case names[d.Name]:
			bad(key("name"), "duplicate name %q", d.Name)
		}
		names[d.Name] = true

		validatePostgres(d.Postgres, key, bad)
		// The slot defaults from the name, so it is empty only for an unnamed
		// entry, which is already reported.
		if id := [2]string{d.Postgres.URL, d.Postgres.Slot}; d.Postgres.URL != "" && d.Postgres.Slot != "" {
			if slots[id] {
				bad(key("postgres.slot"), "slot %q is already used with the same url", d.Postgres.Slot)
			}
			slots[id] = true
		}

		validateSink(d.Sink, key, bad)
		if d.Sink.Type == "file" && d.Sink.Path != "" {
			p, err := filepath.Abs(d.Sink.Path)
			if err != nil {
				p = filepath.Clean(d.Sink.Path)
			}
			if paths[p] {
				bad(key("sink.path"), "another file sink already writes to %q", d.Sink.Path)
			}
			paths[p] = true
		}

		for j, t := range d.Tables {
			validateTable(t, func(k string) string { return key(fmt.Sprintf("tables[%d].%s", j, k)) }, bad)
		}
		validateRetry(d.Retry, key, bad)
		if !slices.Contains(permanentErrors, d.OnPermanentError) {
			bad(key("on_permanent_error"), "must be one of %s", strings.Join(permanentErrors, ", "))
		}
	}
	return errors.Join(errs...)
}

func validatePostgres(p PostgresConfig, key func(string) string, bad reporter) {
	if p.URL == "" {
		bad(key("postgres.url"), "required")
	} else if u, err := url.Parse(p.URL); err != nil {
		bad(key("postgres.url"), "not a valid URL: %v", err)
	} else if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		bad(key("postgres.url"), "scheme must be postgres or postgresql")
	}
	if p.MaxTxBytes <= 0 {
		bad(key("postgres.max_tx_bytes"), "must be positive")
	}
	// Empty only for an unnamed entry, which is already reported.
	if p.Slot != "" && (!slotPattern.MatchString(p.Slot) || len(p.Slot) > maxIdentLen) {
		bad(key("postgres.slot"), "must be 1-%d characters of a-z, 0-9 and _, got %q", maxIdentLen, p.Slot)
	}
	if len(p.Publication) > maxIdentLen {
		bad(key("postgres.publication"), "must be at most %d characters, got %q", maxIdentLen, p.Publication)
	}
}

func validateSink(s SinkConfig, key func(string) string, bad reporter) {
	if !slices.Contains(sinkTypes, s.Type) {
		bad(key("sink.type"), "must be one of %s", strings.Join(sinkTypes, ", "))
	}
	if s.Type == "file" && s.Path == "" {
		bad(key("sink.path"), "required for a file sink")
	}
}

func validateTable(t TableConfig, key func(string) string, bad reporter) {
	schema, table, _ := strings.Cut(t.Name, ".")
	if strings.Count(t.Name, ".") != 1 || schema == "" || table == "" || strings.Contains(t.Name, `"`) {
		bad(key("name"), "must be schema.table, got %q", t.Name)
	}
	if len(t.Ops) == 0 {
		bad(key("ops"), "must not be empty; omit it for the default")
	}
	for i, op := range t.Ops {
		if !slices.Contains(ops, op) {
			bad(key(fmt.Sprintf("ops[%d]", i)), "unknown op %q", op)
		}
	}
}

func validateRetry(r RetryConfig, key func(string) string, bad reporter) {
	if !slices.Contains(retryPolicies, r.Policy) {
		bad(key("retry.policy"), "must be one of %s", strings.Join(retryPolicies, ", "))
	}
	if r.Initial <= 0 {
		bad(key("retry.initial"), "must be positive")
	}
	if r.Max <= 0 {
		bad(key("retry.max"), "must be positive")
	} else if r.Max < r.Initial {
		bad(key("retry.max"), "must not be below retry.initial")
	}
	if r.MaxAttempts < 0 {
		bad(key("retry.max_attempts"), "must not be negative")
	}
}
