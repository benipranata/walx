package config

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/subject"
)

var (
	natsSchemes     = []string{"nats", "tls", "ws", "wss"}
	sinkTypes       = []string{"stdout", "file", "nats"}
	retryPolicies   = []string{"forever", "max_attempts", "stop"}
	permanentErrors = []string{"stop", "skip"}
	ops             = []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate}
)

// maxIdentLen is the longest Postgres identifier, which also bounds slot names.
const maxIdentLen = 63

var (
	// A name is embedded in Event.ID and the default slot and publication
	// (walx_<name>), so it is limited to 58 characters to fit 63, and to
	// what a slot name allows, minus upper case.
	namePattern  = regexp.MustCompile(`^[a-z][a-z0-9_]{0,57}$`)
	slotPattern  = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`)
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
	if c.Metrics.LagInterval <= 0 {
		bad("metrics.lag_interval", "must be positive")
	}
	if c.Metrics.Listen != "" {
		if _, port, err := net.SplitHostPort(c.Metrics.Listen); err != nil || port == "" {
			bad("metrics.listen", "must look like host:port or :port, got %q", c.Metrics.Listen)
		}
	}
	if len(c.Databases) == 0 {
		bad("databases", "at least one entry is required")
	}

	names := map[string]bool{}
	paths := map[string]bool{}
	slots := map[[2]string]bool{}
	pubs := map[[2]string]bool{}
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
		if server, database, ok := serverOf(d.Postgres.URL); ok {
			// Slots are cluster-wide and publications are per database, so
			// two spellings of one server must not share either name.
			if id := [2]string{server, d.Postgres.Slot}; d.Postgres.Slot != "" {
				if slots[id] {
					bad(key("postgres.slot"), "slot %q is already used on the same server", d.Postgres.Slot)
				}
				slots[id] = true
			}
			if id := [2]string{server + database, d.Postgres.Publication}; d.Postgres.Publication != "" {
				if pubs[id] {
					bad(key("postgres.publication"), "publication %q is already used on the same database", d.Postgres.Publication)
				}
				pubs[id] = true
			}
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

		tables := map[string]bool{}
		for j, t := range d.Tables {
			tkey := func(k string) string { return key(fmt.Sprintf("tables[%d].%s", j, k)) }
			validateTable(t, tkey, bad)
			if tables[t.Name] {
				bad(tkey("name"), "duplicate table %q", t.Name)
			}
			tables[t.Name] = true
		}
		validateRetry(d.Retry, key, bad)
		if !slices.Contains(permanentErrors, d.OnPermanentError) {
			bad(key("on_permanent_error"), "must be one of %s", strings.Join(permanentErrors, ", "))
		}
	}
	return errors.Join(errs...)
}

// serverOf returns the host and port of rawURL, and its database name, with
// the scheme and credentials left out; ok is false for an unparseable URL.
func serverOf(rawURL string) (server, database string, ok bool) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", false
	}
	host, port := u.Hostname(), u.Port()
	if host == "" {
		// A unix-socket URL names the server in the host query parameter.
		host = u.Query().Get("host")
		if host == "" {
			return "", "", false
		}
	}
	if port == "" {
		port = "5432"
	}
	return strings.ToLower(host) + ":" + port, "/" + strings.TrimPrefix(u.Path, "/"), true
}

func validatePostgres(p PostgresConfig, key func(string) string, bad reporter) {
	if p.URL == "" {
		bad(key("postgres.url"), "required")
	} else if u, err := url.Parse(p.URL); err != nil {
		// url.Error embeds the whole URL, password included.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		bad(key("postgres.url"), "not a valid URL: %v", err)
	} else if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		bad(key("postgres.url"), "scheme must be postgres or postgresql")
	}
	if p.MaxTxBytes <= 0 {
		bad(key("postgres.max_tx_bytes"), "must be positive")
	}
	if p.MaxTxEvents < 0 {
		bad(key("postgres.max_tx_events"), "must not be negative")
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
	if s.Timeout < 0 {
		bad(key("sink.timeout"), "must not be negative")
	}
	if s.Type == "file" && s.Path == "" {
		bad(key("sink.path"), "required for a file sink")
	}
	if s.Type != "nats" {
		return
	}
	if s.URL == "" {
		bad(key("sink.url"), "required for a nats sink")
	} else {
		for _, raw := range strings.Split(s.URL, ",") {
			validateNatsURL(strings.TrimSpace(raw), key, bad)
		}
	}
	if s.Subject != "" {
		if _, err := subject.Parse(s.Subject); err != nil {
			bad(key("sink.subject"), "%v", err)
		}
	}
}

func validateNatsURL(raw string, key func(string) string, bad reporter) {
	u, err := url.Parse(raw)
	if err != nil {
		// url.Error embeds the whole URL, password included.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		bad(key("sink.url"), "not a valid URL: %v", err)
	} else if !slices.Contains(natsSchemes, u.Scheme) || u.Host == "" {
		bad(key("sink.url"), "must look like nats://host:port (schemes: %s)", strings.Join(natsSchemes, ", "))
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
		} else if slices.Contains(t.Ops[:i], op) {
			bad(key(fmt.Sprintf("ops[%d]", i)), "duplicate op %q", op)
		}
	}
	for _, f := range []struct {
		name string
		cols []string
	}{{"key", t.Key}, {"on_change", t.OnChange}, {"drop_columns", t.DropColumns}} {
		for i, c := range f.cols {
			if strings.TrimSpace(c) == "" {
				bad(key(fmt.Sprintf("%s[%d]", f.name, i)), "must not be blank")
			} else if slices.Contains(f.cols[:i], c) {
				bad(key(fmt.Sprintf("%s[%d]", f.name, i)), "duplicate column %q", c)
			}
		}
	}
	for _, col := range slices.Sorted(maps.Keys(t.Where)) {
		switch v := t.Where[col]; v.(type) {
		case map[string]any, map[any]any, []any:
			bad(key("where."+col), "value must be a scalar, got %T", v)
		}
	}
	if _, ok := t.Where[""]; ok {
		bad(key("where"), "column name must not be empty")
	}
	validatePublicationFilters(t, key, bad)
	if i := slices.Index(t.DropColumns, t.FSMColumn); t.FSMColumn != "" && i >= 0 {
		bad(key(fmt.Sprintf("drop_columns[%d]", i)), "must not drop the fsm_column %q", t.FSMColumn)
	}
}

func validatePublicationFilters(t TableConfig, key func(string) string, bad reporter) {
	if t.PublicationWhere != "" && strings.TrimSpace(t.PublicationWhere) == "" {
		bad(key("publication_where"), "must not be blank; omit it for no row filter")
	}
	if strings.Contains(t.PublicationWhere, ";") {
		bad(key("publication_where"), "must be one expression and must not contain ';'")
	}
	if t.PublicationColumns == nil {
		return
	}
	if len(t.PublicationColumns) == 0 {
		bad(key("publication_columns"), "must not be empty; omit it to publish every column")
		return
	}
	for i, c := range t.PublicationColumns {
		switch {
		case !identPattern.MatchString(c):
			bad(key(fmt.Sprintf("publication_columns[%d]", i)), "must be a column name (letters, digits, _ and $, at most %d characters), got %q", maxIdentLen, c)
		case slices.Contains(t.PublicationColumns[:i], c):
			bad(key(fmt.Sprintf("publication_columns[%d]", i)), "duplicate column %q", c)
		}
	}
	// The filters read these columns from After, so the column list must carry them.
	required := []struct{ what, col string }{{"fsm_column", t.FSMColumn}}
	for _, c := range t.Key {
		required = append(required, struct{ what, col string }{"key column", c})
	}
	for _, c := range slices.Sorted(maps.Keys(t.Where)) {
		required = append(required, struct{ what, col string }{"where column", c})
	}
	for _, c := range t.OnChange {
		required = append(required, struct{ what, col string }{"on_change column", c})
	}
	for _, r := range required {
		if r.col != "" && !slices.Contains(t.PublicationColumns, r.col) {
			bad(key("publication_columns"), "must include %s %q", r.what, r.col)
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
