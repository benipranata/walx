// Package subject builds NATS subjects from a template. It is separate from
// the nats sink so config can validate a template without importing the
// client.
package subject

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/benipranata/walx/internal/event"
)

// DefaultTemplate is the subject template used when sink.subject is unset.
const DefaultTemplate = "cdc.{schema}.{table}.{token}"

// ErrEmptyToken is returned by Render when a placeholder expands to nothing,
// which would make an invalid subject.
var ErrEmptyToken = errors.New("empty subject token")

// statePrefix marks a state value that would otherwise equal an op name or an
// already prefixed state, so the mapping from state to token stays injective.
const statePrefix = "state_"

var (
	placeholders = []string{"database", "schema", "table", "token"}
	opNames      = []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate, event.OpSnapshot}
)

// Values are the expansions of the placeholders. Token is the op or the
// StateToken of the new state.
type Values struct {
	Database, Schema, Table, Token string
}

// Template is a parsed subject template.
type Template struct {
	tokens [][]part
}

type part struct {
	literal string
	name    string // placeholder name, empty for a literal
}

// Parse validates and parses a template: dot-separated tokens of literal text
// and the placeholders {database}, {schema}, {table} and {token}. Literals
// must not hold whitespace or the wildcards * and >.
func Parse(s string) (*Template, error) {
	if s == "" {
		return nil, errors.New("subject template is empty")
	}
	t := &Template{}
	for _, tok := range strings.Split(s, ".") {
		if tok == "" {
			return nil, fmt.Errorf("subject template %q has an empty token", s)
		}
		parts, err := parseToken(tok)
		if err != nil {
			return nil, fmt.Errorf("subject template %q: %w", s, err)
		}
		t.tokens = append(t.tokens, parts)
	}
	return t, nil
}

func parseToken(tok string) ([]part, error) {
	var parts []part
	for tok != "" {
		open := strings.IndexByte(tok, '{')
		end := strings.IndexByte(tok, '}')
		switch {
		case open < 0 && end < 0:
			parts = append(parts, part{literal: tok})
			tok = ""
		case end >= 0 && (open < 0 || end < open):
			return nil, errors.New("unexpected }")
		default:
			if open > 0 {
				parts = append(parts, part{literal: tok[:open]})
			}
			rest := tok[open:]
			end = strings.IndexByte(rest, '}')
			if end < 0 {
				return nil, errors.New("unclosed {")
			}
			name := rest[1:end]
			if !slices.Contains(placeholders, name) {
				return nil, fmt.Errorf("unknown placeholder %q, want one of {%s}", rest[:end+1], strings.Join(placeholders, "}, {"))
			}
			parts = append(parts, part{name: name})
			tok = rest[end+1:]
		}
	}
	for _, p := range parts {
		if strings.ContainsAny(p.literal, "*>") {
			return nil, fmt.Errorf("literal %q holds a reserved wildcard", p.literal)
		}
		if strings.IndexFunc(p.literal, unicode.IsSpace) >= 0 {
			return nil, fmt.Errorf("literal %q holds whitespace", p.literal)
		}
	}
	return parts, nil
}

// Render expands the template. Every value is sanitized, and a used
// placeholder that is empty fails with ErrEmptyToken.
func (t *Template) Render(v Values) (string, error) {
	vals := map[string]string{"database": v.Database, "schema": v.Schema, "table": v.Table, "token": v.Token}
	toks := make([]string, len(t.tokens))
	for i, parts := range t.tokens {
		var b strings.Builder
		for _, p := range parts {
			if p.name == "" {
				b.WriteString(p.literal)
				continue
			}
			s := Sanitize(vals[p.name])
			if s == "" {
				return "", fmt.Errorf("%w: {%s} is empty", ErrEmptyToken, p.name)
			}
			b.WriteString(s)
		}
		toks[i] = b.String()
	}
	return strings.Join(toks, "."), nil
}

// Sanitize replaces what NATS reserves in a subject token (. * >), plus
// whitespace and control characters, with _. Different inputs can map to the
// same output ("a.b" and "a_b").
func Sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '.' || r == '*' || r == '>' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return '_'
		}
		return r
	}, s)
}

// StateToken returns the token for a state value: sanitized, and prefixed
// with "state_" when it equals an op name or already starts with the prefix,
// so a state can never collide with an op token.
func StateToken(state string) string {
	s := Sanitize(state)
	if strings.HasPrefix(s, statePrefix) || slices.Contains(opNames, event.Op(s)) {
		return statePrefix + s
	}
	return s
}
