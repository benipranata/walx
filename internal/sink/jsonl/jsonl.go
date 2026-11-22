// Package jsonl encodes events as JSON lines, the shared output format of the
// stdout and file sinks.
package jsonl

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/benipranata/walx/internal/event"
)

// Encode returns one JSON line per event, in order, with HTML escaping off.
// It fails on the first event that cannot be encoded, naming its ID, and
// returns no bytes then.
func Encode(events []*event.Event) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			return nil, fmt.Errorf("encode event %s: %w", e.ID, err)
		}
	}
	return buf.Bytes(), nil
}
