package nats

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/jsonl"
	"github.com/benipranata/walx/internal/sink/subject"
	"github.com/nats-io/nats.go"
)

// Headers carrying the routing metadata, so a consumer need not decode the
// body. State headers are set only for a table with an fsm_column, and the
// from state only when the event has both row images (never on reference_only).
const (
	HeaderOp         = "Walx-Op"
	HeaderSource     = "Walx-Source"
	HeaderSchema     = "Walx-Schema"
	HeaderTable      = "Walx-Table"
	HeaderCommitLSN  = "Walx-Commit-Lsn"
	HeaderStateFrom  = "Walx-State-From"
	HeaderStateTo    = "Walx-State-To"
	headerValueLimit = 1024
)

// message builds the JetStream message of e: the subject from the template,
// the JSON body, Nats-Msg-Id = Event.ID and the metadata headers.
func (s *Sink) message(e *event.Event, commit event.LSN) (*nats.Msg, error) {
	from, to, hasFrom, hasTo := s.states(e)
	token := string(e.Op)
	if hasTo {
		token = subject.StateToken(to)
	}
	subj, err := s.tmpl.Render(subject.Values{Database: s.database, Schema: e.Schema, Table: e.Table, Token: token})
	if err != nil {
		return nil, fmt.Errorf("event %s: %w", e.ID, err)
	}
	body, err := jsonl.Encode([]*event.Event{e})
	if err != nil {
		return nil, err
	}
	msg := nats.NewMsg(subj)
	msg.Data = bytes.TrimSuffix(body, []byte("\n"))
	msg.Header.Set(nats.MsgIdHdr, e.ID)
	msg.Header.Set(HeaderOp, string(e.Op))
	msg.Header.Set(HeaderSource, headerValue(e.Source))
	msg.Header.Set(HeaderSchema, headerValue(e.Schema))
	msg.Header.Set(HeaderTable, headerValue(e.Table))
	msg.Header.Set(HeaderCommitLSN, commit.String())
	if hasFrom {
		msg.Header.Set(HeaderStateFrom, headerValue(from))
	}
	if hasTo {
		msg.Header.Set(HeaderStateTo, headerValue(to))
	}
	return msg, nil
}

// states returns the fsm_column value before and after the change. The new
// state is After, else Before when After holds Unchanged (a delete has none);
// the old state needs both row images. A NULL, empty or unknown value is no state (an empty
// token would be an invalid subject).
func (s *Sink) states(e *event.Event) (from, to string, hasFrom, hasTo bool) {
	col := s.fsm[e.Schema+"."+e.Table]
	if col == "" {
		return "", "", false, false
	}
	if e.Op != event.OpDelete {
		v, ok := e.After[col]
		if v == event.Unchanged {
			v, ok = e.Before[col]
		}
		if ok && v != event.Unchanged && v != nil && fmt.Sprint(v) != "" {
			to, hasTo = fmt.Sprint(v), true
		}
	}
	if v, _, ok := e.Transition(col); ok && v != event.Unchanged && v != nil {
		from, hasFrom = fmt.Sprint(v), true
	}
	return from, to, hasFrom, hasTo
}

// headerValue keeps a value on one line and short, since a header may not
// hold line breaks.
func headerValue(s string) string {
	s = strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
	if len(s) > headerValueLimit {
		s = s[:headerValueLimit]
	}
	return s
}
