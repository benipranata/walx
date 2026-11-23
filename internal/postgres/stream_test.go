package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestStreamArgs(t *testing.T) {
	tests := []struct {
		name string
		pub  string
		want []string
	}{
		{name: "plain", pub: "walx_a", want: []string{"proto_version '1'", `publication_names '"walx_a"'`}},
		{name: "quote and mixed case", pub: `P'x"`, want: []string{"proto_version '1'", `publication_names '"P''x"""'`}},
		{name: "backslash is literal in the replication scanner", pub: `a\b`, want: []string{"proto_version '1'", `publication_names '"a\b"'`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, streamArgs(tt.pub))
		})
	}
}

func TestClassifyStart(t *testing.T) {
	pgErr := func(code, msg string) error { return &pgconn.PgError{Code: code, Message: msg} }
	tests := []struct {
		name string
		err  error
		is   error
	}{
		{name: "active", err: pgErr(codeObjectInUse, `replication slot "s" is active for PID 12`), is: ErrSlotActive},
		{name: "missing", err: pgErr(codeUndefinedObject, `replication slot "s" does not exist`), is: ErrSlotMissing},
		{name: "missing localized", err: pgErr(codeUndefinedObject, `le slot de réplication « s » n'existe pas`), is: ErrSlotMissing},
		{name: "lost", err: pgErr(codeNotInPrerequisite, `can no longer access replication slot "s"`), is: ErrSlotLost},
		{name: "lost localized", err: pgErr(codeNotInPrerequisite, `Replikationsslot »s« kann nicht mehr verwendet werden`), is: ErrSlotLost},
		{name: "active localized", err: pgErr(codeObjectInUse, "slot actif"), is: ErrSlotActive},
		{name: "physical slot", err: pgErr(codeNotInPrerequisite, "cannot use physical replication slot for logical decoding"), is: ErrSlotLost},
		{name: "option unknown", err: pgErr("22023", `option "proto_version" = "1" is unknown`)},
		{name: "protocol too new", err: pgErr("0A000", "client sent proto_version=99 but server only supports protocol 4 or lower")},
		{name: "bad publication_names", err: pgErr("42602", "invalid publication_names syntax")},
		{name: "unrecognized option", err: pgErr("XX000", "unrecognized pgoutput option: bogus")},
		{name: "other code", err: pgErr("42809", `cannot use physical replication slot for logical decoding`)},
		{name: "not a pg error", err: errors.New("conn closed")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyStart("s", tt.err)
			require.ErrorIs(t, got, tt.err)
			require.ErrorContains(t, got, `"s"`)
			for _, sentinel := range []error{ErrSlotActive, ErrSlotMissing, ErrSlotLost} {
				require.Equal(t, sentinel == tt.is, errors.Is(got, sentinel), "%v", sentinel)
			}
		})
	}
}
