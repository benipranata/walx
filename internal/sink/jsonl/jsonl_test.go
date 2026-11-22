package jsonl_test

import (
	"math"
	"testing"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/jsonl"
	"github.com/stretchr/testify/require"
)

func TestEncode(t *testing.T) {
	tests := []struct {
		name    string
		events  []*event.Event
		want    string
		wantErr string
	}{
		{name: "empty", want: ""},
		{
			name: "html is not escaped, one line per event",
			events: []*event.Event{
				{ID: "a", After: map[string]any{"v": "<b>&"}},
				{ID: "b"},
			},
			want: `{"id":"a","source":"","op":"","schema":"","table":"","after":{"v":"<b>&"},"lsn":"0/0","tx_id":0,"commit_ts":"0001-01-01T00:00:00Z"}` + "\n" +
				`{"id":"b","source":"","op":"","schema":"","table":"","lsn":"0/0","tx_id":0,"commit_ts":"0001-01-01T00:00:00Z"}` + "\n",
		},
		{
			name:    "unencodable event names its id",
			events:  []*event.Event{{ID: "a"}, {ID: "bad", After: map[string]any{"v": math.NaN()}}},
			wantErr: "encode event bad",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := jsonl.Encode(tt.events)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}
