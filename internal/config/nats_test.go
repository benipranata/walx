package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func natsDoc(sink string) string {
	return "databases:\n  - name: a\n    postgres: {url: \"postgres://u@h/d\"}\n" + sink
}

func TestNatsSinkValidation(t *testing.T) {
	tests := []struct {
		name, sink, want string
	}{
		{"url required", "    sink: {type: nats}\n", "databases[0].sink.url: required for a nats sink"},
		{"url scheme", "    sink: {type: nats, url: \"http://h:4222\"}\n", "databases[0].sink.url"},
		{"url unparseable", "    sink: {type: nats, url: \"nats://h:port\"}\n", "databases[0].sink.url"},
		{"second url of a list", "    sink: {type: nats, url: \"nats://a:4222,http://b\"}\n", "databases[0].sink.url"},
		{"unknown placeholder", "    sink: {type: nats, url: \"nats://h\", subject: \"cdc.{tabel}\"}\n", "databases[0].sink.subject"},
		{"wildcard in subject", "    sink: {type: nats, url: \"nats://h\", subject: \"cdc.*\"}\n", "databases[0].sink.subject"},
		{"empty token in subject", "    sink: {type: nats, url: \"nats://h\", subject: \"cdc..x\"}\n", "databases[0].sink.subject"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(natsDoc(tt.sink)))
			require.ErrorContains(t, err, tt.want)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestNatsSinkErrorHidesCredentials(t *testing.T) {
	tests := []struct{ name, sink string }{
		{"password in bad url", "    sink: {type: nats, url: \"nats://u:secret@h:port\"}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(natsDoc(tt.sink)))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestNatsSinkParse(t *testing.T) {
	tests := []struct {
		name        string
		sink        string
		tables      string
		wantURL     string
		wantSubject string
		wantFSM     map[string]string
	}{
		{"defaults", "    sink: {type: nats, url: \"nats://h:4222\"}\n", "", "nats://h:4222", "", nil},
		{"subject kept", "    sink: {type: nats, url: \"tls://h\", subject: \"x.{database}.{token}\"}\n", "", "tls://h", "x.{database}.{token}", nil},
		{"url list", "    sink: {type: nats, url: \"nats://a:4222,nats://b:4222\"}\n", "", "nats://a:4222,nats://b:4222", "", nil},
		{
			"fsm columns collected", "    sink: {type: nats, url: \"nats://h\"}\n",
			"    tables:\n      - {name: public.orders, fsm_column: status}\n      - {name: public.t}\n",
			"nats://h", "", map[string]string{"public.orders": "status"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mustParse(t, natsDoc(tt.tables+tt.sink))
			s := cfg.Databases[0].Sink
			require.Equal(t, tt.wantURL, s.URL)
			require.Equal(t, tt.wantSubject, s.Subject)
			require.Equal(t, tt.wantFSM, s.FSMColumns)
		})
	}
}

func TestFSMColumnsIsNotAYAMLKey(t *testing.T) {
	tests := []struct{ name, sink string }{
		{"dash key", "    sink: {type: nats, url: \"nats://h\", \"-\": x}\n"},
		{"field name", "    sink: {type: nats, url: \"nats://h\", fsmcolumns: {}}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(natsDoc(tt.sink)))
			require.ErrorContains(t, err, "unknown key")
		})
	}
}
