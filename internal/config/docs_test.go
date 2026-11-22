package config_test

import (
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestExampleConfigLoads(t *testing.T) {
	tests := []struct {
		name      string
		env       map[string]string
		wantNames []string
	}{
		{
			name: "example with all variables set",
			env: map[string]string{
				"ORDERS_DATABASE_URL":  "postgres://walx:secret@db1.example.com:5432/orders",
				"BILLING_DATABASE_URL": "postgres://walx:secret@db2.example.com:5432/billing",
			},
			wantNames: []string{"orders", "billing"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			cfg, err := config.Load("../../config.example.yaml")
			require.NoError(t, err)
			var names []string
			for _, d := range cfg.Databases {
				names = append(names, d.Name)
			}
			require.Equal(t, tt.wantNames, names)
		})
	}
}

func TestDocsCoverEveryConfigKey(t *testing.T) {
	tests := []struct {
		name string
		doc  string
	}{
		{"configuration reference", "../../docs/configuration.md"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := os.ReadFile(tt.doc)
			require.NoError(t, err)
			doc := string(raw)
			for _, key := range yamlKeyPaths(reflect.TypeFor[config.Config](), "") {
				require.Contains(t, doc, "`"+key+"`", "%s does not document %s", tt.doc, key)
			}
		})
	}
}

// yamlKeyPaths lists the dotted path of every yaml key under t; list elements
// are written as [].
func yamlKeyPaths(t reflect.Type, prefix string) (paths []string) {
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if name == "-" || name == "" {
			continue
		}
		path := prefix + name
		paths = append(paths, path)
		ft := f.Type
		if ft.Kind() == reflect.Slice {
			ft = ft.Elem()
			if ft.Kind() == reflect.Struct {
				path += "[]"
			}
		}
		if ft.Kind() == reflect.Struct {
			paths = append(paths, yamlKeyPaths(ft, path+".")...)
		}
	}
	return paths
}
