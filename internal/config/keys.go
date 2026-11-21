package config

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
)

// lineKeys remembers the key path written on each line, so that decode errors,
// which only carry a line, can name the key. A line holding several keys (flow
// style) maps to "", since the error cannot say which one it means.
type lineKeys map[int]string

var lineRef = regexp.MustCompile(`^line (\d+): (.*)$`)

// name rewrites "line N: msg" decode errors as "key (line N): msg".
func (l lineKeys) name(err error) error {
	var te *yaml.TypeError
	if !errors.As(err, &te) {
		return err
	}
	errs := make([]error, len(te.Errors))
	for i, msg := range te.Errors {
		errs[i] = errors.New(msg)
		m := lineRef.FindStringSubmatch(msg)
		if m == nil {
			continue
		}
		var line int
		fmt.Sscan(m[1], &line)
		if key := l[line]; key != "" {
			errs[i] = fmt.Errorf("%s (line %d): %s", key, line, m[2])
		}
	}
	return errors.Join(errs...)
}

// checkKeys reports every mapping key under n that t has no yaml field for,
// and records where each key sits. Free-form maps are not descended into.
func checkKeys(n *yaml.Node, t reflect.Type, path string, lines lineKeys) (errs []error) {
	switch {
	case n.Kind == yaml.MappingNode && t.Kind() == reflect.Struct:
		fields := yamlFields(t)
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			p := joinKey(path, key.Value)
			if _, shared := lines[key.Line]; shared {
				lines[key.Line] = ""
			} else {
				lines[key.Line] = p
			}
			ft, ok := fields[key.Value]
			if !ok {
				errs = append(errs, fmt.Errorf("%s (line %d): unknown key", p, key.Line))
				continue
			}
			errs = append(errs, checkKeys(val, ft, p, lines)...)
		}
	case n.Kind == yaml.SequenceNode && t.Kind() == reflect.Slice:
		for i, item := range n.Content {
			errs = append(errs, checkKeys(item, t.Elem(), fmt.Sprintf("%s[%d]", path, i), lines)...)
		}
	}
	return errs
}

// yamlFields maps yaml names to field types; every config field carries a tag.
func yamlFields(t reflect.Type) map[string]reflect.Type {
	fields := make(map[string]reflect.Type, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		fields[name] = f.Type
	}
	return fields
}

func joinKey(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}
