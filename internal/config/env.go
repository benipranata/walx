package config

import (
	"fmt"
	"os"
	"regexp"

	"go.yaml.in/yaml/v3"
)

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv replaces ${NAME} in every scalar value under n. Keys and comments
// are left alone. A variable that is unset is an error; an empty one is not.
func expandEnv(n *yaml.Node, path string) (errs []error) {
	switch n.Kind {
	case yaml.ScalarNode:
		n.Value = envRef.ReplaceAllStringFunc(n.Value, func(ref string) string {
			name := envRef.FindStringSubmatch(ref)[1]
			v, ok := os.LookupEnv(name)
			if !ok {
				errs = append(errs, fmt.Errorf("%s (line %d): environment variable %s is not set", path, n.Line, name))
			}
			return v
		})
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			errs = append(errs, expandEnv(n.Content[i+1], joinKey(path, n.Content[i].Value))...)
		}
	case yaml.SequenceNode:
		for i, item := range n.Content {
			errs = append(errs, expandEnv(item, fmt.Sprintf("%s[%d]", path, i))...)
		}
	}
	return errs
}
