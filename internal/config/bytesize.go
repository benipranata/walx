package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// ByteSize is a size in bytes. YAML accepts a plain integer or a number with a
// unit: B, KiB, MiB, GiB, TiB, KB, MB, GB, TB.
type ByteSize int64

// Longer suffixes come first, since every unit but B... ends in B.
var byteUnits = []struct {
	suffix string
	size   int64
}{
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
	{"B", 1},
}

// UnmarshalYAML parses a size such as 256MiB.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	v, err := parseByteSize(n)
	if err != nil {
		return &yaml.TypeError{Errors: []string{fmt.Sprintf("line %d: %v", n.Line, err)}}
	}
	*b = v
	return nil
}

// String formats the size in the largest binary unit that divides it exactly.
func (b ByteSize) String() string {
	for i := 3; i >= 0; i-- {
		if u := byteUnits[i]; int64(b) != 0 && int64(b)%u.size == 0 {
			return fmt.Sprintf("%d%s", int64(b)/u.size, u.suffix)
		}
	}
	return fmt.Sprintf("%dB", int64(b))
}

func parseByteSize(n *yaml.Node) (ByteSize, error) {
	if n.Kind != yaml.ScalarNode {
		return 0, fmt.Errorf("invalid byte size: expected a scalar")
	}
	num, mult := n.Value, int64(1)
	for _, u := range byteUnits {
		if strings.HasSuffix(num, u.suffix) {
			num, mult = strings.TrimSuffix(num, u.suffix), u.size
			break
		}
	}
	v, err := strconv.ParseInt(num, 10, 64)
	if err != nil || v < 0 || v > math.MaxInt64/mult {
		return 0, fmt.Errorf("invalid byte size %q (want e.g. 256MiB)", n.Value)
	}
	return ByteSize(v * mult), nil
}
