package config

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// ByteSize is a size in bytes. YAML accepts a plain integer or a number with a
// unit: B, KiB, MiB, GiB, TiB, KB, MB, GB, TB.
type ByteSize int64

type byteUnit struct {
	suffix string
	size   int64
}

// binaryUnits run from largest to smallest; String picks the first that divides.
var binaryUnits = []byteUnit{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}}

// parseUnits lists longer suffixes first, since every unit ends in B.
var parseUnits = slices.Concat(binaryUnits,
	[]byteUnit{{"TB", 1e12}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}})

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
	for _, u := range binaryUnits {
		if int64(b) != 0 && int64(b)%u.size == 0 {
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
	for _, u := range parseUnits {
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
