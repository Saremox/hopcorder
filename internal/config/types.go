package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Duration is a time.Duration that unmarshals from YAML strings like "5s".
type Duration time.Duration

// D returns d as a time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// UnmarshalYAML implements yaml.Unmarshaler.
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("line %d: duration must be a string like \"5s\"", n.Line)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q: %w", n.Line, s, err)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// ByteSize is a size in bytes that unmarshals from "1GiB", "512MiB", "10MB"
// or a plain integer.
type ByteSize int64

// UnmarshalYAML implements yaml.Unmarshaler.
func (b *ByteSize) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("line %d: size must be a string like \"1GiB\"", n.Line)
	}
	v, err := ParseByteSize(s)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*b = v
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (b ByteSize) MarshalYAML() (any, error) { return strconv.FormatInt(int64(b), 10), nil }

var byteUnits = []struct {
	suffix string
	mult   int64
}{
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
	{"B", 1},
}

// ParseByteSize parses sizes such as "1GiB", "500MB" or "1048576".
func ParseByteSize(s string) (ByteSize, error) {
	t := strings.TrimSpace(s)
	mult := int64(1)
	for _, u := range byteUnits {
		if strings.HasSuffix(t, u.suffix) {
			t, mult = strings.TrimSpace(strings.TrimSuffix(t, u.suffix)), u.mult
			break
		}
	}
	v, err := strconv.ParseInt(t, 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("invalid size %q (use e.g. \"1GiB\", \"512MiB\")", s)
	}
	if v > 0 && mult > (1<<62)/v {
		return 0, fmt.Errorf("size %q too large", s)
	}
	return ByteSize(v * mult), nil
}
