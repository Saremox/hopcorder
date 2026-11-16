package main

import (
	"testing"
	"testing/fstest"
)

func TestCgroupMemoryLimit(t *testing.T) {
	for _, tt := range []struct {
		name string
		fs   fstest.MapFS
		want int64
	}{
		{"none", fstest.MapFS{}, 0},
		{"v2 container", fstest.MapFS{
			"proc/self/cgroup":         {Data: []byte("0::/\n")},
			"sys/fs/cgroup/memory.max": {Data: []byte("1073741824\n")},
		}, 1 << 30},
		{"v2 unlimited", fstest.MapFS{
			"proc/self/cgroup":         {Data: []byte("0::/\n")},
			"sys/fs/cgroup/memory.max": {Data: []byte("max\n")},
		}, 0},
		{"v2 systemd unit under a smaller slice", fstest.MapFS{
			"proc/self/cgroup": {Data: []byte("0::/system.slice/hopcorder-server.service\n")},
			"sys/fs/cgroup/system.slice/hopcorder-server.service/memory.max": {Data: []byte("2147483648\n")},
			"sys/fs/cgroup/system.slice/memory.max":                          {Data: []byte("1073741824\n")},
		}, 1 << 30},
		{"v1", fstest.MapFS{
			"proc/self/cgroup":                           {Data: []byte("12:memory:/docker/abc\n")},
			"sys/fs/cgroup/memory/memory.limit_in_bytes": {Data: []byte("536870912\n")},
		}, 512 << 20},
		{"v1 unlimited", fstest.MapFS{
			"sys/fs/cgroup/memory/memory.limit_in_bytes": {Data: []byte("9223372036854771712\n")},
		}, 0},
	} {
		if got := cgroupMemoryLimit(tt.fs); got != tt.want {
			t.Errorf("%s: %d, want %d", tt.name, got, tt.want)
		}
	}
}

func TestSetMemoryLimitRespectsGOMEMLIMIT(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "100MiB")
	fs := fstest.MapFS{"sys/fs/cgroup/memory.max": {Data: []byte("1073741824\n")}}
	if got := setMemoryLimit(fs); got != 0 {
		t.Errorf("GOMEMLIMIT set, but the limit was changed to %d", got)
	}
}
