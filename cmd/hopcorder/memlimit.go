package main

import (
	"bufio"
	"io/fs"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
)

// memLimitShare is the part of the cgroup's memory limit the Go runtime
// aims to stay below; the rest is for stacks, the binary and the kernel.
const memLimitShare = 0.9

// setMemoryLimit sets the Go runtime's soft memory limit to 90% of the
// cgroup memory limit the process runs under, unless GOMEMLIMIT is set or
// there is no limit. Without it the heap grows to twice what is live before
// a collection, so a server with 600 MiB of live state gets OOM-killed in a
// 1 GiB container. It returns the limit set, or 0.
func setMemoryLimit(fsys fs.FS) int64 {
	if os.Getenv("GOMEMLIMIT") != "" {
		return 0
	}
	limit := cgroupMemoryLimit(fsys)
	if limit <= 0 {
		return 0
	}
	soft := int64(float64(limit) * memLimitShare)
	debug.SetMemoryLimit(soft)
	return soft
}

// cgroupMemoryLimit returns the memory limit of the process's cgroup in
// bytes, or 0 if there is none or it cannot be read. fsys is the root file
// system ("/" in production). With cgroup v2 every level from the process's
// cgroup up to the root counts (systemd puts MemoryMax= on the unit's
// cgroup, a container's limit is on its own); cgroup v1 has one file.
func cgroupMemoryLimit(fsys fs.FS) int64 {
	var limit int64
	lower := func(v int64) {
		if v > 0 && (limit == 0 || v < limit) {
			limit = v
		}
	}
	if dir, ok := cgroupV2Path(fsys); ok {
		for d := dir; ; d = path.Dir(d) {
			lower(readLimit(fsys, path.Join("sys/fs/cgroup", d, "memory.max")))
			if d == "/" || d == "." {
				break
			}
		}
	}
	lower(readLimit(fsys, "sys/fs/cgroup/memory.max"))
	lower(readLimit(fsys, "sys/fs/cgroup/memory/memory.limit_in_bytes"))
	return limit
}

// cgroupV2Path returns the process's cgroup v2 path ("0::/path" in
// /proc/self/cgroup).
func cgroupV2Path(fsys fs.FS) (string, bool) {
	f, err := fsys.Open("proc/self/cgroup")
	if err != nil {
		return "", false
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if p, ok := strings.CutPrefix(sc.Text(), "0::"); ok && strings.HasPrefix(p, "/") {
			return path.Clean(p), true
		}
	}
	return "", false
}

// readLimit reads a cgroup memory limit file: a number of bytes, or "max"
// (v2) or a huge number (v1) for none.
func readLimit(fsys fs.FS, name string) int64 {
	b, err := fs.ReadFile(fsys, name)
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil || v <= 0 || v >= 1<<60 {
		return 0
	}
	return v
}
