package scripts

// A fake gh and git for the release scripts' tests: this test binary, run
// under the name gh or git (symlinks in a directory put first on PATH).
// gh only records its arguments ($FAKEGH_LOG), for the README's commands.

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	switch filepath.Base(os.Args[0]) {
	case "gh":
		os.Exit(fakeGH(os.Args[1:]))
	case "git":
		os.Exit(fakeGit(os.Args[1:]))
	case "curl", "wget":
		fmt.Fprintln(os.Stderr, "fake: no network in tests")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// fakeBin returns a directory with gh, git and curl that are this binary.
func fakeBin(t *testing.T) string {
	t.Helper()
	for _, tool := range []string{"bash", "jq", "sha256sum", "awk"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("needs %s: %v", tool, err)
		}
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, name := range []string{"gh", "git", "curl", "wget"} {
		if err := os.Symlink(self, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// fakeEnv is the environment for a script that runs the fakes in bin: first
// on PATH, and, built with -race, without the race detector's second of
// sleep at every exit.
func fakeEnv(bin string) []string {
	return append(os.Environ(), "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
}

// The repository, the commit a run builds, and another.
const (
	repo   = "Saremox/hopcorder"
	commit = "c0ffeec0ffeec0ffeec0ffeec0ffeec0ffeec0ff"
	other  = "badc0debadc0debadc0debadc0debadc0debadc0"
)

func fakeGH(args []string) int {
	// Record the call (one JSON array per line) and succeed.
	f, err := os.OpenFile(os.Getenv("FAKEGH_LOG"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fake gh:", err)
		return 1
	}
	defer f.Close()
	b, _ := json.Marshal(args)
	fmt.Fprintf(f, "%s\n", b)
	return 0
}

// fakeGit answers `git ls-remote <url> refs/tags/<tag>^{}` like GitHub
// would for a repository where <tag> is an annotated tag of $FAKEGIT_COMMIT
// (no such tag without it), next to a decoy tag a/refs/tags/<tag> of
// another commit: ls-remote matches patterns against the end of ref names.
func fakeGit(args []string) int {
	m := regexp.MustCompile(`^refs/tags/(.+)\^\{\}$`).FindStringSubmatch(args[len(args)-1])
	if len(args) != 3 || args[0] != "ls-remote" || m == nil {
		fmt.Fprintf(os.Stderr, "fake git: unexpected %q\n", args)
		return 1
	}
	tag, commit := m[1], os.Getenv("FAKEGIT_COMMIT")
	if commit == "" {
		return 0
	}
	refs := []string{
		"refs/tags/a/refs/tags/" + tag, "1111111111111111111111111111111111111111",
		"refs/tags/a/refs/tags/" + tag + "^{}", "2222222222222222222222222222222222222222",
		"refs/tags/" + tag, "3333333333333333333333333333333333333333",
		"refs/tags/" + tag + "^{}", commit,
	}
	for i := 0; i < len(refs); i += 2 {
		if ref := refs[i]; ref == args[2] || strings.HasSuffix(ref, "/"+args[2]) {
			fmt.Printf("%s\t%s\n", refs[i+1], ref)
		}
	}
	return 0
}
