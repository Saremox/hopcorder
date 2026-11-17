package scripts

// Tests of the `gh attestation verify` commands that release-notes.sh
// prints and README.md shows, against the fake gh and git of fake_test.go.
// No network, no GitHub.

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// cert is what gh checks of an attestation's signing certificate.
type cert struct{ san, repo, ref, digest string }

// accepts tells whether `gh attestation verify` with these flags accepts an
// attestation signed with this certificate, as gh 2.89 decides
// (pkg/cmd/attestation/verify/policy.go and verification/extensions.go,
// sigstore-go 1.1.4's SAN matcher): --cert-identity exactly, case
// included; --signer-workflow as a regex anchored at the start only;
// --repo, --source-ref and --source-digest ignoring case.
func accepts(flags map[string]string, c cert) bool {
	if want, ok := flags["--cert-identity"]; ok && c.san != want {
		return false
	}
	if wf, ok := flags["--signer-workflow"]; ok && !regexp.MustCompile("^https://github.com/"+wf).MatchString(c.san) {
		return false
	}
	if r, ok := flags["--repo"]; ok && !strings.EqualFold("https://github.com/"+r, c.repo) {
		return false
	}
	if r, ok := flags["--source-ref"]; ok && r != "" && !strings.EqualFold(r, c.ref) {
		return false
	}
	if d, ok := flags["--source-digest"]; ok && d != "" && !strings.EqualFold(d, c.digest) {
		return false
	}
	return true
}

// verifyFlags parses `gh attestation verify <subject> <flags>`.
func verifyFlags(t *testing.T, args []string) (subject string, flags map[string]string) {
	t.Helper()
	if len(args) < 3 || args[0] != "attestation" || args[1] != "verify" {
		t.Fatalf("not gh attestation verify: %q", args)
	}
	flags = map[string]string{}
	for i := 2; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--deny-self-hosted-runners":
			flags[a] = "true"
		case strings.HasPrefix(a, "-"):
			k, v, ok := strings.Cut(a, "=")
			if !ok {
				i++
				v = args[i]
			}
			flags[k] = v
		case subject != "":
			t.Fatalf("two subjects in %q", args)
		default:
			subject = a
		}
	}
	return subject, flags
}

// checkVerify checks one gh attestation verify command: it names the
// release workflow and the tag exactly, and the tag's commit, and it
// rejects certificates of a workflow run of anything else.
func checkVerify(t *testing.T, args []string, tagCommit string) {
	t.Helper()
	_, flags := verifyFlags(t, args)
	for _, weak := range []string{"--signer-workflow", "--signer-repo", "--cert-identity-regex", "-i", "--owner", "--cert-oidc-issuer"} {
		if _, ok := flags[weak]; ok {
			t.Errorf("%s in %q", weak, args)
		}
	}
	ref := flags["--source-ref"]
	if !regexp.MustCompile(`^refs/tags/v[0-9A-Za-z.+-]+$`).MatchString(ref) {
		t.Fatalf("--source-ref %q in %q", ref, args)
	}
	workflow := "https://github.com/" + repo + "/.github/workflows/images.yml"
	want := map[string]string{
		"--repo":                     repo,
		"--cert-identity":            workflow + "@" + ref,
		"--source-digest":            tagCommit,
		"--deny-self-hosted-runners": "true",
	}
	for k, v := range want {
		if flags[k] != v {
			t.Errorf("%s is %q, want %q, in %q", k, flags[k], v, args)
		}
	}
	upper := "refs/tags/V" + strings.TrimPrefix(ref, "refs/tags/v")
	certs := []struct {
		cert
		ok bool
	}{
		{cert{workflow + "@" + ref, "https://github.com/" + repo, ref, tagCommit}, true},
		// The same tag name, but V: a tag ruleset for v* may not cover it.
		{cert{workflow + "@" + upper, "https://github.com/" + repo, upper, other}, false},
		// Another workflow file of the tagged commit, whose name starts the same.
		{cert{workflow + ".evil.yml@" + ref, "https://github.com/" + repo, ref, tagCommit}, false},
		{cert{"https://github.com/" + repo + "/.github/workflows/imagesXyml.yml@" + ref, "https://github.com/" + repo, ref, tagCommit}, false},
		{cert{workflow + "@refs/heads/evil", "https://github.com/" + repo, "refs/heads/evil", other}, false},
		{cert{workflow + "@" + ref, "https://github.com/" + repo, ref, other}, false},
		{cert{"https://github.com/mallory/hopcorder/.github/workflows/images.yml@" + ref, "https://github.com/mallory/hopcorder", ref, tagCommit}, false},
	}
	for _, c := range certs {
		if got := accepts(flags, c.cert); got != c.ok {
			t.Errorf("%q: accepts %+v: %v, want %v", args, c.cert, got, c.ok)
		}
	}
}

// The round-three README's flags passed the V certificate: the model of gh
// above does say so.
func TestVerifyModel(t *testing.T) {
	old := map[string]string{"--repo": repo, "--signer-workflow": repo + "/.github/workflows/images.yml",
		"--source-ref": "refs/tags/v0.1.2"}
	evil := cert{"https://github.com/" + repo + "/.github/workflows/images.yml@refs/tags/V0.1.2",
		"https://github.com/" + repo, "refs/tags/V0.1.2", other}
	if !accepts(old, evil) {
		t.Error("the model rejects what gh 2.89 accepts")
	}
}

func TestReleaseNotesVerifyCommands(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip(err)
	}
	agent, server := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	cmd := exec.CommandContext(t.Context(), "bash", "release-notes.sh", "v0.1.2", agent, server)
	cmd.Env = append(os.Environ(), "GITHUB_REPOSITORY="+repo, "GITHUB_SHA="+commit)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("release-notes.sh: %v", err)
	}
	joined := strings.ReplaceAll(string(out), "\\\n", " ")
	var subjects []string
	for _, line := range strings.Split(joined, "\n") {
		if !strings.HasPrefix(line, "gh attestation verify ") {
			continue
		}
		args := strings.Fields(line)[1:]
		subject, _ := verifyFlags(t, args)
		subjects = append(subjects, subject)
		checkVerify(t, args, commit)
	}
	want := []string{"hopcorder_0.1.2_linux_amd64.tar.gz",
		"oci://ghcr.io/saremox/hopcorder-agent@" + agent, "oci://ghcr.io/saremox/hopcorder-server@" + server}
	if !slices.Equal(subjects, want) {
		t.Errorf("verify commands for %q, want %q\n%s", subjects, want, out)
	}
	if !strings.Contains(string(out), "git ls-remote https://github.com/"+repo+" 'refs/tags/v0.1.2^{}'") {
		t.Errorf("the notes do not say where to take the commit from:\n%s", out)
	}
}

// readmeBlocks returns README.md's bash blocks that contain want.
func readmeBlocks(t *testing.T, want string) []string {
	t.Helper()
	b, err := os.ReadFile("../README.md")
	if err != nil {
		t.Fatal(err)
	}
	var blocks []string
	var cur *strings.Builder
	for _, line := range strings.Split(string(b), "\n") {
		switch {
		case cur == nil && line == "```bash":
			cur = &strings.Builder{}
		case cur != nil && line == "```":
			if strings.Contains(cur.String(), want) {
				blocks = append(blocks, cur.String())
			}
			cur = nil
		case cur != nil:
			cur.WriteString(line + "\n")
		}
	}
	if len(blocks) == 0 {
		t.Fatalf("README.md has no bash block with %q", want)
	}
	return blocks
}

// README's commands, run with the fake gh and git: every gh attestation
// verify names the workflow, the tag and the tag's commit exactly, and none
// runs without the commit.
func TestReadmeVerifyCommands(t *testing.T) {
	bin := fakeBin(t)
	for i, block := range readmeBlocks(t, "gh attestation verify") {
		for _, known := range []bool{true, false} {
			log := filepath.Join(t.TempDir(), "gh.log")
			cmd := exec.CommandContext(t.Context(), "bash", "-c", block)
			cmd.Dir = t.TempDir()
			tagCommit := commit
			if !known {
				tagCommit = "" // no such tag
			}
			cmd.Env = append(fakeEnv(bin),
				"FAKEGH_LOG="+log, "FAKEGIT_COMMIT="+tagCommit)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			err := cmd.Run()
			calls, _ := os.ReadFile(log)
			if !known {
				// No such tag: no commit, and so no verify without one.
				if err == nil || len(calls) != 0 {
					t.Errorf("block %d ran %s without the tag's commit (%v)", i, calls, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("block %d: %v\n%s\n%s", i, err, stderr.String(), block)
			}
			n := 0
			for _, line := range strings.Split(strings.TrimSpace(string(calls)), "\n") {
				var args []string
				if err := json.Unmarshal([]byte(line), &args); err != nil {
					t.Fatalf("block %d: %v", i, err)
				}
				checkVerify(t, args, commit)
				n++
			}
			if n < 2 {
				t.Errorf("block %d verifies %d subjects, want a file and an image", i, n)
			}
		}
	}
}

// README's install unpacks a saved and checked archive, without the owners
// stored in it (v0.1.1's are uid 1001, which GNU tar run as root restores).
func TestReadmeInstall(t *testing.T) {
	tarRun := regexp.MustCompile(`(^|[|;&(]\s*)tar\s`)
	piped := regexp.MustCompile(`\|\s*tar\s`)
	for _, block := range readmeBlocks(t, "tar ") {
		checked := false
		for _, line := range strings.Split(block, "\n") {
			if strings.Contains(line, "sha256sum --ignore-missing -c") || strings.HasPrefix(line, "verify ") {
				checked = true
			}
			if !tarRun.MatchString(line) {
				continue
			}
			if piped.MatchString(line) {
				t.Errorf("README pipes a download into tar: %s", line)
			}
			if !strings.Contains(line, "--no-same-owner") {
				t.Errorf("README unpacks without --no-same-owner: %s", line)
			}
			if !checked {
				t.Errorf("README unpacks before checking the archive: %s", line)
			}
		}
	}
}
