package scripts

// Tests of release-publish.sh: the release it publishes has exactly this
// run's files, for the commit the tag names, whatever someone else does to
// the release meanwhile; against the fake gh of fake_test.go. No network,
// no GitHub.

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// Annotated tag objects.
const (
	tagObj = "7a6a7a6a7a6a7a6a7a6a7a6a7a6a7a6a7a6a7a6a"
	rcObj  = "7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"
)

var distFiles = map[string]string{
	"hopcorder_0.2.0_linux_amd64.tar.gz": "archive",
	"hopcorder-agent-0.2.0.tgz":          "chart",
	"SHA256SUMS":                         "sums",
}

type publishRun struct {
	out   string
	err   error
	state *ghState
}

func runPublish(t *testing.T, tag string, setup func(*ghState)) publishRun {
	t.Helper()
	bin := fakeBin(t)
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "dist"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range distFiles {
		if err := os.WriteFile(filepath.Join(dir, "dist", name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	notes := "Fixes.\n\n**Verify**\n\n- the digests\n"
	if err := os.WriteFile(filepath.Join(dir, "notes.md"), []byte(notes), 0o644); err != nil {
		t.Fatal(err)
	}
	s := &ghState{Repo: repo, Refs: map[string]ghObject{tag: {"commit", commit}},
		Tags: map[string]ghObject{}, Hooks: map[string][]ghHook{}}
	setup(s)
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := s.save(statePath); err != nil {
		t.Fatal(err)
	}
	script, err := filepath.Abs("release-publish.sh")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "bash", script, tag)
	cmd.Dir = dir
	cmd.Env = append(fakeEnv(bin),
		"FAKEGH_STATE="+statePath, "GITHUB_REPOSITORY="+repo, "GITHUB_SHA="+commit,
		"RELEASE_PUBLISH_TRIES=3", "RELEASE_PUBLISH_PAUSE=0")
	out, err := cmd.CombinedOutput()
	t.Logf("release-publish.sh %s:\n%s", tag, out)
	final, lerr := loadState(statePath)
	if lerr != nil {
		t.Fatal(lerr)
	}
	return publishRun{string(out), err, final}
}

// published checks that exactly one release of the tag is left, published,
// with exactly the files of dist/.
func (r publishRun) published(t *testing.T, tag string) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("release-publish.sh failed: %v\n%s", r.err, r.out)
	}
	if len(r.state.Releases) != 1 {
		t.Fatalf("%d releases, want 1\n%s", len(r.state.Releases), r.out)
	}
	rel := r.state.Releases[0]
	if rel.Draft || rel.TagName != tag || rel.Author.Login != bot {
		t.Fatalf("release %+v, want a published release of %s by %s", rel, tag, bot)
	}
	var names []string
	for _, a := range rel.Assets {
		if want := digestOf([]byte(distFiles[a.Name])); *a.Digest != *want {
			t.Errorf("asset %s: digest %s, want %s", a.Name, *a.Digest, *want)
		}
		names = append(names, a.Name)
	}
	if len(names) != len(distFiles) {
		t.Errorf("assets %v, want those of dist/", names)
	}
}

// failed checks that the script failed saying want, and that the releases
// left are by left (author logins; a published one is marked with a "!").
func (r publishRun) failed(t *testing.T, want string, left ...string) {
	t.Helper()
	if r.err == nil {
		t.Fatalf("release-publish.sh succeeded, want a failure\n%s", r.out)
	}
	if !strings.Contains(r.out, want) {
		t.Errorf("output lacks %q:\n%s", want, r.out)
	}
	var got []string
	for _, rel := range r.state.Releases {
		who := rel.Author.Login
		if !rel.Draft {
			who += "!"
		}
		got = append(got, who)
	}
	if !slices.Equal(got, left) {
		t.Errorf("releases left by %v, want %v\n%s", got, left, r.out)
	}
}

func TestReleasePublish(t *testing.T) {
	const tag = "v0.2.0"
	none := func(*ghState) {}
	t.Run("lightweight tag", func(t *testing.T) {
		runPublish(t, tag, none).published(t, tag)
	})
	t.Run("annotated tag", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Refs[tag] = ghObject{"tag", tagObj}
			s.Tags[tagObj] = ghObject{"commit", commit}
		}).published(t, tag)
	})
	// git tag -a v0.2.0 v0.2.0-rc.1: a tag of a tag.
	t.Run("annotated tag of an annotated tag", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Refs[tag] = ghObject{"tag", tagObj}
			s.Tags[tagObj] = ghObject{"tag", rcObj}
			s.Tags[rcObj] = ghObject{"commit", commit}
		}).published(t, tag)
	})
	t.Run("pre-release", func(t *testing.T) {
		r := runPublish(t, "v0.2.0-rc.1", none)
		r.published(t, "v0.2.0-rc.1")
		if !r.state.Releases[0].Prerelease {
			t.Error("not a pre-release")
		}
	})
	t.Run("digests computed late", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) { s.NoDigest = 2 }).published(t, tag)
	})
	t.Run("tag API errors, then answers", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Refs[tag] = ghObject{"tag", tagObj}
			s.Tags[tagObj] = ghObject{"commit", commit}
			s.Faults = []*ghFault{{Path: "/git/ref/tags/", Times: 2}, {Path: "/git/tags/", Times: 2, AfterPublish: true}}
		}).published(t, tag)
	})

	t.Run("tag API keeps failing before publishing", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Faults = []*ghFault{{Path: "/git/ref/tags/", Times: -1}}
		}).failed(t, "cannot check what v0.2.0 names")
	})
	// Not a confirmed difference: the release stays, to be checked by hand.
	t.Run("tag API keeps failing after publishing", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Faults = []*ghFault{{Path: "/git/ref/tags/", Times: -1, AfterPublish: true}}
		}).failed(t, "check by hand that it names "+commit, bot+"!")
	})
	t.Run("tag moved before publishing", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["create"] = []ghHook{{Do: "move-tag", SHA: other}}
		}).failed(t, "the tag moved after this run checked it")
	})
	t.Run("tag moved while publishing", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["publish"] = []ghHook{{Do: "move-tag", SHA: other}}
		}).failed(t, "the tag now names commit "+other)
	})

	// Someone else's release of the tag, or files in this run's draft.
	t.Run("a release of the tag exists already", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["create"] = []ghHook{{Do: "foreign-release"}}
		}).failed(t, "someone else created a release of v0.2.0", "mallory!")
	})
	t.Run("a draft of the tag appears", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["create"] = []ghHook{{Do: "foreign-release", Draft: true}}
		}).failed(t, "someone else created a release of v0.2.0", "mallory")
	})
	t.Run("a file added to the draft", func(t *testing.T) {
		r := runPublish(t, tag, func(s *ghState) {
			s.Hooks["create"] = []ghHook{{Do: "add-asset", Name: "hopcorder_0.2.0_linux_riscv64.tar.gz", Content: "evil"}}
		})
		r.failed(t, "someone else changed the draft release")
		if !strings.Contains(r.out, "+ "+*digestOf([]byte("evil"))+" uploaded hopcorder_0.2.0_linux_riscv64.tar.gz") ||
			!strings.Contains(r.out, "uploaded by mallory") {
			t.Errorf("output does not show the added file and who uploaded it:\n%s", r.out)
		}
	})
	t.Run("a file of the draft replaced", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["create"] = []ghHook{{Do: "replace-asset", Name: "SHA256SUMS", Content: "evil"}}
		}).failed(t, "someone else changed the draft release")
	})
	t.Run("a file of the draft removed", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["create"] = []ghHook{{Do: "remove-asset", Name: "hopcorder-agent-0.2.0.tgz"}}
		}).failed(t, "- "+*digestOf([]byte("chart"))+" uploaded hopcorder-agent-0.2.0.tgz")
	})
	t.Run("the draft's notes changed", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["create"] = []ghHook{{Do: "edit-notes", Content: "gh attestation verify --signer-workflow …"}}
		}).failed(t, "the release's notes are not notes.md")
	})
	t.Run("a file added while publishing", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["publish"] = []ghHook{{Do: "add-asset", Name: "extra", Content: "evil"}}
		}).failed(t, "the published release is not what this run built")
	})
	t.Run("someone else's release published first", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) {
			s.Hooks["publish"] = []ghHook{{Do: "foreign-release"}}
		}).failed(t, "could not publish the draft release", "mallory!")
	})
	t.Run("no such tag", func(t *testing.T) {
		runPublish(t, tag, func(s *ghState) { delete(s.Refs, tag) }).failed(t, "creating the draft release failed")
	})
}
