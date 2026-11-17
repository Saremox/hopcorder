package scripts

// The fake gh's GitHub: releases, refs and annotated tags, kept in a JSON
// file ($FAKEGH_STATE) between calls. It answers what release-publish.sh
// asks (gh release create, gh api), runs --jq programs through jq, and can
// fail calls (faults) or act as someone else between the script's calls
// (hooks).

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type ghState struct {
	Repo     string
	NextID   int
	Releases []*ghRelease
	// Refs: tag name -> what refs/tags/<name> names; Tags: annotated tag
	// object -> what it names.
	Refs   map[string]ghObject
	Tags   map[string]ghObject
	Faults []*ghFault
	// Hooks: "create" (after gh release create), "publish" (right before
	// a release is published).
	Hooks map[string][]ghHook
	// NoDigest: asset listings that still lack digests.
	NoDigest  int
	Published bool
	Calls     []string
}

type ghObject struct {
	Type string `json:"type"`
	SHA  string `json:"sha"`
}

type ghUser struct {
	Login string `json:"login"`
}

type ghAsset struct {
	Name      string  `json:"name"`
	Digest    *string `json:"digest"`
	State     string  `json:"state"`
	Uploader  ghUser  `json:"uploader"`
	CreatedAt string  `json:"created_at"`
}

type ghRelease struct {
	ID         int       `json:"id"`
	TagName    string    `json:"tag_name"`
	Name       string    `json:"name"`
	Body       string    `json:"body"`
	Draft      bool      `json:"draft"`
	Prerelease bool      `json:"prerelease"`
	Immutable  bool      `json:"immutable"`
	HTMLURL    string    `json:"html_url"`
	Author     ghUser    `json:"author"`
	Assets     []ghAsset `json:"assets"`
}

// ghFault fails API calls whose path contains Path: Times of them (-1:
// all), only once a release was published if AfterPublish.
type ghFault struct {
	Path         string
	Times        int
	AfterPublish bool
}

// ghHook is someone else's doing: add-asset, replace-asset or remove-asset
// (Name, Content) and edit-notes (Content) on the release at hand,
// foreign-release (Draft) of the same tag, move-tag (SHA).
type ghHook struct {
	Do      string
	Name    string
	Content string
	Draft   bool
	SHA     string
}

const bot = "github-actions[bot]"

func digestOf(b []byte) *string {
	sum := sha256.Sum256(b)
	d := "sha256:" + hex.EncodeToString(sum[:])
	return &d
}

func (s *ghState) find(id int) *ghRelease {
	for _, r := range s.Releases {
		if r.ID == id {
			return r
		}
	}
	return nil
}

func (s *ghState) newRelease(tag, title, body string, draft, pre bool, author string) *ghRelease {
	s.NextID++
	r := &ghRelease{ID: s.NextID, TagName: tag, Name: title, Body: body, Draft: true,
		Prerelease: pre, Author: ghUser{author}, Assets: []ghAsset{}}
	r.HTMLURL = fmt.Sprintf("https://github.com/%s/releases/tag/untagged-%d", s.Repo, r.ID)
	s.Releases = append(s.Releases, r)
	if !draft {
		s.publish(r)
	}
	return r
}

func (s *ghState) publish(r *ghRelease) {
	r.Draft, r.Immutable, s.Published = false, true, true
	r.HTMLURL = fmt.Sprintf("https://github.com/%s/releases/tag/%s", s.Repo, r.TagName)
}

// runHooks does, once, what someone else does at that point.
func (s *ghState) runHooks(when string, r *ghRelease) {
	hooks := s.Hooks[when]
	delete(s.Hooks, when)
	for _, h := range hooks {
		switch h.Do {
		case "add-asset":
			r.Assets = append(r.Assets, ghAsset{Name: h.Name, Digest: digestOf([]byte(h.Content)),
				State: "uploaded", Uploader: ghUser{"mallory"}, CreatedAt: "2026-11-17T12:00:30Z"})
		case "replace-asset":
			for i := range r.Assets {
				if r.Assets[i].Name == h.Name {
					r.Assets[i].Digest, r.Assets[i].Uploader = digestOf([]byte(h.Content)), ghUser{"mallory"}
				}
			}
		case "remove-asset":
			kept := []ghAsset{}
			for _, a := range r.Assets {
				if a.Name != h.Name {
					kept = append(kept, a)
				}
			}
			r.Assets = kept
		case "edit-notes":
			r.Body = h.Content
		case "foreign-release":
			f := s.newRelease(r.TagName, "hopcorder "+r.TagName, "trust me", true, false, "mallory")
			f.Assets = []ghAsset{{Name: "hopcorder.tar.gz", Digest: digestOf([]byte("evil")),
				State: "uploaded", Uploader: ghUser{"mallory"}}}
			if !h.Draft {
				s.publish(f)
			}
		case "move-tag":
			s.Refs[r.TagName] = ghObject{"commit", h.SHA}
		default:
			panic("unknown hook " + h.Do)
		}
	}
}

func loadState(path string) (*ghState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s ghState
	return &s, json.Unmarshal(b, &s)
}

func (s *ghState) save(path string) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func githubGH(path string, args []string) int {
	s, err := loadState(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fake gh:", err)
		return 1
	}
	var code int
	switch {
	case len(args) >= 2 && args[0] == "release" && args[1] == "create":
		code = s.releaseCreate(args[2:])
	case len(args) >= 2 && args[0] == "release" && args[1] == "delete":
		code = s.releaseDelete(args[2:])
	case len(args) >= 1 && args[0] == "api":
		code = s.api(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "fake gh: unknown command %q\n", args)
		code = 1
	}
	if err := s.save(path); err != nil {
		fmt.Fprintln(os.Stderr, "fake gh:", err)
		return 1
	}
	return code
}

func (s *ghState) releaseCreate(args []string) int {
	var pos []string
	var title, notes string
	draft, verifyTag, pre := false, false, false
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--draft":
			draft = true
		case "--verify-tag":
			verifyTag = true
		case "--prerelease":
			pre = true
		case "--title":
			i++
			title = args[i]
		case "--notes-file":
			i++
			b, err := os.ReadFile(args[i])
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 1
			}
			notes = string(b)
		default:
			if strings.HasPrefix(args[i], "-") {
				fmt.Fprintf(os.Stderr, "fake gh: unknown flag %s\n", args[i])
				return 1
			}
			pos = append(pos, args[i])
		}
	}
	tag := pos[0]
	s.Calls = append(s.Calls, "release create "+tag)
	if _, ok := s.Refs[tag]; verifyTag && !ok {
		fmt.Fprintf(os.Stderr, "tag %s doesn't exist in the repo %s, aborting due to --verify-tag flag\n", tag, s.Repo)
		return 1
	}
	if !draft { // gh's own check, without --draft
		for _, r := range s.Releases {
			if r.TagName == tag && !r.Draft {
				fmt.Fprintf(os.Stderr, "a release with the same tag name already exists: %s\n", tag)
				return 1
			}
		}
	}
	r := s.newRelease(tag, title, notes, true, pre, bot)
	for _, f := range pos[1:] {
		b, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
		r.Assets = append(r.Assets, ghAsset{Name: filepath.Base(f), Digest: digestOf(b),
			State: "uploaded", Uploader: ghUser{bot}, CreatedAt: "2026-11-17T12:00:00Z"})
	}
	s.runHooks("create", r)
	if !draft {
		s.publish(r)
	}
	fmt.Println(r.HTMLURL)
	return 0
}

// releaseDelete is gh release delete <tag>, which the step that
// release-publish.sh replaced used.
func (s *ghState) releaseDelete(args []string) int {
	s.Calls = append(s.Calls, "release delete "+args[0])
	for i, r := range s.Releases {
		if r.TagName == args[0] {
			s.Releases = append(s.Releases[:i], s.Releases[i+1:]...)
			return 0
		}
	}
	fmt.Fprintln(os.Stderr, "release not found")
	return 1
}

var (
	reReleases = regexp.MustCompile(`^repos/[^/]+/[^/]+/releases$`)
	reRelease  = regexp.MustCompile(`^repos/[^/]+/[^/]+/releases/([0-9]+)$`)
	reAssets   = regexp.MustCompile(`^repos/[^/]+/[^/]+/releases/([0-9]+)/assets$`)
	reRef      = regexp.MustCompile(`^repos/[^/]+/[^/]+/git/ref/tags/(.+)$`)
	reTagObj   = regexp.MustCompile(`^repos/[^/]+/[^/]+/git/tags/([0-9a-f]{40})$`)
)

func httpError(status int, msg string) int {
	fmt.Fprintf(os.Stderr, "gh: %s (HTTP %d)\n", msg, status)
	return 1
}

func (s *ghState) api(args []string) int {
	method, jq, path := "GET", "", ""
	fields := map[string]string{}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--paginate":
		case "--method", "-X":
			i++
			method = args[i]
		case "--jq", "-q":
			i++
			jq = args[i]
		case "-F", "-f", "--field", "--raw-field":
			i++
			k, v, _ := strings.Cut(args[i], "=")
			fields[k] = v
		default:
			if strings.HasPrefix(args[i], "-") {
				fmt.Fprintf(os.Stderr, "fake gh: unknown flag %s\n", args[i])
				return 1
			}
			path = args[i]
		}
	}
	path, _, _ = strings.Cut(path, "?")
	s.Calls = append(s.Calls, method+" "+path)
	for _, f := range s.Faults {
		if strings.Contains(path, f.Path) && f.Times != 0 && (!f.AfterPublish || s.Published) {
			if f.Times > 0 {
				f.Times--
			}
			return httpError(502, "Server Error")
		}
	}
	var out any
	var m []string
	switch {
	case reReleases.MatchString(path) && method == "GET":
		out = s.Releases
	case reAssets.MatchString(path) && method == "GET":
		m = reAssets.FindStringSubmatch(path)
		id, _ := strconv.Atoi(m[1])
		r := s.find(id)
		if r == nil {
			return httpError(404, "Not Found")
		}
		assets := append([]ghAsset(nil), r.Assets...)
		if s.NoDigest > 0 {
			s.NoDigest--
			for i := range assets {
				assets[i].Digest = nil
			}
		}
		out = assets
	case reRelease.MatchString(path):
		m = reRelease.FindStringSubmatch(path)
		id, _ := strconv.Atoi(m[1])
		r := s.find(id)
		if r == nil {
			return httpError(404, "Not Found")
		}
		switch method {
		case "GET":
			out = r
		case "DELETE":
			for i := range s.Releases {
				if s.Releases[i] == r {
					s.Releases = append(s.Releases[:i], s.Releases[i+1:]...)
					break
				}
			}
			return 0
		case "PATCH":
			if fields["draft"] == "false" && r.Draft {
				s.runHooks("publish", r)
				for _, o := range s.Releases {
					if o != r && o.TagName == r.TagName && !o.Draft {
						return httpError(422, "Validation Failed: tag_name already_exists")
					}
				}
				s.publish(r)
			}
			out = r
		default:
			return httpError(405, "Method Not Allowed")
		}
	case reRef.MatchString(path) && method == "GET":
		m = reRef.FindStringSubmatch(path)
		obj, ok := s.Refs[m[1]]
		if !ok {
			return httpError(404, "Not Found")
		}
		out = map[string]any{"ref": "refs/tags/" + m[1], "object": obj}
	case reTagObj.MatchString(path) && method == "GET":
		m = reTagObj.FindStringSubmatch(path)
		obj, ok := s.Tags[m[1]]
		if !ok {
			return httpError(404, "Not Found")
		}
		out = map[string]any{"sha": m[1], "object": obj}
	default:
		return httpError(404, "Not Found")
	}
	b, err := json.Marshal(out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if jq == "" {
		fmt.Printf("%s\n", b)
		return 0
	}
	cmd := exec.Command("jq", "-r", jq)
	cmd.Stdin = strings.NewReader(string(b))
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return 1
	}
	return 0
}
