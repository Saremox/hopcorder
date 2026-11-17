#!/usr/bin/env bash
# Checks that a tag may be released. .github/workflows/images.yml runs it
# before anything of the tag is built (with --wait-for-ci) and again in the
# release environment, right before publishing:
#
# - the tag is v<major>.<minor>.<patch>[-<pre-release>] (so the name is safe
#   as an image tag, a chart version and a file name) and points at the
#   commit this run builds ($GITHUB_SHA), a commit of main;
# - the ci workflow succeeded on that commit, on main or run by hand
#   (--wait-for-ci waits up to 25 minutes for a run still in progress);
# - no release (draft or published) of that tag exists: a version is
#   released once (GitHub lists drafts only to tokens with push access, so
#   verify-tag's run, with contents: read, sees published releases only;
#   publish's run, with contents: write, sees drafts too);
# - the latest release is older: its commit is an ancestor of this one and
#   its version lower, so neither an old commit nor a lower version can take
#   :latest.
#
# Needs a checkout with full history and tags, gh (GH_TOKEN with contents:
# read and actions: read; GH_REPO=owner/name) and jq.
#
#   GITHUB_SHA=<commit> GH_REPO=owner/name scripts/release-verify.sh [--wait-for-ci] v1.2.3
set -euo pipefail
wait_for_ci=false
if [ "${1:-}" = --wait-for-ci ]; then
  wait_for_ci=true
  shift
fi
tag=${1:?usage: release-verify.sh [--wait-for-ci] vX.Y.Z}
: "${GITHUB_SHA:?}" "${GH_REPO:?}"
fail() {
  echo "release-verify: $tag: $*" >&2
  exit 1
}

semver='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$'
[[ $tag =~ $semver ]] || fail "not v<major>.<minor>.<patch>[-<pre-release>]"
version=${tag#v}

commit=$(git rev-parse --verify --quiet "$GITHUB_SHA^{commit}") || fail "commit $GITHUB_SHA not in this checkout"
tagged=$(git rev-parse --verify --quiet "refs/tags/$tag^{commit}") || fail "no such tag in this checkout"
[ "$tagged" = "$commit" ] || fail "points at $tagged, but this run builds $commit (was the tag moved?)"
git merge-base --is-ancestor "$commit" origin/main || fail "$commit is not on main"

# ci on this commit: pushes to main and runs by hand (a pull request's run
# tested a merge commit, not this one).
ci_state() {
  gh api "repos/$GH_REPO/actions/workflows/ci.yml/runs?head_sha=$commit&per_page=100" --jq '
    [.workflow_runs[] | select((.event == "push" and .head_branch == "main") or .event == "workflow_dispatch")]
    | if any(.[]; .conclusion == "success") then "success"
      elif any(.[]; .status != "completed") then "running"
      else "missing" end'
}
tries=1
if $wait_for_ci; then tries=50; fi
for ((i = 1; ; i++)); do
  state=$(ci_state)
  case $state in
  success) break ;;
  running)
    [ "$i" -lt "$tries" ] || fail "ci is still running on $commit"
    echo "release-verify: waiting for ci on $commit"
    sleep 30
    ;;
  *) fail "no successful ci run on $commit (on main, or run by hand)" ;;
  esac
done

releases=$(gh release list --limit 1000 --json tagName,isLatest,isDraft)
# Anyone with write access can create a release of a pushed tag, before
# this workflow does: say what that means.
if jq -e --arg t "$tag" 'any(.[]; .tagName == $t and .isDraft)' <<<"$releases" >/dev/null; then
  fail "a draft release of $tag exists: left by a cancelled run of publish, or someone else's. Look at it (files, notes, who uploaded them); then delete it and run publish again"
fi
if jq -e --arg t "$tag" 'any(.[]; .tagName == $t)' <<<"$releases" >/dev/null; then
  fail "a release of $tag exists already. If no run of this workflow published it, someone else did: look at it (files, notes, author) before anything else, and do not point anyone at it. Release the next version"
fi
latest=$(jq -r '.[] | select(.isLatest) | .tagName' <<<"$releases")
if [ -n "$latest" ]; then
  git merge-base --is-ancestor "refs/tags/$latest" "$commit" 2>/dev/null ||
    fail "$commit does not contain the latest release, $latest"
  core=${version%%-*}
  latest_core=${latest#v}
  latest_core=${latest_core%%-*}
  highest=$(printf '%s\n' "$core" "$latest_core" | sort -V | tail -n 1)
  if [ "$core" = "$latest_core" ] || [ "$highest" != "$core" ]; then
    fail "version $core is not above the latest release, $latest"
  fi
fi
echo "release-verify: $tag ($commit) may be released${latest:+ (latest release: $latest)}"
