#!/usr/bin/env bash
# Creates and publishes the GitHub release of a tag for the publish job of
# .github/workflows/images.yml, from dist/ and notes.md in the current
# directory, and checks that what is published is exactly what this run
# built. Anyone with write access can create a release of a pushed tag, and
# change a draft, until a release is published (then immutable releases
# keep it); GitHub's own release attestation, and so
# `gh release verify-asset`, vouches for whatever files the release has.
#
# 1. The release is created as a draft with every file of dist/, and found
#    again by its id: it must be the only release of the tag, draft or not.
# 2. Its files are compared with dist/ by name and SHA-256 (GitHub's digest
#    of each asset): none missing, none different, none added. Its title,
#    notes and pre-release flag must be this run's too.
# 3. The tag must still name the commit this run built ($GITHUB_SHA),
#    through any number of annotated tags (a tag of a tag, as from
#    `git tag -a v1.0.0 v1.0.0-rc.1`, has two).
# 4. The draft is published by its id (`gh release edit <tag>` would pick
#    whichever release of the tag GitHub answers with first), and 2 and 3
#    are checked again: a change made just before the release was
#    published. After that, with immutable releases, neither can change.
#
# Before publishing, anything wrong deletes this run's draft and fails (a
# release someone else created stays, for whoever looks into it). After
# publishing, a difference that GitHub confirms deletes the release (the tag
# stays) and fails; when GitHub cannot be asked (API errors, retried), it
# fails without deleting and says what to check by hand.
#
# Needs gh (GH_TOKEN with contents: write, which also lists drafts; GH_REPO)
# and jq.
#
#   GITHUB_REPOSITORY=owner/name GITHUB_SHA=<commit> scripts/release-publish.sh vX.Y.Z
set -euo pipefail
tag=${1:?usage: release-publish.sh vX.Y.Z}
: "${GITHUB_REPOSITORY:?}" "${GITHUB_SHA:?}"
repo=$GITHUB_REPOSITORY
# Tries of each GitHub API call, and the pause between two.
tries=${RELEASE_PUBLISH_TRIES:-5}
pause=${RELEASE_PUBLISH_PAUSE:-10}

error() {
  echo "::error::release-publish: $tag: $*"
}
# The tag goes into jq programs and API paths.
semver='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$'
if [[ ! $tag =~ $semver ]]; then
  error "not v<major>.<minor>.<patch>[-<pre-release>]"
  exit 1
fi
if [[ ! $GITHUB_SHA =~ ^[0-9a-f]{40}$ ]]; then
  error "not a commit: '$GITHUB_SHA'"
  exit 1
fi
if [ ! -s notes.md ] || ! compgen -G 'dist/*' >/dev/null; then
  error "needs dist/ and notes.md in $PWD"
  exit 1
fi
title="hopcorder $tag"
prerelease=false
case $tag in *-*) prerelease=true ;; esac

# gh api, tried again on failure: GitHub's API fails now and then.
api() {
  local i out
  for ((i = 1; ; i++)); do
    if out=$(gh api "$@"); then
      printf '%s\n' "$out"
      return 0
    fi
    if [ "$i" -ge "$tries" ]; then
      echo "release-publish: gh api $*: failed $tries times" >&2
      return 1
    fi
    echo "release-publish: gh api $*: failed, trying again in ${pause}s" >&2
    sleep "$pause"
  done
}

# Every release of the tag, drafts included: "<id> <draft> <html_url> <author>".
releases_of_tag() {
  api --paginate "repos/$repo/releases?per_page=100" \
    --jq ".[] | select(.tag_name == \"$tag\") | \"\(.id) \(.draft) \(.html_url) \(.author.login)\""
}

# What this run built: "<sha256:hex> uploaded <name>", sorted.
want=$(
  cd dist
  for f in *; do
    sum=$(sha256sum -- "$f")
    echo "sha256:${sum%% *} uploaded $f"
  done | LC_ALL=C sort
)

# A release's files, like $want. GitHub computes an asset's digest after
# the upload, so an asset without one yet is asked about again.
assets_of() {
  local i got
  for ((i = 1; ; i++)); do
    got=$(api --paginate "repos/$1/releases/$2/assets?per_page=100" \
      --jq '.[] | "\(.digest // "no-digest") \(.state) \(.name)"' | LC_ALL=C sort) || return 1
    if ! grep -q '^no-digest ' <<<"$got" || [ "$i" -ge "$tries" ]; then
      printf '%s\n' "$got"
      return 0
    fi
    sleep "$pause"
  done
}

# Checks release $1's files and fields (draft: $2). Status 0: as built; 1: a
# difference (printed); 2: GitHub could not be asked.
check_release() {
  local id=$1 draft=$2 got json fields body
  got=$(assets_of "$repo" "$id") || return 2
  json=$(api "repos/$repo/releases/$id") || return 2
  fields=$(jq -r '[.tag_name, .name, (.draft | tostring), (.prerelease | tostring)] | join(" ")' <<<"$json") || return 2
  body=$(jq -r .body <<<"$json") || return 2
  local bad=0
  if [ "$got" != "$want" ]; then
    error "the release's files are not the ones this run built (+ the release has, - this run built):"
    diff <(printf '%s\n' "$want") <(printf '%s\n' "$got") | sed -n 's/^< /- /p; s/^> /+ /p'
    api --paginate "repos/$repo/releases/$id/assets?per_page=100" \
      --jq '.[] | "  \(.name): uploaded by \(.uploader.login) at \(.created_at)"' || true
    bad=1
  fi
  if [ "$fields" != "$tag $title $draft $prerelease" ]; then
    error "the release is '$fields', not '$tag $title $draft $prerelease' (tag, title, draft, pre-release)"
    bad=1
  fi
  if [ "$body" != "$(<notes.md)" ]; then
    error "the release's notes are not notes.md:"
    diff <(cat notes.md) <(printf '%s\n' "$body") || true
    bad=1
  fi
  return "$bad"
}

# The object the tag names now, through annotated tags (at most 10):
# "commit <sha>" normally. Status 1: GitHub could not tell.
tag_target() {
  local obj i
  obj=$(api "repos/$repo/git/ref/tags/$tag" --jq '.object.type + " " + .object.sha') || return 1
  for ((i = 0; ; i++)); do
    if [[ ! $obj =~ ^(commit|tag|tree|blob)\ [0-9a-f]{40}$ ]]; then
      echo "release-publish: GitHub says $tag names '$obj'" >&2
      return 1
    fi
    [ "${obj%% *}" = tag ] || break
    if [ "$i" -ge 10 ]; then
      echo "release-publish: $tag is an annotated tag more than 10 levels deep" >&2
      return 1
    fi
    obj=$(api "repos/$repo/git/tags/${obj#* }" --jq '.object.type + " " + .object.sha') || return 1
  done
  echo "$obj"
}

# Status 0: the tag names $GITHUB_SHA; 1: something else (printed); 2:
# GitHub could not tell.
check_tag() {
  local target
  target=$(tag_target) || return 2
  [ "$target" = "commit $GITHUB_SHA" ] && return 0
  error "the tag now names $target, not commit $GITHUB_SHA, which this run built"
  return 1
}

delete_release() {
  if ! gh api --method DELETE "repos/$repo/releases/$1" >/dev/null; then
    error "could not delete release $1: delete it by hand"
  fi
}

# Before publishing: delete this run's draft and fail.
abandon() {
  error "$*"
  echo "release-publish: deleting this run's draft release $id; nothing was published"
  delete_release "$id"
  exit 1
}

# 1. The draft, with every file (gh uploads them, then prints its URL).
pre=()
if $prerelease; then pre=(--prerelease); fi
if ! url=$(gh release create "$tag" dist/* --draft --verify-tag "${pre[@]}" \
  --title "$title" --notes-file notes.md); then
  error "creating the draft release failed. Releases of $tag now (id, draft, URL, author):"
  releases_of_tag || true
  echo "If one of them is not this run's, someone else created it: look at it before anything else. A draft of this run's, if any, is left for you to delete."
  exit 1
fi
url=$(tail -n 1 <<<"$url")
if ! releases=$(releases_of_tag); then
  error "cannot list the releases of $tag: the draft gh created ($url) is left for you to delete"
  exit 1
fi
id=$(awk -v u="$url" '$3 == u {print $1}' <<<"$releases")
if [[ ! $id =~ ^[0-9]+$ ]]; then
  error "cannot find the draft gh created ($url) among the releases of $tag: delete it by hand. Releases of $tag (id, draft, URL, author):"
  printf '%s\n' "$releases"
  exit 1
fi
if [ "$(wc -l <<<"$releases")" != 1 ]; then
  echo "Releases of $tag (id, draft, URL, author):"
  printf '%s\n' "$releases"
  abandon "someone else created a release of $tag while this job ran (the others above): look at it before anything else"
fi

# 2. and 3., on the draft.
rc=0
check_release "$id" true || rc=$?
case $rc in
0) ;;
1) abandon "someone else changed the draft release while this job ran (above): look into it before anything else" ;;
*) abandon "cannot check the draft release" ;;
esac
rc=0
check_tag || rc=$?
case $rc in
0) ;;
1) abandon "the tag moved after this run checked it" ;;
*) abandon "cannot check what $tag names" ;;
esac

# 4. Publish it, by id. If that fails, the release says whether it was
# published all the same (an answer lost on the way).
for ((i = 1; ; i++)); do
  if gh api --method PATCH "repos/$repo/releases/$id" -F draft=false >/dev/null; then
    break
  fi
  if ! draft=$(api "repos/$repo/releases/$id" --jq .draft); then
    error "publishing release $id failed, and GitHub cannot say whether it is published: check it by hand"
    exit 1
  fi
  if [ "$draft" = false ]; then
    break
  fi
  if [ "$i" -ge "$tries" ]; then
    echo "Releases of $tag (id, draft, URL, author):"
    releases_of_tag || true
    abandon "could not publish the draft release (if another release of $tag was published meanwhile, someone else did: look at it)"
  fi
  sleep "$pause"
done
echo "release-publish: published release $id of $tag"

# Again, now that it is published. Delete it only on a confirmed difference.
rc=0
check_release "$id" false || rc=$?
case $rc in
0) ;;
1)
  error "the published release is not what this run built (above): deleting release $id of $tag (the tag stays)"
  delete_release "$id"
  exit 1
  ;;
*)
  error "cannot check the published release $id; check by hand that its files match SHA256SUMS of this run's artifact, and delete it if not"
  exit 1
  ;;
esac
rc=0
check_tag || rc=$?
case $rc in
0) ;;
1)
  error "deleting release $id of $tag again (the tag stays): its images, charts and files are commit $GITHUB_SHA's"
  delete_release "$id"
  exit 1
  ;;
*)
  error "cannot check what $tag names now; check by hand that it names $GITHUB_SHA (git ls-remote origin 'refs/tags/$tag^{}'), and delete release $id if not"
  exit 1
  ;;
esac
if others=$(releases_of_tag) && [ "$(wc -l <<<"$others")" != 1 ]; then
  echo "::warning::release-publish: $tag: other releases of the tag exist (drafts someone else created): look at them and delete them"
  printf '%s\n' "$others"
fi
if [ "$(api "repos/$repo/releases/$id" --jq .immutable || true)" != true ]; then
  echo "::warning::release-publish: $tag: GitHub does not report release $id as immutable: are immutable releases still on (repository settings)?"
fi
echo "release-publish: $tag: release $id has exactly this run's files, for commit $GITHUB_SHA"
