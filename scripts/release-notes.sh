#!/usr/bin/env bash
# Prints the "Verify" section that the publish job of
# .github/workflows/images.yml appends to a release's notes (the tag's
# section of CHANGELOG.md): the commit, the images by digest, and how to
# check the build provenance attestations publish created for the
# release files and both images. The digests here are a convenience; the
# attestations are what proves them, checked against the tag's commit.
#
#   GITHUB_REPOSITORY=owner/name GITHUB_SHA=<commit> \
#     scripts/release-notes.sh vX.Y.Z <agent image digest> <server image digest>
set -euo pipefail
usage="usage: release-notes.sh vX.Y.Z <agent image digest> <server image digest>"
tag=${1:?$usage}
agent=${2:?$usage}
server=${3:?$usage}
: "${GITHUB_REPOSITORY:?}" "${GITHUB_SHA:?}"
fail() {
  echo "release-notes: $*" >&2
  exit 1
}
for d in "$agent" "$server"; do
  [[ $d =~ ^sha256:[0-9a-f]{64}$ ]] || fail "not an image digest: '$d'"
done
[[ $GITHUB_SHA =~ ^[0-9a-f]{40}$ ]] || fail "not a commit: '$GITHUB_SHA'"
[[ $tag =~ ^v[0-9A-Za-z.+-]+$ ]] || fail "not a release tag: '$tag'"
version=${tag#v}
repo=$GITHUB_REPOSITORY
# gh (2.89) compares --cert-identity exactly, case included. --signer-workflow
# is a regex anchored at the start only (images.yml.x.yml matches), and
# --source-ref and --source-digest ignore case (refs/tags/V1.0.0 matches):
# so the workflow file and the tag are named in --cert-identity, and
# --source-digest names the commit. scripts/verify_test.go checks these.
flags="\\
  --repo $repo --cert-identity https://github.com/$repo/.github/workflows/images.yml@refs/tags/$tag \\
  --source-ref refs/tags/$tag --source-digest $GITHUB_SHA --deny-self-hosted-runners"

cat <<EOF

**Verify**

Built from commit \`$GITHUB_SHA\` by the \`images\` workflow for \`$tag\`.
The images, by digest (multi-arch indexes):

- \`ghcr.io/saremox/hopcorder-agent:$version@$agent\`
- \`ghcr.io/saremox/hopcorder-server:$version@$server\`

Every file below and both images have a signed build provenance attestation
from that run. Check one with the GitHub CLI before you use it. Take the
commit from the tag rather than from these notes
(\`git ls-remote https://github.com/$repo 'refs/tags/$tag^{}'\`; README.md
has the same commands):

\`\`\`bash
gh attestation verify hopcorder_${version}_linux_amd64.tar.gz $flags
gh attestation verify oci://ghcr.io/saremox/hopcorder-agent@$agent $flags
gh attestation verify oci://ghcr.io/saremox/hopcorder-server@$server $flags
\`\`\`

Charts: verify \`hopcorder-server-$version.tgz\` or \`hopcorder-agent-$version.tgz\`
below the same way and install that file. \`--set image.tag=$version@<digest>\`
pins a chart's image to the digest above.
EOF
