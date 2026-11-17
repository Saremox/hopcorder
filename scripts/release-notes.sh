#!/usr/bin/env bash
# Prints the "Verify" section that the publish job of
# .github/workflows/images.yml appends to a release's notes (the tag's
# section of CHANGELOG.md): the commit, the images by digest, and how to
# check the build provenance attestations publish created for the release
# files and both images. The digests here are a convenience; the
# attestations are what proves them.
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
flags="\\
  --repo $repo --signer-workflow $repo/.github/workflows/images.yml \\
  --source-ref refs/tags/$tag --source-digest $GITHUB_SHA --deny-self-hosted-runners"

cat <<EOF

**Verify**

Built from commit \`$GITHUB_SHA\` by the \`images\` workflow for \`$tag\`.
The images, by digest (multi-arch indexes):

- \`ghcr.io/saremox/hopcorder-agent:$version@$agent\`
- \`ghcr.io/saremox/hopcorder-server:$version@$server\`

Every file below and both images have a signed build provenance attestation
from that run. Check one with the GitHub CLI before you use it:

\`\`\`bash
gh attestation verify hopcorder_${version}_linux_amd64.tar.gz $flags
gh attestation verify oci://ghcr.io/saremox/hopcorder-agent@$agent $flags
\`\`\`

Charts: \`helm pull oci://ghcr.io/saremox/charts/hopcorder-server --version $version\`,
verify the \`.tgz\` the same way and install that file. \`--set image.tag=$version@<digest>\`
pins a chart's image to the digest above.
EOF
