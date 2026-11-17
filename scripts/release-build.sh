#!/usr/bin/env bash
# Builds the release assets into dist/: static Linux binaries (amd64, arm64)
# with the license, README, example configs and systemd units, the Helm chart
# packages and SHA256SUMS; and notes.md, the version's section of
# CHANGELOG.md (empty if there is none). Used by .github/workflows/images.yml
# for releases and, as a dry run, for pull requests and main.
#
#   scripts/release-build.sh 1.2.3
set -euo pipefail
version=${1:?usage: release-build.sh <version without the leading v>}
cd "$(dirname "$0")/.."

# Releases are built with go.mod's toolchain (the Go release of the images,
# with its security fixes), not whatever Go happens to be installed.
want=$(sed -n 's/^toolchain //p' go.mod)
have=$(go env GOVERSION)
if [ -z "$want" ] || [ "$have" != "$want" ]; then
  echo "release-build: building with $have, go.mod's toolchain is ${want:-missing}" >&2
  exit 1
fi

rm -rf dist build
mkdir -p dist build

for arch in amd64 arm64; do
  d="hopcorder_${version}_linux_${arch}"
  mkdir -p "build/$d"
  CGO_ENABLED=0 GOOS=linux GOARCH="$arch" go build -trimpath \
    -ldflags "-s -w -X main.version=v${version}" -o "build/$d/hopcorder" ./cmd/hopcorder
  cp LICENSE README.md deploy/agent.example.yaml deploy/server.example.yaml \
    deploy/hopcorder.service deploy/hopcorder-server.service deploy/hopcorder-server.socket "build/$d/"
  tar -C build -czf "dist/$d.tar.gz" "$d"
done
out=$("build/hopcorder_${version}_linux_amd64/hopcorder" version)
echo "$out"
grep -qF "v${version}" <<<"$out"

for chart in hopcorder-server hopcorder-agent; do
  helm package "deploy/helm/$chart" --version "$version" --app-version "$version" -d dist
done

(cd dist && sha256sum -- * > SHA256SUMS && cat SHA256SUMS)
# The section "## v<version>" of CHANGELOG.md, up to the next one.
awk -v h="## v${version}" '$0 == h {on = 1; next} on && /^## / {exit} on' CHANGELOG.md > notes.md
