#!/bin/sh
# Build a Mao Dispatch upload package. Optional arguments are co-located
# runtime resource paths such as bridge, provider, templates or vendor.
#
# 目标架构由 GOOS / GOARCH 决定，默认本机：
#   ./package.sh                          本机架构
#   GOOS=linux GOARCH=amd64 ./package.sh  平台常见的 Linux x86_64
#   GOOS=linux GOARCH=arm64 ./package.sh  Linux ARM
#
# 入口名和版本都从 manifest.json 读，不在本脚本里写死。产物二进制直接构建到临时
# 暂存目录，不污染工作目录。
#
# MUST: the archive may contain only regular files and directories.
# Platform plugininstall extract() rejects any other tar entry
# (symlinks are type 50). Stage with cp -RL so node_modules/.bin and
# similar links are dereferenced; never tar czf a live tree that still
# has symlinks. COPYFILE_DISABLE=1 keeps macOS AppleDouble files out.
set -eu
cd "$(dirname "$0")"

read_manifest() {
  sed -n 's/^[[:space:]]*"'"$1"'": "\([^"]*\)".*$/\1/p' manifest.json | head -n 1
}

entrypoint=$(read_manifest entrypoint)
version=$(read_manifest version)
for required in entrypoint version; do
  eval "value=\$$required"
  if [ -z "$value" ]; then
    echo "package.sh: cannot read $required from manifest.json" >&2
    exit 1
  fi
done

schema=$(ls ./*.input.schema.json 2>/dev/null | head -n 1)
if [ -z "$schema" ]; then
  echo "package.sh: no *.input.schema.json next to manifest.json" >&2
  exit 1
fi

target_os=${GOOS:-$(go env GOHOSTOS)}
target_arch=${GOARCH:-$(go env GOHOSTARCH)}
host_os=$(go env GOHOSTOS)
host_arch=$(go env GOHOSTARCH)

stage=$(mktemp -d "${TMPDIR:-/tmp}/${entrypoint}.XXXXXX")
trap 'rm -rf "$stage"' EXIT INT TERM

# 契约自检始终用本机架构的二进制跑：交叉编译出来的产物在这台机器上执行不了，
# 而 verify 检查的是 manifest/能力/协议，与目标架构无关。
GOWORK=off CGO_ENABLED=0 GOOS="$host_os" GOARCH="$host_arch" \
  go build -o "$stage/verify-probe" .
"$stage/verify-probe" verify
rm -f "$stage/verify-probe"

GOWORK=off CGO_ENABLED=0 GOOS="$target_os" GOARCH="$target_arch" \
  go build -o "$stage/$entrypoint" .

cp manifest.json "$schema" "$stage/"
# config.json is the channel's freeform, operator-editable settings, read at
# startup and on the ReloadConfig RPC. Ship it so the platform can manage it.
[ -f config.json ] && cp config.json "$stage/"

for resource in "$@"; do
  case "$resource" in
    ""|/*|..|../*|*/../*)
      echo "package.sh: runtime resource must be a safe relative path: $resource" >&2
      exit 1
      ;;
  esac
  if [ ! -e "$resource" ]; then
    echo "package.sh: runtime resource not found: $resource" >&2
    exit 1
  fi
  # Dereference links because the platform upload gate accepts directories and
  # regular files only. A leftover symlink becomes tar type 50 and fails with
  #   unsupported tar entry ".../node_modules/.bin/tldts" (type 50)
  cp -RL "$resource" "$stage/"
done

# Fail closed if any symlink (or other non-regular file) survived staging.
# Portable on macOS and Linux: BSD and GNU find both support these -type tests.
leftovers=$(find "$stage" \( -type l -o -type b -o -type c -o -type p -o -type s \) -print)
if [ -n "$leftovers" ]; then
  echo "package.sh: staging dir still contains non-regular files (symlinks, devices, sockets)." >&2
  echo "package.sh: the upload gate rejects them (symlink = type 50). Use cp -RL; never tar a tree that still has node_modules/.bin links." >&2
  printf '%s\n' "$leftovers" >&2
  exit 1
fi

mkdir -p dist
out="dist/${entrypoint}-${version}-${target_os}-${target_arch}.tar.gz"
COPYFILE_DISABLE=1 tar -czf "$out" -C "$stage" .
bytes=$(wc -c < "$out" | tr -d ' ')
if [ "$bytes" -gt 67108864 ]; then
  echo "package.sh: package exceeds the default 64MiB upload limit: $bytes bytes" >&2
  exit 1
fi

echo "package.sh: wrote $out ($bytes bytes) for ${target_os}/${target_arch}"
tar -tzf "$out"
