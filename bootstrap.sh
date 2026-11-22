#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
deps="$root/.deps"
compiler_revision=99ff1bd21d4ea8673b240dc0ac6fc593d436f61b
core_revision=10fdaa23fec14da1aec3bfbcf06f28858c88bbfa
mkdir -p "$deps"

fetch_checkout() {
  local url="$1" revision="$2" output="$3"
  shift 3
  if [[ -d "$output" ]]; then return; fi
  (
    staging="$(mktemp -d "$deps/checkout.XXXXXX")"
    trap 'rm -rf "$staging"' EXIT
    git -C "$staging" init -q
    git -C "$staging" fetch -q --depth 1 --no-tags "$url" "$revision"
    git -C "$staging" switch -q --detach FETCH_HEAD
    if (( $# )); then git -C "$staging" apply "$@"; fi
    mv "$staging" "$output"
  )
}

fetch_crate() {
  local name="$1" version="$2" sha="$3"
  if [[ -d "$deps/$name" ]]; then return; fi
  (
    staging="$(mktemp -d "$deps/crate.XXXXXX")"
    trap 'rm -rf "$staging"' EXIT
    curl --fail --silent --show-error --location \
      "https://static.crates.io/crates/$name/$name-$version.crate" -o "$staging/source.crate"
    printf '%s  %s\n' "$sha" "$staging/source.crate" | shasum -a 256 -c -
    tar -xzf "$staging/source.crate" -C "$staging"
    patch -d "$staging/$name-$version" -p1 < "$root/patches/$name-jvm.patch"
    mv "$staging/$name-$version" "$deps/$name"
  )
}

activation_patch="$root/patches/sdk-rust-activation-stream.patch"
fetch_checkout https://github.com/temporalio/sdk-rust.git "$core_revision" "$deps/sdk-rust" \
  "$root/patches/sdk-rust-jvm.patch" "$root/patches/sdk-rust-java-callback.patch" "$activation_patch"

# Apply the activation-stream fix to SDK checkouts prepared by an earlier bootstrap.
(
  export GIT_CEILING_DIRECTORIES="$deps"
  cd "$deps/sdk-rust"
  if ! git apply --reverse --check "$activation_patch" 2>/dev/null; then
    git apply "$activation_patch"
  fi
)

fetch_checkout https://github.com/IntegralPilot/rustc_codegen_jvm.git "$compiler_revision" "$deps/compiler"
fetch_crate tonic 0.14.9 e2cdfc673aabd30fe8fee92e65837b644c37c5d47f0f97ae6d5e0f1237d15e1b
fetch_crate http 1.5.0 918d3568bebf352712bc2ef3d46a8bcf1a75b373be6539de198e9105cbbf9ce0
