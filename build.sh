#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
cd "$root"
compiler="${CARGO_JVM_BACKEND_PATH:-$root/.deps/compiler}"
export CARGO_JVM_BACKEND_PATH="$compiler"
export CARGO_JVM_CONFIG="${CARGO_JVM_CONFIG:-$root/.deps/cargo-jvm.toml}"
cli="$compiler/cargo-jvm/target/release/cargo-jvm"
if [[ ! -x "$cli" ]]; then
  (cd "$compiler" && python3 build.py all)
fi

mkdir -p build/classes
"$cli" package --locked --manifest-path bridge/Cargo.toml --lib --output "$root/build/bridge.jar" \
  --config 'build.rustflags = ["--cfg", "getrandom_backend=\"custom\""]'
mvn -q org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
  -Dmdep.outputFile="$root/build/classpath.txt"
javac --release 21 -cp "build/bridge.jar:$(cat build/classpath.txt)" \
  -d build/classes src/main/java/poc/*.java
