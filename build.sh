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
protos="$root/.deps/sdk-rust/crates/protos/protos"
mkdir -p build/classes/temporal/poc
protoc -I "$protos/local" -I "$protos/api_upstream" -I "$protos" --include_imports \
  --descriptor_set_out=build/classes/temporal/poc/schema.pb \
  temporal/sdk/core/workflow_activation/workflow_activation.proto \
  temporal/sdk/core/workflow_completion/workflow_completion.proto \
  temporal/api/workflowservice/v1/request_response.proto
find src/main/java -name '*.java' -print > build/java-sources.txt
javac --release 21 -cp "build/bridge.jar:$(cat build/classpath.txt)" \
  -d build/classes @build/java-sources.txt

# Assemble one Maven artifact containing the Java API and compiled Core bytecode.
mkdir -p build/classes/META-INF
cp LICENSE build/classes/META-INF/LICENSE.sdk-java-poc
cp LICENSE.sdk-java build/classes/META-INF/LICENSE.sdk-java
artifact_dir="$root/build/maven-repository/io/temporal/sdk-java-poc/0.1.0-SNAPSHOT"
mkdir -p "$artifact_dir"
cp build/bridge.jar "$artifact_dir/sdk-java-poc-0.1.0-SNAPSHOT.jar.tmp"
jar --update --file "$artifact_dir/sdk-java-poc-0.1.0-SNAPSHOT.jar.tmp" -C build/classes .
mv "$artifact_dir/sdk-java-poc-0.1.0-SNAPSHOT.jar.tmp" "$artifact_dir/sdk-java-poc-0.1.0-SNAPSHOT.jar"
cp pom.xml "$artifact_dir/sdk-java-poc-0.1.0-SNAPSHOT.pom"
printf 'SDK Java POC Maven repository %s\n' "$root/build/maven-repository"
