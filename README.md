# SDK Java POC

This repository experiments with running Temporal SDK Core as JVM bytecode. The Rust worker polls
for workflow activations and accepts protobuf completions from Java. Java owns the gRPC channel,
issues unary RPCs, and signals completion back to Rust. The bridge uses JVM classes and the Java
NIO transport.

## Architecture decision

Build `sdk-java-poc` as a standalone Java SDK backed by SDK Core. Applications declare
`sdk-java-poc` as their sole Temporal SDK dependency. The POC provides both the public Java
API and its implementation.

- Check in the Java API, gRPC client, and deterministic workflow runner from SDK Java
  revision `2ff9c2c1` under [`src/sdk-java/java`](src/sdk-java/java). Gradle compiles that
  source directly and packages it with Core in the POC artifact.
- Drive the existing Java workflow executor through a Core-backed `ReplayWorkflowContext`.
  Core polls workflow tasks, replays history, matches commands, and requests cache eviction.
  Java runs workflow code and manages workflow threads, promises, and callbacks.
- Target JDK 21. The POC starts fresh workflow runs on Core and replays their Core-authored
  histories. The legacy Java SDK continues running workflows that started on its engine.

The POC supports typed workflows, timers, signals, queries, updates, remote and local activities,
child workflows, external workflow signals and cancellation, Nexus calls, continue-as-new, Worker
Deployment Versioning, and `Workflow.getVersion` through Core patch markers. Core dispatches local
activities to Java. The existing Java workers continue to poll remote activity and Nexus handler
tasks. Core uses `WorkerOptions` to configure workflow pollers and reports each workflow's pinned
or auto-upgrade behavior from the Java runner.
The public `Workflow.sideEffect` and `Workflow.mutableSideEffect` overloads are commented out.
Offline history replay is outside the POC.

## Build

Use JDK 21, Python 3.11+, Git, curl, and Rustup.

```sh
./bootstrap.sh
./gradlew publish
```

Bootstrap fetches the pinned SDK Java commit for source verification and checks out SDK Rust
revision `10fdaa23`. It applies this repository's Rust patches, including the JVM build
patch from `0fc90e3c`. It also pins the compiler to upstream
[`experimental2` at `99ff1bd`](https://github.com/IntegralPilot/rustc_codegen_jvm/commit/99ff1bd).
The HTTP and Tonic crate archives are checksum-verified before patching. The patches are applied
to local copies under `.deps/`.

Gradle runs the pinned JVM Rust compiler, generates Java protobuf classes and gRPC stubs from
Core’s schemas, and builds one JAR containing Java and SDK Core bytecode. It publishes `io.temporal:sdk-java-poc:0.1.0-SNAPSHOT` to the file repository at
`build/maven-repository/`. You can set `CARGO_JVM_BACKEND_PATH` to reuse a built compiler
checkout at the same revision. `./build.sh` also runs `./gradlew publish`.

## Verify the SDK

```sh
./gradlew verifySdkJava
```

The verifier compares the checked-in workflow runner, its callers, and the client source
with the pinned SDK Java commit byte for byte. It allows the specific comments that disable
the public side-effect overloads in `Workflow.java` and checks that they are absent from the
compiled API. The checked-in sources include SDK Java’s Java 21 virtual-thread implementation. The Core integration sources live under `src/main/java`,
where `ReplayWorkflowExecutor.java` and `SyncWorkflowWorker.java` connect the runner to Core.

The verifier compares the compiled interface and its nested types with a separate compilation of
the verbatim SDK Java sources. It checks every public instance method of the Core implementation,
including inherited methods and generic signatures.
It calls each unimplemented operation and checks that it throws `UnimplementedException`.
Gradle's `check` runs this verification against the packaged SDK JAR.

With the Temporal CLI on your path, run the activation regression test against a temporary local
server. The test restarts the worker after the operation results are in history and verifies replay.

```sh
./gradlew verifyCoreActivations
```

## Bridge surface

A `CoreWorker` takes a caller-owned `io.grpc.Channel`, namespace, task queue, and Java activation
handler. Handler input and output are the serialized SDK Core
[`WorkflowActivation`](https://github.com/temporalio/sdk-rust/blob/10fdaa23/crates/protos/protos/local/temporal/sdk/core/workflow_activation/workflow_activation.proto)
and [`WorkflowActivationCompletion`](https://github.com/temporalio/sdk-rust/blob/10fdaa23/crates/protos/protos/local/temporal/sdk/core/workflow_completion/workflow_completion.proto)
messages. `CoreWorkflowActivationHandler` decodes jobs, drives `ReplayWorkflowExecutor`,
and collects typed commands from `CoreReplayWorkflowContext`. An initialize job supplies
the start attributes used by the Java runner. A timer resolution queues the existing Java
timer callback. An eviction closes the Java runner for that run.

## JVM worker workarounds

The JVM build uses an `Arc` for Core's activation-stream mutex. With the pinned
`rustc_codegen_jvm` revision, repeated async borrows of the inline mutex through the
worker's `LazyLock` can resolve to different backing addresses. In stalled runs, the first
guard returned its permit to one address and the next poll reached another address with
zero permits. Core queued `FireTimer` while the consumer waited for the lock.

- [ ] Report the `LazyLock` reference-identity bug to `rustc_codegen_jvm` with the
  [standalone Rust reproducer](probes/lazy-lock-jvm-repro.rs). The same two live borrows
  compare equal natively and unequal on the pinned JVM build, including under `-Xint`.

The JVM build also retains the concrete workflow poller type during shutdown. This avoids
the missing trait class the pinned compiler emits for the boxed poller shutdown call.
