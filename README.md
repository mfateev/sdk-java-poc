# SDK Java POC

This repository experiments with running Temporal SDK Core as JVM bytecode. The Rust worker polls
for workflow activations and accepts protobuf completions from Java. Java owns the gRPC channel,
issues unary RPCs, and signals completion back to Rust. The bridge uses JVM classes and the Java
NIO transport.

## Architecture decision

Build `sdk-java-poc` as a standalone Java SDK backed by SDK Core. Applications declare
`sdk-java-poc` as their sole Temporal SDK dependency. The POC provides both the public Java
API and its implementation.

- Copy the public API types from SDK Java, including the annotations, options, and other
  types needed by their signatures. Use SDK Java revision `2ff9c2c1` as the starting API
  reference and retain upstream license notices with copied source. Preserve package names
  and signatures for the API surface implemented by the POC.
- Write a new Java implementation over the Core activation and completion protocol.
  Core owns workflow task polling, history replay, and the state machines. The POC owns
  workflow execution and scheduling in Java, application callbacks, and Java gRPC transport.
- Target source compatibility for the copied API on JDK 21. The POC starts fresh workflow
  runs on Core and replays their Core-authored histories. The legacy Java SDK continues
  running workflows that started on its engine.

The current timer example exercises the bridge. The next POC milestone is an application
that registers and completes a workflow through the copied public API with
`sdk-java-poc` as its only Temporal SDK dependency.

## Build

Use JDK 21, Maven, Python 3.11+, Git, curl, Rustup, and `protoc`. The smoke test also needs the
Temporal CLI.

```sh
./bootstrap.sh
./build.sh
```

Bootstrap checks out public SDK Rust revision `10fdaa23` and applies this repository's patches,
including the JVM build patch from `0fc90e3c`. It also pins the compiler to upstream
[`experimental2` at `99ff1bd`](https://github.com/IntegralPilot/rustc_codegen_jvm/commit/99ff1bd).
The HTTP and Tonic crate archives are checksum-verified before patching. The patches are applied
to local copies under `.deps/`.

The build creates `build/bridge.jar` and compiles the Java sources into `build/classes/`.
You can set `CARGO_JVM_BACKEND_PATH` to reuse a built compiler checkout at the same revision.

## Run

```sh
python3 smoke.py --transport-only
python3 smoke.py
python3 smoke.py --timer
python3 smoke.py --short-timer
```

The first test checks that the Core client receives server capabilities over Java gRPC. The
second starts a Core worker, hands its initial workflow activation to Java, and verifies that
Temporal records workflow completion. The timer tests verify `InitializeWorkflow` and
`FireTimer` in Java and workflow completion on the server. They use a two-second timer and a
50 ms timer, respectively. The tests use a temporary local Temporal server. Logs are written to
`build/server.log`, `build/worker.log`, and `build/workflow.log`.

The example can also connect to an existing local server.

```sh
java -Xss16m -cp "build/classes:build/bridge.jar:$(cat build/classpath.txt)" \
  poc.TimerWorker 127.0.0.1 7233 java-poc

temporal workflow execute --type TimerWorkflow --task-queue java-poc --workflow-id java-poc-timer
```

## Bridge surface

A `CoreWorker` takes a caller-owned `io.grpc.Channel`, namespace, task queue, and Java activation
handler. Handler input and output are the serialized SDK Core
[`WorkflowActivation`](https://github.com/temporalio/sdk-rust/blob/10fdaa23/crates/protos/protos/local/temporal/sdk/core/workflow_activation/workflow_activation.proto)
and [`WorkflowActivationCompletion`](https://github.com/temporalio/sdk-rust/blob/10fdaa23/crates/protos/protos/local/temporal/sdk/core/workflow_completion/workflow_completion.proto)
messages. Java code supplies workflow execution logic. SDK Core supplies workflow task handling.

The bridge handles workflow tasks. The example uses a plaintext local gRPC channel and a minimal
Java workflow handler. Legacy Java histories, activities, Nexus, and connection lifecycle behavior
at scale remain unverified. The smoke test verifies workflow completion. Worker shutdown still
fails in some runs.

## JVM activation-stream workaround

The JVM build uses an `Arc` for Core's activation-stream mutex. With the pinned
`rustc_codegen_jvm` revision, repeated async borrows of the inline mutex through the
worker's `LazyLock` can resolve to different backing addresses. In stalled runs, the first
guard returned its permit to one address and the next poll reached another address with
zero permits. Core queued `FireTimer` while the consumer waited for the lock.

- [ ] Report the `LazyLock` reference-identity bug to `rustc_codegen_jvm` with the
  [standalone Rust reproducer](probes/lazy-lock-jvm-repro.rs). The same two live borrows
  compare equal natively and unequal on the pinned JVM build, including under `-Xint`.
