# SDK Java POC

This repository experiments with running Temporal SDK Core as JVM bytecode. The Rust worker polls
for workflow activations and accepts protobuf completions from Java. Java owns the gRPC channel,
issues unary RPCs, and signals completion back to Rust. The bridge uses JVM classes and the Java
NIO transport.

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
```

The first test checks that the Core client receives server capabilities over Java gRPC. The
second starts a Core worker, hands its initial workflow activation to Java, and verifies that
Temporal records workflow completion. The tests use a temporary local Temporal server. Logs are
written to `build/server.log`, `build/worker.log`, and `build/workflow.log`.

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
at scale remain unverified. The smoke test verifies workflow completion separately from worker
shutdown, which can time out in the current build.

## Timer probes

`python3 smoke.py --timer` runs a two-second timer workflow. A successful run receives
`InitializeWorkflow` and `FireTimer` in Java and records workflow completion on the server.
This probe has also timed out. `python3 smoke.py --short-timer` runs the 50 ms case that first
exposed the issue. In failing runs, Temporal records the timer firing and starts the next
workflow task, which times out before Java receives `FireTimer`. On failure, the smoke runner
also attempts to save a thread dump in `build/worker-threads.log`.
