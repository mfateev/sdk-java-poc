# SDK Java POC

This repository experiments with running Temporal SDK Core as JVM bytecode. The Rust worker polls
for workflow activations and accepts protobuf completions from Java. Java owns the gRPC channel,
issues unary RPCs, and signals completion back to Rust. The bridge uses JVM classes and the Java
NIO transport.

## Architecture decision

Build `sdk-java-poc` as a standalone Java SDK backed by SDK Core. Applications declare
`sdk-java-poc` as their sole Temporal SDK dependency. The POC provides both the public Java
API and its implementation.

- Compile the Java API, gRPC client, and deterministic workflow runner from pinned SDK Java
  source at revision `2ff9c2c1`. Package them with Core in the POC artifact.
- Drive the existing Java workflow executor through a Core-backed `ReplayWorkflowContext`.
  Core polls workflow tasks, replays history, matches commands, and requests cache eviction.
  Java runs workflow code and manages workflow threads, promises, and callbacks.
- Target JDK 21. The POC starts fresh workflow runs on Core and replays their Core-authored
  histories. The legacy Java SDK continues running workflows that started on its engine.

The timer milestone covers typed workflow registration, synchronous invocation, and durable
timers through `Workflow.sleep`, including replay after a worker restart. Worker support for
activities, Nexus, updates, continue-as-new, side effects, version markers, and offline history
replay is outside this milestone.

## Build

Use JDK 21, Python 3.11+, Git, curl, and Rustup. The smoke test also needs the Temporal CLI.

```sh
./bootstrap.sh
./gradlew publish
```

Bootstrap checks out the pinned SDK Java source and SDK Rust revision `10fdaa23`, then applies
this repository's Rust patches,
including the JVM build patch from `0fc90e3c`. It also pins the compiler to upstream
[`experimental2` at `99ff1bd`](https://github.com/IntegralPilot/rustc_codegen_jvm/commit/99ff1bd).
The HTTP and Tonic crate archives are checksum-verified before patching. The patches are applied
to local copies under `.deps/`.

Gradle runs the pinned JVM Rust compiler, generates Java protobuf classes and gRPC stubs from
Core’s schemas, and builds one JAR containing Java and SDK Core bytecode. It publishes `io.temporal:sdk-java-poc:0.1.0-SNAPSHOT` to the file repository at
`build/maven-repository/`. You can set `CARGO_JVM_BACKEND_PATH` to reuse a built compiler
checkout at the same revision. `./build.sh` also runs `./gradlew publish`.

The standalone `poc-timer` build on the `codex/sdk-java-poc` branch of `samples-java`
resolves this Maven artifact and demonstrates the public API. Its restart verifier kills the
first worker while the first timer is pending, starts a new JVM, and checks the result and server
history.

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
java -Xss16m -cp "$(cat build/classpath.txt)" \
  poc.TimerWorker 127.0.0.1 7233 java-poc

temporal workflow execute --type TimerWorkflow --task-queue java-poc --workflow-id java-poc-timer
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

The low-level smoke tests exercise the bridge directly and check clean worker shutdown.
The public timer sample exercises the SDK Java runner and the Core-backed context.

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
