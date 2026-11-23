#!/usr/bin/env python3
"""Exercise the Core activation adapters through Java workflows and a worker restart."""

import argparse
from collections import Counter
from contextlib import ExitStack
import json
import os
from pathlib import Path
import socket
import subprocess
import time
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
QUEUE = "core-activation-probe"


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise


def counts(history):
    return Counter(event["eventType"] for event in history.get("events", []))


def verify(logs, classpath, java_executable):
    logs.mkdir(parents=True, exist_ok=True)
    classes = logs / "classes"
    classes.mkdir(exist_ok=True)
    javac = java_executable.with_name("javac" + java_executable.suffix)
    subprocess.run(
        [str(javac), "-cp", classpath, "-d", str(classes), str(ROOT / "probes/CoreActivationProbe.java")],
        check=True,
    )
    java = [str(java_executable), "-Xss16m", "-cp", os.pathsep.join((str(classes), classpath)),
            "CoreActivationProbe"]
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    target = f"127.0.0.1:{port}"
    workflow_id = f"core-activation-{uuid4()}"
    environment = {key: value for key, value in os.environ.items() if not key.startswith("TEMPORAL_")}

    def temporal(*args, check=True):
        return subprocess.run(
            ["temporal", *args, "--address", target], capture_output=True,
            text=True, check=check, timeout=15, env=environment,
        )

    def history(id=workflow_id):
        result = temporal("workflow", "show", "--workflow-id", id, "--output", "json", check=False)
        return json.loads(result.stdout) if result.returncode == 0 else {}

    with ExitStack() as cleanup:
        def launch(label, args):
            stream = cleanup.enter_context((logs / f"{label}.log").open("w"))
            process = subprocess.Popen(args, stdout=stream, stderr=subprocess.STDOUT, env=environment)
            cleanup.callback(stop, process)
            return process

        server = launch("server", [
            "temporal", "server", "start-dev", "--headless", "--ip", "127.0.0.1", "--port", str(port),
        ])
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                if server.poll() is not None:
                    raise RuntimeError("Temporal server exited")
                time.sleep(0.1)
        else:
            raise TimeoutError("Temporal server did not start")
        temporal("operator", "nexus", "endpoint", "create", "--name", QUEUE,
                 "--target-namespace", "default", "--target-task-queue", QUEUE)
        worker = launch("worker-before", [*java, target, "worker"])
        client = launch("client", [*java, target, "client", workflow_id])
        before = {}
        try:
            deadline = time.monotonic() + 65
            while time.monotonic() < deadline:
                before = history()
                events = before.get("events", [])
                seen = counts(before)
                if (seen["EVENT_TYPE_NEXUS_OPERATION_COMPLETED"] == 2
                        and events[-1]["eventType"] == "EVENT_TYPE_WORKFLOW_TASK_COMPLETED"):
                    break
                if worker.poll() is not None or client.poll() is not None:
                    raise RuntimeError("Worker or client exited before the restart")
                if seen["EVENT_TYPE_WORKFLOW_TASK_FAILED"] > 0:
                    raise RuntimeError("Workflow task failed before the restart")
                time.sleep(0.2)
            else:
                raise TimeoutError("Workflow did not reach the replay checkpoint")
        finally:
            (logs / "before-restart.json").write_text(json.dumps(before, indent=2))

        worker.kill()
        worker.wait(timeout=5)
        replacement = launch("worker-after", [*java, target, "worker"])
        temporal("workflow", "signal", "--workflow-id", workflow_id, "--name", "finish")
        try:
            if client.wait(timeout=45) != 0:
                raise RuntimeError("Client failed after restart")
        finally:
            after = history()
            (logs / "after-restart.json").write_text(json.dumps(after, indent=2))

        expected = {
            "EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED": 1,
            "EVENT_TYPE_WORKFLOW_TASK_FAILED": 0,
            "EVENT_TYPE_ACTIVITY_TASK_COMPLETED": 1,
            "EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED": 1,
            "EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED": 1,
            "EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED": 1,
            "EVENT_TYPE_NEXUS_OPERATION_COMPLETED": 2,
            "EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED": 1,
            "EVENT_TYPE_TIMER_FIRED": 1,
        }
        seen = counts(after)
        if any(seen[name] != count for name, count in expected.items()):
            raise RuntimeError(f"Unexpected parent workflow history {seen}")
        target_history = history(workflow_id + "-target")
        (logs / "target.json").write_text(json.dumps(target_history, indent=2))
        if counts(target_history)["EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED"] != 1:
            raise RuntimeError("The external workflow was not canceled")
        print((logs / "client.log").read_text().strip())
        print(f"Replayed operations after replacing worker JVM {worker.pid} with {replacement.pid}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classpath", required=True, help="SDK JAR and runtime dependencies.")
    parser.add_argument("--java", type=Path, default=Path("java"), help="Java 21 executable.")
    args = parser.parse_args()
    verify(ROOT / "build/core-activation-verification", args.classpath, args.java)
