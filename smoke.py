"""Run the Java bridge against an in-memory Temporal server on loopback."""
import argparse
import socket
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BUILD = ROOT / "build"
HOST = "127.0.0.1"
QUEUE = "java-poc-smoke"


def read(path):
    return path.read_text(errors="replace")


def wait_for_server(server, port):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and server.poll() is None:
        try:
            with socket.create_connection((HOST, port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(read(BUILD / "server.log"))


def stop(process):
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=12)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--transport-only", action="store_true")
    mode.add_argument("--timer", action="store_true")
    mode.add_argument("--short-timer", action="store_true")
    args = parser.parse_args()
    workflow_type = "SingleTaskWorkflow"
    if args.timer:
        workflow_type = "TimerWorkflow"
    elif args.short_timer:
        workflow_type = "ShortTimerWorkflow"

    with socket.socket() as reservation:
        reservation.bind((HOST, 0))
        port = reservation.getsockname()[1]

    classpath = read(BUILD / "classpath.txt").strip()
    server, worker, workflow = None, None, None
    with (BUILD / "server.log").open("w") as server_log, \
         (BUILD / "worker.log").open("w") as worker_log, \
         (BUILD / "workflow.log").open("w") as workflow_log:
        try:
            server = subprocess.Popen(
                ["temporal", "server", "start-dev", "--ip", HOST, "--port", str(port), "--headless"],
                stdout=server_log, stderr=subprocess.STDOUT,
            )
            wait_for_server(server, port)
            worker = subprocess.Popen(
                ["java", "-Xss16m", "-cp", classpath, "poc.TimerWorker", HOST, str(port),
                 "--transport-only" if args.transport_only else QUEUE],
                stdout=worker_log, stderr=subprocess.STDOUT,
            )
            if args.transport_only:
                if worker.wait(timeout=30) != 0:
                    raise RuntimeError(read(BUILD / "worker.log"))
                print(read(BUILD / "worker.log"))
                return
            workflow = subprocess.Popen(
                ["temporal", "workflow", "execute", "--address", f"{HOST}:{port}", "--namespace", "default",
                 "--type", workflow_type,
                 "--task-queue", QUEUE, "--workflow-id", "java-poc-smoke",
                 "--execution-timeout", "60s", "--task-timeout", "30s"],
                stdout=workflow_log, stderr=subprocess.STDOUT,
            )
            deadline = time.monotonic() + 65
            while workflow.poll() is None and worker.poll() is None and time.monotonic() < deadline:
                time.sleep(0.1)
            output = read(BUILD / "workflow.log")
            if workflow.poll() != 0 or "COMPLETED" not in output:
                if worker.poll() is None:
                    with (BUILD / "worker-threads.log").open("w") as trace:
                        subprocess.run(["jcmd", str(worker.pid), "Thread.print"], stdout=trace,
                                       stderr=subprocess.STDOUT, timeout=5, check=False)
                raise RuntimeError(output + "\n" + read(BUILD / "worker.log"))
            logs = read(BUILD / "worker.log")
            expected = ["Java received InitializeWorkflow"]
            if args.timer or args.short_timer:
                expected.append("Java received FireTimer")
            if not all(line in logs for line in expected):
                raise RuntimeError("Missing Java workflow activations\n" + logs)
            stop(worker)
            logs = read(BUILD / "worker.log")
            if "Core worker shut down cleanly" not in logs:
                raise RuntimeError("Core worker shutdown did not complete cleanly\n" + logs)
            print(output)
            print(logs)
        finally:
            stop(workflow)
            stop(worker)
            stop(server)


if __name__ == "__main__":
    main()
