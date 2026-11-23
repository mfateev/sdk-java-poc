#!/usr/bin/env python3
"""Verify the SDK Java source copies and the Core workflow context contract."""

import argparse
from difflib import unified_diff
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
from tempfile import TemporaryDirectory


ROOT = Path(__file__).resolve().parent
SOURCE_ROOTS = (
    "temporal-sdk/src/main/java",
    "temporal-sdk/src/main/java21",
    "temporal-serviceclient/src/main/java",
)
CORE_INTEGRATION = {
    "io/temporal/internal/replay/ReplayWorkflowExecutor.java",
    "io/temporal/internal/worker/SyncWorkflowWorker.java",
}
VIRTUAL_THREAD_DELEGATE = "io/temporal/internal/task/VirtualThreadDelegate.java"


def reference_sources(checkout, revision):
    git = ["git", "-C", str(checkout)]
    if subprocess.check_output(
        git + ["rev-parse", "--show-toplevel"], text=True
    ).strip() != str(checkout.resolve()):
        raise ValueError(f"Expected an SDK Java Git checkout at {checkout}")

    trees = {root: {} for root in SOURCE_ROOTS}
    with subprocess.Popen(
        git + ["archive", "--format=tar", revision, *SOURCE_ROOTS], stdout=subprocess.PIPE
    ) as archive:
        with tarfile.open(fileobj=archive.stdout, mode="r|") as sources:
            for source in sources:
                if not source.isfile() or not source.name.endswith(".java"):
                    continue
                for root, tree in trees.items():
                    if source.name.startswith(root + "/"):
                        name = source.name.removeprefix(root + "/")
                        tree[name] = sources.extractfile(source).read()
                        break
        if archive.wait():
            raise ValueError(f"Could not read SDK Java sources at {revision}")

    stock, java21, client = trees.values()
    if stock.keys() & java21.keys() != {VIRTUAL_THREAD_DELEGATE}:
        raise ValueError("Unexpected overlap between the Java and Java 21 source trees")
    if (stock.keys() | java21.keys()) & client.keys():
        raise ValueError("SDK Java and service client source paths overlap")
    expected = stock | java21 | client
    absent = CORE_INTEGRATION - expected.keys()
    if absent:
        raise ValueError(f"Core integration source paths missing upstream: {', '.join(sorted(absent))}")
    for replacement in CORE_INTEGRATION:
        del expected[replacement]
    return expected


def java_files(root):
    return {file.relative_to(root).as_posix(): file for file in root.rglob("*.java")}


def verify_copies(checkout, copies):
    bootstrap = (ROOT / "bootstrap.sh").read_text()
    match = re.search(r"^java_revision=([0-9a-f]{40})$", bootstrap, re.MULTILINE)
    if not match:
        raise ValueError("Could not read the pinned SDK Java revision from bootstrap.sh")
    revision = match.group(1)
    expected = reference_sources(checkout, revision)
    copied = java_files(copies)
    local = java_files(ROOT / "src/main/java")
    problems = []
    problems.extend(f"missing copy {name}" for name in sorted(expected.keys() - copied.keys()))
    problems.extend(f"unexpected copy {name}" for name in sorted(copied.keys() - expected.keys()))
    problems.extend(
        f"changed copy {name}"
        for name in sorted(copied.keys() & expected.keys())
        if copied[name].read_bytes() != expected[name]
    )
    problems.extend(f"local source shadows {name}" for name in sorted(local.keys() & expected.keys()))
    problems.extend(
        f"missing Core integration source {name}" for name in sorted(CORE_INTEGRATION - local.keys())
    )
    if problems:
        raise ValueError("SDK Java source parity failed:\n  " + "\n  ".join(problems))
    print(f"Verified {len(expected)} verbatim SDK Java sources at {revision[:8]}.", flush=True)


def verify_compiled_contract(java, classpath, copies):
    # Recompile the two verbatim interfaces independently to catch stale or modified class files.
    replay = copies / "io/temporal/internal/replay"
    java = Path(java)
    javac = str(java.with_name("javac" + java.suffix))
    javap = str(java.with_name("javap" + java.suffix))
    with TemporaryDirectory(prefix="sdk-java-contract-") as directory:
        subprocess.run(
            [javac, "-classpath", classpath, "-d", directory,
             str(replay / "ReplayAware.java"), str(replay / "ReplayWorkflowContext.java")],
            check=True,
        )
        reference_classpath = os.pathsep.join((directory, classpath))
        for file in sorted(Path(directory).rglob("*.class")):
            name = ".".join(file.relative_to(directory).with_suffix("").parts)
            expected = subprocess.check_output(
                [javap, "-classpath", reference_classpath, "-public", "-s", name], text=True
            )
            actual = subprocess.check_output(
                [javap, "-classpath", classpath, "-public", "-s", name], text=True
            )
            if expected != actual:
                diff = "".join(unified_diff(
                    expected.splitlines(keepends=True), actual.splitlines(keepends=True),
                    fromfile="SDK Java", tofile="POC",
                ))
                raise ValueError(f"Compiled contract differs for {name}\n{diff}")
    print("Verified the compiled ReplayWorkflowContext interface, parent, and nested types.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdk-java", type=Path, default=ROOT / ".deps/sdk-java")
    parser.add_argument("--copied-sources", type=Path, default=ROOT / "build/generated/sdk-java")
    parser.add_argument("--classpath", help="Defaults to the packaged JAR classpath in build/classpath.txt.")
    parser.add_argument("--java", default="java")
    args = parser.parse_args()

    try:
        verify_copies(args.sdk_java, args.copied_sources)
        classpath = args.classpath or (ROOT / "build/classpath.txt").read_text().strip()
        verify_compiled_contract(args.java, classpath, args.copied_sources)
        subprocess.run(
            [args.java, "--class-path", classpath, str(ROOT / "probes/VerifyReplayWorkflowContext.java")],
            check=True,
        )
    except (OSError, ValueError, subprocess.CalledProcessError, tarfile.TarError) as error:
        print(error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
