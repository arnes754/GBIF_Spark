"""Run every test file in this folder, each in a fresh python process.

    uv run python -m gbif_spark test       # the same as:
    uv run python -m gbif_spark.tests

They need no built data: each file starts a small local Spark session and
checks hand-made rows. The one exception is test_curate's shard-picker check,
which reads the snapshot's shard listing (from data/shard_cache.json, or from
S3 the first time). A process per file keeps one file's session out of the
next. Exits non-zero if any file reports a failure.
"""
import pathlib
import subprocess
import sys
import time

from gbif_spark import paths
from gbif_spark.helpers.bench import banner


def main():
    files = sorted(pathlib.Path(__file__).resolve().parent.glob("test_*.py"))
    results = []
    for f in files:
        module = f"gbif_spark.tests.{f.stem}"
        banner(module)
        t0 = time.perf_counter()
        rc = subprocess.run([sys.executable, "-u", "-m", module],
                            cwd=paths.REPO).returncode
        results.append((module, rc, time.perf_counter() - t0))

    banner("tests")
    for module, rc, secs in results:
        print(f"  {'ok  ' if rc == 0 else 'FAIL'} {module:<36}{secs:6.0f}s")
    failed = sum(1 for _, rc, _ in results if rc != 0)
    print(f"\n{len(results)} test files, {failed} failed")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
