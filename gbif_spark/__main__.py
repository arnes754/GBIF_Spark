"""The end result in one command: build the data, run the job, print the answer.

    uv run python -m gbif_spark all             # everything, end to end
    uv run python -m gbif_spark all --gb 5      # the same over a 5 GB slice

`all` is `data` followed by `job`, and each half runs on its own too:

    uv run python -m gbif_spark data            # build the data only
    uv run python -m gbif_spark job             # run the job only

`data` is four steps, and each of them is a command of its own as well:

    1. curate build     the fact table, from parquet shards on S3
    2. registry keys    the datasetkeys the fact table references
    3. registry fetch   those datasets and their publishers, from the GBIF API
    4. registry build   the publisher dimension, from what was fetched

`job` is step 5: the nine-stage job, its result tables and the run log.
Everything after `curate`, `registry` or `job` goes to that module as it is,
so `uv run python -m gbif_spark job --help` lists the job's tuning flags.
Flags that `all` does not know go to the job as well:

    uv run python -m gbif_spark all --batches b0000 --shuffle-partitions 24

Each step of `all` and `data` runs in a fresh python process with its own
Spark session, and its command is printed above its output, so a failure
shows which step broke and how to re-run only that one. Steps that are
already done are skipped: a second `all` downloads nothing and goes straight
to the job.

The rest:

    uv run python -m gbif_spark status          # what is built, what has run
    uv run python -m gbif_spark test            # the tests
    uv run python -m gbif_spark day 5           # one day of weeks/, by number

Run everything from the repo root.
"""
import argparse
import importlib
import pathlib
import runpy
import shlex
import subprocess
import sys
import time

from gbif_spark import paths
from gbif_spark.helpers.bench import banner

FORWARD = {
    "job": "gbif_spark.pipeline.run_job",
    "curate": "gbif_spark.pipeline.curate",
    "registry": "gbif_spark.pipeline.registry",
    "test": "gbif_spark.tests.__main__",
}

TARGET_SLACK = 0.9


def newer(path, *than):
    """True if `path` exists and is at least as new as each of `than` that
    exists."""
    if not path.exists():
        return False
    return all(path.stat().st_mtime >= p.stat().st_mtime
               for p in than if p.exists())


def data_steps(args, curate, registry):
    """The data steps as (title, decide) pairs. decide() is called just before
    its step runs, because each step looks at what the one before it wrote,
    and returns (command, None) to run it or (None, reason) to skip it."""
    manifest = curate.manifest_path(curate.DEFAULT_OUT)
    dimension_done = pathlib.Path(registry.DEFAULT_OUT) / "_SUCCESS"

    def facts():
        batches = curate.load_manifest(curate.DEFAULT_OUT)["batches"]
        have = sum(b["source_bytes"] for b in batches) / 1024**3
        if have >= TARGET_SLACK * args.gb:
            return None, (f"the table already holds {have:.2f} GB of the "
                          f"snapshot (target {args.gb:g} GB)")
        return ["curate", "build", "--gb", f"{round(args.gb - have, 2):g}"], None

    def keys():
        if not args.force and newer(registry.KEYS, manifest):
            return None, "keys.json is newer than the fact table"
        return ["registry", "keys"], None

    def fetch():
        return ["registry", "fetch"], None

    def dimension():
        if not args.force and newer(dimension_done, registry.DATASETS,
                                    registry.ORGS):
            return None, "the dimension is newer than the registry cache"
        return ["registry", "build"], None

    return [("fact table", facts), ("dataset keys", keys),
            ("registry fetch", fetch), ("dimension", dimension)]


def job_command(extra):
    """Step 5. A run started by `all` is tagged end-to-end in the run log,
    unless the flags passed on give it a tag of their own."""
    cmd = ["job", *extra]
    if not any(a == "--tag" or a.startswith("--tag=") for a in extra):
        cmd += ["--tag", "end-to-end"]
    return cmd


def run_steps(steps):
    """Run (title, decide) steps in order and stop at the first failure.
    Returns (title, outcome, seconds) for every step it reached."""
    log = []
    for i, (title, decide) in enumerate(steps, 1):
        banner(f"step {i}/{len(steps)}: {title}")
        cmd, skip = decide()
        if skip:
            print(f"  skipped - {skip}", flush=True)
            log.append((title, "skipped", None))
            continue
        shown = "uv run python -m gbif_spark " + shlex.join(cmd)
        print(f"  $ {shown}\n", flush=True)
        t0 = time.perf_counter()
        try:
            rc = subprocess.run([sys.executable, "-u", "-m", "gbif_spark", *cmd],
                                cwd=paths.REPO).returncode
        except KeyboardInterrupt:
            rc = 130
        log.append((title, "ok" if rc == 0 else f"FAILED (exit {rc})",
                    time.perf_counter() - t0))
        if rc != 0:
            print_log(log)
            print(f"\n  Stopped at step {i}. Fix what the output above complains"
                  f" about, then re-run\n  only this step:\n\n    {shown}\n\n"
                  f"  or the whole command again: finished steps are skipped.")
            raise SystemExit(rc)
    return log


def print_log(log, wall=None):
    banner("summary")
    for title, outcome, secs in log:
        took = "" if secs is None else f"{secs:8.0f}s"
        print(f"  {title:<18}{outcome:<20}{took}")
    if wall is not None:
        print(f"  {'total':<38}{wall:8.0f}s")


def cmd_build(args, extra):
    """`all` and `data`."""
    try:
        from gbif_spark.pipeline import curate, registry
    except RuntimeError as exc:
        raise SystemExit(str(exc))
    steps = data_steps(args, curate, registry)
    if args.cmd == "all":
        steps.append(("job", lambda: (job_command(extra), None)))
    else:
        steps.append(("check", lambda: (["curate", "status"], None)))

    t0 = time.perf_counter()
    log = run_steps(steps)
    print_log(log, time.perf_counter() - t0)
    print(f"\n  fact table   {paths.FACT_TABLE}"
          f"\n  dimension    {paths.DIMENSION}")
    if args.cmd == "all":
        print(f"  results      {paths.RESULTS}"
              f"\n  run log      {paths.RUNS_LOG}"
              f"\n\n  every run so far: uv run python -m gbif_spark job --report")


def cmd_status():
    """What is built and what has run, from each module's own report."""
    for title, cmd in [(None, ["curate", "status"]),
                       ("registry cache and dimension", ["registry", "status"]),
                       (None, ["job", "--report"])]:
        if title:
            banner(title)
        subprocess.run([sys.executable, "-u", "-m", "gbif_spark", *cmd],
                       cwd=paths.REPO)


def cmd_day(argv):
    """`day N [flags]`: run weeks/weekK/dayN.py exactly as `python -m` would."""
    days = {f.stem[3:]: f for f in paths.PACKAGE.glob("weeks/week*/day*.py")}
    if not argv or argv[0] not in days:
        raise SystemExit("usage: python -m gbif_spark day N [flags]   (N is one "
                         f"of {', '.join(sorted(days, key=int))})")
    f = days[argv[0]]
    sys.argv = [str(f), *argv[1:]]
    runpy.run_module(f"gbif_spark.weeks.{f.parent.name}.{f.stem}",
                     run_name="__main__", alter_sys=True)


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] in FORWARD:
        sys.argv = [f"python -m gbif_spark {argv[0]}", *argv[1:]]
        return importlib.import_module(FORWARD[argv[0]]).main()
    if argv and argv[0] == "day":
        return cmd_day(argv[1:])

    p = argparse.ArgumentParser(prog="python -m gbif_spark", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", metavar="command")
    for name, text in [("all", "build the data if needed, then run the job"),
                       ("data", "build the fact table and the publisher dimension")]:
        s = sub.add_parser(name, help=text, description=text, allow_abbrev=False,
                           epilog="Any other flag goes to the job; `python -m "
                                  "gbif_spark job --help` lists them."
                           if name == "all" else None)
        s.add_argument("--gb", type=float, default=2.0,
                       help="how much of the snapshot the fact table should hold, "
                            "in GB (default 2). It is topped up to that; shards "
                            "already in it are never downloaded again")
        s.add_argument("--force", action="store_true",
                       help="redo the key listing and the dimension build even "
                            "when they look up to date")
    for name, text in [("job", "run the nine-stage job (job --help lists its flags)"),
                       ("curate", "the fact table step on its own (curate --help)"),
                       ("registry", "the dimension steps on their own (registry --help)"),
                       ("status", "what has been built and what has been run"),
                       ("test", "run the tests"),
                       ("day", "run one day of weeks/ by its number, e.g. day 5")]:
        sub.add_parser(name, help=text)

    args, extra = p.parse_known_args(argv)
    if args.cmd is None:
        return p.print_help()
    if extra and args.cmd != "all":
        p.error(f"unrecognized arguments: {' '.join(extra)}")
    if args.cmd == "status":
        return cmd_status()
    return cmd_build(args, extra)


if __name__ == "__main__":
    main()
