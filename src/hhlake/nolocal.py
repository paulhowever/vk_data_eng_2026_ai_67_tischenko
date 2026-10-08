import argparse
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path

from . import config

RUNS = (
    ("hhlake.mr.run", "--k", "1", "--no-write"),
    ("hhlake.duck", "--k", "1", "--no-write"),
    ("hhlake.verify", "--naive"),
    ("hhlake.salary",),
    ("hhlake.regions",),
)
SKIP = ("map    task", "reduce task", "{")


def short(path):
    home = str(Path.home())
    return str(path).replace(home, "~", 1)


def usages():
    found = []
    for path in sorted((config.ROOT / "src").rglob("*.py")):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "DATASET_PATH" in line:
                found.append(f"{path.relative_to(config.ROOT)}:{number}: {line.strip()}")
    return found


def run(module, *args, tail):
    command = ["python", "-m", module, *args]
    print(f"\n$ uv run {shlex.join(command)}", flush=True)
    started = time.perf_counter()
    result = subprocess.run(
        [sys.executable, "-m", module, *args], cwd=config.ROOT, capture_output=True, text=True, check=False
    )
    lines = [line for line in (result.stdout + result.stderr).splitlines() if not line.startswith(SKIP)]
    if len(lines) > tail:
        print(f"... {len(lines) - tail} lines skipped ...")
    print("\n".join(lines[-tail:]))
    print(f"exit code {result.returncode}, {time.perf_counter() - started:.1f} s", flush=True)
    return result.returncode


def parse_args(argv=None):
    parser = argparse.ArgumentParser(prog="python -m hhlake.nolocal",
                                     description="Hide the local dataset and rerun every S3 computation")
    parser.add_argument("--tail", type=int, default=12)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    source = config.DATASET_PATH
    hidden = source.with_name(source.name + ".hidden")
    if not source.exists():
        raise SystemExit(f"{source} does not exist, nothing to hide")
    if hidden.exists():
        raise SystemExit(f"{hidden} already exists")
    print("check: every computation reads the dataset from S3, the local copy is hidden for the whole run")
    print("\n$ grep -rn DATASET_PATH src/")
    print("\n".join(usages()))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))
    codes = []
    try:
        source.rename(hidden)
        print(f"\n$ mv {short(source)} {short(hidden)}")
        print(f"$ test -e {short(source)} -> {'exists' if source.exists() else 'no such file'}")
        codes = [run(*item, tail=args.tail) for item in RUNS]
    finally:
        if hidden.exists() and not source.exists():
            hidden.rename(source)
        print(f"\n$ mv {short(hidden)} {short(source)}")
        print(f"$ test -e {short(source)} -> {'exists' if source.exists() else 'no such file'}")
    ok = len(codes) == len(RUNS) and not any(codes)
    print(f"\n{len(RUNS)} runs without the local file, exit codes {codes}: {'all ok' if ok else 'FAILED'}")
    if not ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
