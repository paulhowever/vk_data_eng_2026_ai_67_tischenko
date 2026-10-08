import argparse
import json
import multiprocessing as mp
import time

MIB = 1 << 20
MODES = ("single", "pool", "chain", "nested")


def hold(mib, seconds):
    block = b"\x01" * (mib * MIB)
    time.sleep(seconds)
    return len(block)


def hold_with_child(mib, seconds):
    child = mp.get_context("spawn").Process(target=hold, args=(mib, seconds))
    child.start()
    hold(mib, seconds)
    child.join()


def run(mode, mib, children, seconds):
    ctx = mp.get_context("spawn")
    if mode == "single":
        hold(mib, seconds)
        return mib
    if mode == "pool":
        procs = [ctx.Process(target=hold, args=(mib, seconds)) for _ in range(children)]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
        return children * mib
    if mode == "chain":
        for _ in range(children):
            proc = ctx.Process(target=hold, args=(mib, seconds))
            proc.start()
            proc.join()
        return mib
    proc = ctx.Process(target=hold_with_child, args=(mib, seconds))
    proc.start()
    proc.join()
    return 2 * mib


def main():
    parser = argparse.ArgumentParser(description="Synthetic memory workload for validating the bench harness")
    parser.add_argument("--mode", choices=MODES, default="single")
    parser.add_argument("--mib", type=int, default=128)
    parser.add_argument("--children", type=int, default=3)
    parser.add_argument("--hold", type=float, default=1.0)
    parser.add_argument("--parent-mib", type=int, default=0)
    parser.add_argument("--exit-code", type=int, default=0)
    args = parser.parse_args()
    started = time.perf_counter()
    ballast = b"\x01" * (args.parent_mib * MIB)
    payload = run(args.mode, args.mib, args.children, args.hold)
    report = {
        "mode": args.mode,
        "mib": args.mib,
        "children": args.children,
        "parent_mib": len(ballast) // MIB,
        "expected_payload_mib": len(ballast) // MIB + payload,
        "exit_code": args.exit_code,
        "time_total": round(time.perf_counter() - started, 3),
    }
    print(json.dumps(report))
    raise SystemExit(args.exit_code)


if __name__ == "__main__":
    main()
