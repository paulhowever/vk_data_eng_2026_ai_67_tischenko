import sys

import duckdb

from hhlake import config, s3

REFERENCE = config.ROOT / "results" / "mapreduce" / "k2.csv"


def compare(key, target):
    data = s3.get_bytes(key)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    con = duckdb.connect()
    con.execute(f"CREATE TABLE ref AS SELECT * FROM read_csv('{REFERENCE}', header=true, all_varchar=false)")
    con.execute(f"CREATE TABLE k8s AS SELECT * FROM read_csv('{target}', header=true, all_varchar=false)")
    rows = {name: con.execute(f"SELECT count(*), sum(count) FROM {name}").fetchone() for name in ("ref", "k8s")}
    only_k8s = con.execute("SELECT count(*) FROM (SELECT * FROM k8s EXCEPT ALL SELECT * FROM ref)").fetchone()[0]
    only_ref = con.execute("SELECT count(*) FROM (SELECT * FROM ref EXCEPT ALL SELECT * FROM k8s)").fetchone()[0]
    top = con.execute("SELECT skill1 || '+' || skill2, count FROM k8s ORDER BY count DESC, skill1, skill2 LIMIT 10").fetchall()
    print(f"{config.s3_url(key)} -> {target.relative_to(config.ROOT)}")
    print(f"  reference {REFERENCE.relative_to(config.ROOT)}: rows {rows['ref'][0]:,} sum {rows['ref'][1]:,}")
    print(f"  kubernetes: rows {rows['k8s'][0]:,} sum {rows['k8s'][1]:,}")
    print(f"  k8s EXCEPT ALL ref: {only_k8s} rows")
    print(f"  ref EXCEPT ALL k8s: {only_ref} rows")
    print("  top-10: " + ", ".join(f"{pair} {count}" for pair, count in top))
    return only_k8s == 0 and only_ref == 0


def main():
    print(f"endpoint {config.ENDPOINT}")
    ok = True
    for run in sys.argv[1:] or ["indexed", "full"]:
        ok &= compare(f"results/k8s/{run}/k2.csv", config.ROOT / "hw5" / "results" / run / "k2.csv")
    print("MATCH" if ok else "MISMATCH")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
