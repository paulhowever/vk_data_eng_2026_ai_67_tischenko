import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load_env(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env(ROOT / ".env")

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:19000")
BUCKET = os.environ.get("S3_BUCKET", "hh-lake")
ACCESS_KEY = os.environ.get("RUSTFS_ACCESS_KEY", "")
SECRET_KEY = os.environ.get("RUSTFS_SECRET_KEY", "")
REGION = "us-east-1"
DATASET_PATH = Path(os.path.expanduser(os.environ.get("DATASET_PATH", "~/Downloads/it_vacancies_2024.csv")))

RAW_KEY = "raw/it_vacancies_2024.csv"
PARQUET_KEY = "raw/it_vacancies_2024.parquet"
RESULTS_MR = "results/mapreduce"
RESULTS_DUCK = "results/duckdb"
RESULTS_SALARY = "results/salary"
RESULTS_REGIONS = "results/regions"
RESULTS_BENCH = "results/bench"
TMP_MR = "tmp/mr"

LOCAL_RESULTS = ROOT / "results"


def duck_endpoint():
    return ENDPOINT.split("://", 1)[-1]


def s3_url(key):
    return f"s3://{BUCKET}/{key}"
