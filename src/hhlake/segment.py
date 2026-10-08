FIELDS = [
    "source", "vacancy_id", "status", "snapshot_date", "published_at", "area", "employer_id",
    "employer_inn", "employer_ogrn", "employer_name", "name", "role", "sphere", "salary_from",
    "salary_to", "currency", "gross", "experience", "schedule", "employment", "key_skills",
    "description", "url",
]
COL = {name: i for i, name in enumerate(FIELDS)}

ROLE = "Программист, разработчик"
KEYWORD = "php"
REGIONS = ["Москва", "Санкт-Петербург", "Екатеринбург"]

NET_FACTOR = 0.87
MIN_SALARY = 20_000
MAX_SALARY = 1_000_000


def is_variant(row):
    return row[COL["role"]] == ROLE and KEYWORD in row[COL["name"]].lower()


def normalize_skills(raw):
    if not raw:
        return []
    return sorted({token.strip().lower() for token in raw.split(";") if token.strip()})


def _number(value):
    return float(value) if value not in ("", None) else None


def salary_net(row):
    if row[COL["currency"]] != "RUR":
        return None
    low = _number(row[COL["salary_from"]])
    high = _number(row[COL["salary_to"]])
    if low is None and high is None:
        return None
    if low is not None and high is not None:
        mid = (low + high) / 2
    else:
        mid = low if low is not None else high
    if row[COL["gross"]] == "true":
        mid = mid * NET_FACTOR
    return mid if MIN_SALARY <= mid <= MAX_SALARY else None
