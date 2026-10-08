import csv
import io
import re
import sys
from dataclasses import dataclass

from ..segment import COL, FIELDS

csv.field_size_limit(sys.maxsize)

BOUNDARY = re.compile(rb"\n(?=hh,\d+,)")
DATE = re.compile(r"\d{4}-\d{2}-\d{2}")
STATUSES = frozenset({"active", "not_active"})
URL_PREFIX = "https://hh.ru/vacancy/"
TAIL = 64 << 10
MARGIN = 64


def plan(size, chunk):
    return [(start, min(start + chunk, size)) for start in range(0, size, chunk)]


class Window:
    def __init__(self, fetch, size, lo, hi, tail=TAIL):
        self.fetch = fetch
        self.size = size
        self.lo = lo
        self.tail = tail
        self.buf = fetch(lo, min(hi, size))
        self.requests = 1
        self.fetched = len(self.buf)

    @property
    def hi(self):
        return self.lo + len(self.buf)

    def grow(self):
        if self.hi >= self.size:
            return False
        more = self.fetch(self.hi, min(self.hi + self.tail, self.size))
        self.buf += more
        self.requests += 1
        self.fetched += len(more)
        return True


@dataclass
class Split:
    start: int
    end: int
    first: int
    last: int
    data: bytes
    requests: int
    fetched: int


def record_end(buf, pos):
    quotes = 0
    while (newline := buf.find(b"\n", pos)) >= 0:
        quotes += buf.count(b'"', pos, newline)
        if quotes % 2 == 0:
            return newline + 1
        pos = newline + 1
    return None


def parse_record(data):
    try:
        return next(csv.reader(io.StringIO(data.decode("utf-8"), newline="")), None)
    except (UnicodeDecodeError, csv.Error):
        return None


def is_vacancy(row):
    if row is None or len(row) != len(FIELDS):
        return False
    vacancy_id = row[COL["vacancy_id"]]
    return (
        vacancy_id.isascii()
        and vacancy_id.isdigit()
        and row[COL["status"]] in STATUSES
        and DATE.fullmatch(row[COL["snapshot_date"]]) is not None
        and row[COL["url"]] == URL_PREFIX + vacancy_id
    )


def plausible(window, pos):
    offset = pos - window.lo
    while True:
        end = record_end(window.buf, offset)
        settled = end is not None and len(window.buf) - end >= MARGIN
        if settled or not window.grow():
            break
    if end is None:
        end = len(window.buf)
    if not is_vacancy(parse_record(window.buf[offset:end])):
        return False
    return window.lo + end >= window.size or BOUNDARY.match(window.buf, end - 1) is not None


def next_start(window, at):
    pos = max(at - 1 - window.lo, 0)
    while True:
        match = BOUNDARY.search(window.buf, pos)
        if match is None:
            if window.grow():
                continue
            return window.size
        candidate = window.lo + match.end()
        if plausible(window, candidate):
            return candidate
        pos = match.start() + 1


def read_split(fetch, size, start, end, tail=TAIL):
    window = Window(fetch, size, max(start - 1, 0), end + tail, tail)
    first = next_start(window, start)
    last = next_start(window, end) if first < end else first
    data = window.buf[first - window.lo:last - window.lo]
    return Split(start, end, first, last, data, window.requests, window.fetched)


def read_header(fetch, size, tail=TAIL):
    window = Window(fetch, size, 0, tail, tail)
    while (end := record_end(window.buf, 0)) is None and window.grow():
        pass
    end = len(window.buf) if end is None else end
    return parse_record(window.buf[:end]), end


def rows(data):
    return csv.reader(io.TextIOWrapper(io.BytesIO(data), encoding="utf-8", newline=""))
