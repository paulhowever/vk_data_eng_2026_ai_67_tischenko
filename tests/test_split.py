import csv
import io
import random
from itertools import accumulate

import pytest

from hhlake.mr import split
from hhlake.segment import FIELDS


def vacancy(number, description, role="Программист, разработчик"):
    vacancy_id = str(100_000_000 + number)
    row = dict.fromkeys(FIELDS, "")
    row.update(
        source="hh",
        vacancy_id=vacancy_id,
        status="active" if number % 2 else "not_active",
        snapshot_date="2024-09-03",
        published_at="2024-09-02",
        area="Москва",
        name=f"PHP-разработчик {number}",
        role=role,
        key_skills="PHP; MySQL;Git",
        description=description,
        url=f"https://hh.ru/vacancy/{vacancy_id}",
    )
    return [row[name] for name in FIELDS]


def render(rows, terminator):
    buf = io.StringIO()
    csv.writer(buf, lineterminator=terminator).writerows(rows)
    return buf.getvalue()


FAKE_PLAIN = render([vacancy(999_001, "ложная запись", role="Тестировщик")], "\n")
FAKE_QUOTED = render([vacancy(999_002, 'ложная, "в кавычках"')], "\n")
TRICKS = [
    "строка\nс переводом",
    "windows\r\nперевод",
    "\nhh,123456789,active,2024-01-01,ложный префикс",
    "\nhh,987654321,",
    '"\n"hh,1,"',
    'кавычки "внутри", и запятые',
    "\n" + FAKE_PLAIN + "хвост после ложной строки",
    "\n" + FAKE_QUOTED,
    "multi\n\n\nline\r\r\n",
    "\u2028 разделители \x85",
    "слон 🐘 " * 40,
    "",
]


def evil_csv(terminator, count, seed=7):
    rng = random.Random(seed)
    rows = [vacancy(i, "".join(rng.sample(TRICKS, rng.randint(0, 3)))) for i in range(count)]
    lines = [render([row], terminator).encode("utf-8") for row in [FIELDS, *rows]]
    starts = list(accumulate(len(line) for line in lines[:-1]))
    data = b"".join(lines)[: -len(terminator)]
    return data, rows, starts


def fetcher(data):
    return lambda start, end: data[start:end]


def collect(data, chunk, tail):
    result = []
    for start, end in split.plan(len(data), chunk):
        piece = split.read_split(fetcher(data), len(data), start, end, tail)
        assert piece.first >= start
        assert piece.first >= end or piece.first < end <= piece.last
        result.extend(split.rows(piece.data))
    return result


@pytest.fixture(scope="module", params=["\n", "\r\n"], ids=["lf", "crlf"])
def terminator(request):
    return request.param


@pytest.fixture(scope="module")
def dataset(terminator):
    data, rows, starts = evil_csv(terminator, 240)
    reference = list(csv.reader(io.StringIO(data.decode("utf-8"), newline="")))
    assert reference == [FIELDS, *rows]
    assert not data.endswith(b"\n")
    return data, rows, starts


def test_dataset_contains_fake_boundaries(dataset):
    data, rows, starts = dataset
    candidates = [match.end() for match in split.BOUNDARY.finditer(data)]
    assert set(starts) < set(candidates)


@pytest.mark.parametrize("pieces", [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377])
@pytest.mark.parametrize("tail", [1, 7, 64, split.TAIL])
def test_chunks_reassemble_full_parse(dataset, pieces, tail):
    data, rows, _ = dataset
    chunk = -(-len(data) // pieces)
    assert collect(data, chunk, tail) == rows


@pytest.mark.parametrize("chunk", [1, 17, 100, 1000])
def test_tiny_chunks(terminator, chunk):
    data, rows, _ = evil_csv(terminator, 30, seed=11)
    assert collect(data, chunk, 5) == rows


def test_split_exactly_at_record_starts(dataset):
    data, rows, starts = dataset
    size = len(data)
    for position in starts:
        for tail in (1, 2, 3):
            left = split.read_split(fetcher(data), size, 0, position, tail)
            right = split.read_split(fetcher(data), size, position, size, tail)
            assert left.last == position == right.first
            assert list(split.rows(left.data)) + list(split.rows(right.data)) == rows


def test_fake_boundaries_are_rejected(dataset):
    data, _, starts = dataset
    real = set(starts)
    for match in split.BOUNDARY.finditer(data):
        position = match.end()
        window = split.Window(fetcher(data), len(data), position - 1, position + 1, 3)
        assert split.plausible(window, position) == (position in real)


def test_header_is_skipped(dataset):
    data, rows, starts = dataset
    fields, end = split.read_header(fetcher(data), len(data), 3)
    assert fields == FIELDS
    assert end == starts[0]
    whole = split.read_split(fetcher(data), len(data), 0, len(data))
    assert whole.first == starts[0]
    assert whole.last == len(data)
