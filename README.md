# MapReduce vs DuckDB на RustFS: навыки и зарплаты PHP-разработчиков

Домашнее задание 3, вариант «Программист, разработчик на PHP». Датасет IT-вакансий hh.ru за 2024 год
(`it_vacancies_2024.csv`, 847 619 384 Б) лежит в объектном хранилище RustFS. Собственный MapReduce на Python и
DuckDB считают по нему частоты навыков, пар и троек навыков. Потом по этим частотам считаются зарплатные прибавки
и сравниваются три региона. Любое вычисление читает данные только из S3-бакета `hh-lake`.

Отчёт с выводами, таблицами и графиками: [`report/report.pdf`](report/report.pdf), исходник `report/report.tex`.
Выводы всех прогонов, на которые ссылается отчёт, лежат в `report/logs/`.

## Требования

- Docker с Compose v2;
- [uv](https://docs.astral.sh/uv/), Python 3.11+ (uv поставит его сам);
- [mc](https://min.io/docs/minio/linux/reference/minio-mc.html) (MinIO Client);
- локальная копия `it_vacancies_2024.csv`, она нужна только для загрузки в бакет.

## Настройка

```bash
cp .env.example .env
```

В `.env` замените заглушки на случайные ключи (например, `openssl rand -hex 20`) и укажите путь к датасету в
`DATASET_PATH`. Файл `.env` в git не попадает.

```bash
docker compose up -d
docker compose ps
mc alias set hh http://localhost:19000 "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY"
uv sync
```

Получается один узел RustFS с 4 «дисками» `.data/disk1..4`, это erasure set EC 2+2. S3 API доступен на порту
19000, веб-консоль на порту 19001. Порты 9000/9001 на этой машине заняты другим проектом.

## Запуск по шагам

Все команды выполняются из корня репозитория. Каждый шаг — модуль `python -m hhlake.<имя>`, поэтому отдельных
скриптов `scripts/01…07` нет.

### Загрузка и хранилище

```bash
uv run python -m hhlake.upload
uv run python -m hhlake.storage
```

`upload` создаёт бакет, загружает CSV по частям (multipart) и сравнивает sha256 локального файла с sha256 объекта,
прочитанного обратно из S3. ETag объекта из частей не является MD5 содержимого, поэтому целостность проверяется по
sha256. `DATASET_PATH` читается только этим модулем.

`storage` печатает `docker compose ps`, `mc stat`, `mc ls -r`, `mc du`, размеры `.data/disk*`, долю объекта на
каждом диске (0,5 на диск, 2,0× суммарно, то есть EC 2+2), sha256 объекта через `mc cat` и через boto3, а также
пропускную способность чтения из S3 при 1–10 параллельных запросах.

### MapReduce

```bash
uv run python -m hhlake.mr.run --k 1 2 3
uv run python -m hhlake.mr.run --k 1 2 3 --regions
uv run python -m hhlake.mr.naive
```

Объект режется на чанки по 32 МиБ (26 map-задач). Каждый маппер делает один Range GET со «хвостом» 64 КиБ и сам
выравнивает границы своего диапазона по началу записи. Combiner работает внутри маппера, shuffle идёт через
`tmp/mr/<job>/` в S3 с партиционированием по crc32, reducer'ов 4. После задания промежуточные файлы удаляются.
Полезные флаги: `--workers`, `--chunk-mib`, `--reducers`, `--no-write`, `--job`, `--keep-tmp`.
`mr.naive` — версия v1 для бонуса: один процесс, потоковое чтение, без combiner'а.

Отказ маппера: процесс маппера 7 завершается через `os._exit(137)` на первой попытке.

```bash
uv run python -m hhlake.mr.run --k 1 2 3 --fail-mapper 7 --fail-attempt 1 --keep-tmp --job faulttest
mc ls -r hh/hh-lake/tmp/mr/faulttest/
mc rm -r --force hh/hh-lake/tmp/mr/faulttest/
```

С флагом `--keep-tmp` задание само печатает содержимое `tmp/mr/faulttest/`: какие попытки приняты по
`_manifest.json`, а какие остались «сиротами» от упавших и снятых попыток. Перед записью результата sha256 каждого
CSV сравнивается с объектом, который уже лежит в S3 после прогона без сбоя.

### DuckDB

```bash
uv run python -m hhlake.duck --k 1 2 3
uv run python -m hhlake.duck --k 1 2 3 --materialize
uv run python -m hhlake.duck --k 1 2 3 --regions
uv run python -m hhlake.duck --explain
uv run python -m hhlake.duck --to-parquet
uv run python -m hhlake.duck --source parquet --k 1 2 3 --no-write
```

По умолчанию CSV читается из S3 заново для каждого k. С `--materialize` вакансии варианта один раз читаются во
временную таблицу `(vacancy_id, area, skill)`, и все k считаются из неё. `--explain` печатает `EXPLAIN ANALYZE` и
HTTP-журнал httpfs: 1 HEAD, 27 GET, 808 МиБ, то есть объект прочитан из S3 целиком и ровно один раз.
`--to-parquet` пишет `raw/it_vacancies_2024.parquet` (zstd) для бонуса.

### Сверка, рост числа сочетаний, анализ

```bash
uv run python -m hhlake.verify --naive
uv run python -m hhlake.growth
uv run python -m hhlake.salary
uv run python -m hhlake.regions
uv run python -m hhlake.nolocal
```

- `verify` читает результаты MapReduce, наивного MapReduce и DuckDB из S3 и делает `EXCEPT ALL` в обе стороны
  для k=1,2,3 и региональных таблиц. Рядом печатается таблица контрольных чисел.
- `growth` строит распределение числа навыков на вакансию и таблицу «n → вакансий, ΣC(n,2), ΣC(n,3), доля»,
  а также рост по k: эмиты, combiner, уникальные ключи, shuffle, время и память из бенча.
  Пишет `results/growth/` и `report/img/growth.png`.
- `salary` считает прибавки пар и троек навыков и пишет `results/salary/*.csv`.
- `regions` строит профиль трёх регионов и топ сочетаний по региональному заданию MapReduce, пишет
  `results/regions/*.csv`.
- `nolocal` временно переименовывает локальный датасет, повторяет `mr.run`, `duck`, `verify`, `salary`,
  `regions` и возвращает файл на место.

### Бенчмарк

```bash
uv run python -m hhlake.bench --suite all --runs 5 --restart-storage
```

Наборы сценариев: `selftest` (проверка самого измерителя на синтетической нагрузке), `main` (MapReduce и DuckDB
по k), `scaling` (число воркеров и размер чанка), `bonus` (до/после), `all` = main + scaling + bonus. Каждый
сценарий запускается отдельным процессом: 1 холодный прогон и `--runs` тёплых. Измеряются время и пик RSS всего
дерева процессов (psutil, опрос раз в 20 мс), снимается `docker stats` для RustFS. Результаты пишутся в
`results/bench/{runs,summary,timeline,docker_stats}.csv` (в S3 и локально), графики — в `report/img/bench_*.png`.
`--restart-storage` перезапускает контейнер RustFS перед каждым сценарием. Без этого память RustFS растёт от
прогона к прогону до предела VM Docker (7,65 ГиБ), и контейнер убивает OOM (`report/logs/bench_oom.txt`).
`--list` печатает сценарии, `--plot-only` перерисовывает графики по сохранённым таблицам.

### Тесты

```bash
uv run pytest
```

Тесты покрывают: нарезку на «злом» синтетическом CSV (переводы строк и ложные границы внутри полей, CRLF,
крошечные чанки), стабильность crc32-партиционирования между spawn-процессами, shuffle без потерь, повтор упавшего
маппера и учёт попыток, правила варианта, нормализации и зарплаты в Python и SQL, а также измеритель памяти бенча.

## Справка

### Бакет `hh-lake`

```
raw/it_vacancies_2024.csv               исходный датасет
raw/it_vacancies_2024.parquet           копия в Parquet для бонуса
results/mapreduce/k{1,2,3}.csv          skill1[,skill2[,skill3]],count
results/mapreduce/regions_k{1,2,3}.csv  area,skill1..k,count
results/mapreduce_naive/k{1,2,3}.csv    MapReduce v1
results/duckdb/k{1,2,3}.csv, regions_k{1,2,3}.csv
results/salary/{pairs,triples}{,_up,_down}.csv, singles.csv
results/regions/profile.csv, top_combos.csv
results/growth/skills_per_vacancy.csv
results/bench/{runs,summary,timeline,docker_stats}.csv
tmp/mr/<job>/map-<i>-a<attempt>/part-<r>, tmp/mr/<job>/_manifest.json   промежуточные, удаляются после задания
```

Локальные копии итоговых таблиц лежат в `results/` с той же структурой. Таблицы сочетаний отсортированы по
`count` по убыванию, при равенстве — по навыкам по возрастанию. Региональные таблицы сначала отсортированы по `area`.

### Правила

- Вариант: `role = 'Программист, разработчик' AND name ILIKE '%php%'` (в Python — `segment.is_variant`).
- Навыки: `key_skills` делится по `;`, каждый токен обрезается по краям и приводится к нижнему регистру, пустые
  отбрасываются, повторы внутри вакансии убираются, список сортируется. Сочетания k=1,2,3 — сортированные кортежи
  (`itertools.combinations` по отсортированному списку; в SQL — self-join с условием `s1.skill < s2.skill < s3.skill`).
  Синонимы (php7/php8, js/javascript) не склеиваются.
- Зарплата: только `currency = 'RUR'`; середина вилки или та граница, что есть; при `gross = true` умножается на
  0,87; остаются значения от 20 000 до 1 000 000.
- Прибавка пары = медиана(A∩B) − медиана(A), где A — более частый навык пары по `results/mapreduce/k1.csv`.
  Прибавка тройки считается от самой частой пары внутри неё (по `k2.csv`); контрольная колонка — от самого
  частого навыка. Порог поддержки: не меньше 30 вакансий с зарплатой и не меньше 10 разных работодателей.
- Регионы: Москва, Санкт-Петербург, Екатеринбург.

### Контрольные числа

| | значение |
|---|---|
| записей в CSV | 229 388 |
| вакансий варианта | 5 382 |
| из них с навыками | 4 380 |
| k=1 | 955 ключей, Σcount 31 835, топ `php` 4 033 |
| k=2 | 15 921 ключ, Σcount 135 362, топ `mysql + php` 2 096 |
| k=3 | 127 466 ключей, Σcount 472 071, топ `git + mysql + php` 1 030 |

### Код

```
src/hhlake/config.py     endpoint, бакет, ключи из .env, раскладка ключей
src/hhlake/s3.py         клиент boto3, Range GET, put/get, sha256
src/hhlake/segment.py    правила варианта, нормализации навыков и зарплаты
src/hhlake/upload.py     загрузка датасета и сверка sha256
src/hhlake/storage.py    доказательства по хранилищу и замер чтения из S3
src/hhlake/mr/split.py   нарезка объекта и выравнивание границ записей
src/hhlake/mr/engine.py  мастер: пул процессов, повторы, манифест, инъекция сбоя
src/hhlake/mr/jobs.py    map / combine / partition / reduce
src/hhlake/mr/run.py     CLI задания MapReduce и проверки инвариантов
src/hhlake/mr/naive.py   MapReduce v1 для бонуса
src/hhlake/duck.py       DuckDB: SQL варианта, сочетания, COPY в S3, EXPLAIN, Parquet
src/hhlake/verify.py     сверка MapReduce и DuckDB
src/hhlake/growth.py     рост числа сочетаний с k
src/hhlake/salary.py     зарплатные прибавки
src/hhlake/regions.py    сравнение регионов
src/hhlake/nolocal.py    прогон без локального файла
src/hhlake/bench.py      профилирование
src/hhlake/memprobe.py   синтетическая нагрузка для проверки измерителя
```
