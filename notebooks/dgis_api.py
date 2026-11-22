"""Клиент Places API 2ГИС для ноутбуков 03_2gis_gap_fill, 03_2gis_enrich и 03_2gis_all_coffee.

`collect_area` собирает все заведения нужных рубрик в области, деля её на квадраты (см. раздел ниже).

`enrich` догружает поля уже найденных организаций через /3.0/items/byid, до 100 ID за запрос.
Ответы хранятся в SQLite целиком, поэтому разбор можно повторять без новых запросов.
"""
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import timezone
from email.utils import parsedate_to_datetime
import json
import math
import os
from pathlib import Path
import random
import re
import sqlite3
import time

import requests

try:  # блокировка файла прогресса: fcntl на macOS/Linux, msvcrt на Windows
    import fcntl
except ImportError:
    fcntl = None
    import msvcrt

PLACES_URL = "https://catalog.api.2gis.com/3.0/items"
BYID_URL = "https://catalog.api.2gis.com/3.0/items/byid"
BYID_MAX_IDS = 100  # предел /3.0/items/byid по документации


class CollectionStopped(RuntimeError):
    """Остановка, после которой сохранённый прогресс можно использовать снова."""


# ---------------------------------------------------------------- общий HTTP-слой

def read_api_key(project_root):
    """Ключ из переменной окружения DGIS_API_KEY или строки DGIS_API_KEY=... в .env в корне проекта."""
    key = os.environ.get("DGIS_API_KEY", "").strip()
    env_file = Path(project_root) / ".env"
    if not key and env_file.is_file():
        for line in env_file.read_text(encoding="utf-8-sig").splitlines():
            name, sep, value = line.strip().partition("=")
            if sep and name.strip().removeprefix("export ").strip() == "DGIS_API_KEY":
                key = value.strip().strip("'\"")
    if not key:
        raise ValueError("Нет ключа: задайте DGIS_API_KEY в окружении или строкой DGIS_API_KEY=... в .env в корне проекта.")
    return key


def retry_after_seconds(value, now):
    """Retry-After может содержать число секунд или дату HTTP."""
    if not value:
        return 0
    value = value.strip()
    try:
        if value.isascii() and value.isdecimal():
            return int(value) if len(value) <= 12 else math.inf
        retry_at = parsedate_to_datetime(value)
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0, retry_at.timestamp() - now)
    except (TypeError, ValueError, OverflowError):
        return 0


class RequestBudget:
    """Лимит всех попыток и самое раннее время следующего запроса."""

    def __init__(self, db, limit, interval, *, now=time.time, sleep=time.sleep):
        self.db = db
        self.limit = limit
        self.interval = interval
        self.now = now
        self.sleep = sleep

    def defer(self, deadline):
        with self.db:
            self.db.execute("UPDATE run SET next_at = max(next_at, ?)", (deadline,))

    def reserve(self):
        attempts, next_at = self.db.execute("SELECT attempts, next_at FROM run").fetchone()
        if attempts >= self.limit:
            raise CollectionStopped("Достигнут суммарный лимит HTTP-попыток.")
        delay = next_at - self.now()
        if delay > 60:
            raise CollectionStopped("Пауза после временной ошибки ещё действует.")
        while delay > 0:
            self.sleep(delay)
            delay = next_at - self.now()
        # Списываем попытку до отправки. При прерывании счётчик консервативен.
        with self.db:
            self.db.execute("UPDATE run SET attempts = attempts + 1, next_at = ?", (self.now() + self.interval,))


def _server_message(error, key):
    """Текст ошибки 2ГИС для пользователя: не длиннее 200 символов и без ключа."""
    message = error.get("message") if isinstance(error, dict) else None
    if not isinstance(message, str) or not message.strip():
        return ""
    message = message.strip()[:200]
    if key:
        message = message.replace(key, "***")
    return f" {message}"


def get_with_retry(get, params, budget, max_attempts=3, url=PLACES_URL):
    """Проверяет HTTP и meta.code, возвращает JSON успешного ответа."""
    for attempt in range(max_attempts):
        budget.reserve()
        retry_after = None
        try:
            response = get(url, params=params, timeout=(5, 15), allow_redirects=False)
        except (requests.exceptions.SSLError, requests.exceptions.ProxyError):
            raise CollectionStopped("Ошибка TLS или прокси.") from None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            reason = "временный сетевой сбой"
        except requests.exceptions.RequestException:
            raise CollectionStopped("Ошибка при отправке HTTP-запроса.") from None
        else:
            status = response.status_code
            retry_after = response.headers.get("Retry-After")
            if status == 200:
                try:
                    data = response.json()
                except ValueError:
                    raise CollectionStopped("Ответ 2ГИС не является JSON.") from None
                meta = data.get("meta") if isinstance(data, dict) else None
                code = meta.get("code") if isinstance(meta, dict) else None
                if type(code) is not int:
                    raise CollectionStopped("В ответе 2ГИС отсутствует числовой meta.code.")
                if code == 200:
                    return data
                error = meta.get("error")
                if code == 404 and isinstance(error, dict) and error.get("type") == "itemNotFound":
                    return {"result": {"items": [], "total": 0}}
                if code not in (408, 429) and not 500 <= code <= 599:
                    raise CollectionStopped(f"2ГИС вернул meta.code {code}.{_server_message(error, params.get('key'))}")
                reason = f"meta.code {code}"
            else:
                if status not in (408, 429) and not 500 <= status <= 599:
                    raise CollectionStopped(f"2ГИС вернул HTTP {status}.")
                reason = f"HTTP {status}"
        now = budget.now()
        server_delay = retry_after_seconds(retry_after, now)
        delay = max(server_delay, 2 ** attempt + random.uniform(0, 0.5))
        budget.defer(now + delay)
        if attempt + 1 == max_attempts or delay > 60:
            raise CollectionStopped(f"Пауза в сборе 2ГИС ({reason}).")


RUN_TABLE = """
CREATE TABLE IF NOT EXISTS run (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    plan TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_at REAL NOT NULL DEFAULT 0
);
"""


@contextmanager
def file_lock(path):
    """Один процесс на файл прогресса. Блокировка снимается при выходе или завершении процесса."""
    with Path(path).open("a+") as lock:
        try:
            if fcntl is not None:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            else:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            raise CollectionStopped("Этот файл прогресса уже используется.") from None
        try:
            yield
        finally:
            if fcntl is None:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


# ---------------------------------------------------------------- поиск по кругам (как в 03_2gis_data_safe)

@dataclass(frozen=True)
class Search:
    query: str = "кофейня"
    radius_m: int = 1000
    page_size: int = 10
    max_pages: int = 5

    def __post_init__(self):
        if (not isinstance(self.query, str) or not self.query.strip()
                or type(self.radius_m) is not int or not 1 <= self.radius_m <= 50_000):
            raise ValueError("Проверьте текст поиска и радиус.")
        if (type(self.page_size) is not int or not 1 <= self.page_size <= 50
                or type(self.max_pages) is not int or not 1 <= self.max_pages <= 1_000_000):
            raise ValueError("Проверьте размер страницы и предел страниц.")

    def params(self):
        return {
            "q": self.query, "type": "branch", "radius": self.radius_m,
            "page_size": self.page_size, "fields": "items.point,items.reviews",
        }


def page_items(data, page_size):
    result = data.get("result")
    total = result.get("total") if isinstance(result, dict) else None
    if type(total) is not int or total < 0:
        raise CollectionStopped("В ответе 2ГИС отсутствует корректный result.total.")
    items = result.get("items", [] if total == 0 else None)
    if not isinstance(items, list) or len(items) > min(total, page_size):
        raise CollectionStopped("Некорректный список result.items.")
    for item in items:
        item_id = item.get("id") if isinstance(item, dict) else None
        if not isinstance(item_id, str) or not item_id.strip():
            raise CollectionStopped("В result.items есть объект без строкового ID.")
    return total, items


SCHEMA = RUN_TABLE + """
CREATE TABLE IF NOT EXISTS centers (
    point TEXT PRIMARY KEY,
    next_page INTEGER NOT NULL DEFAULT 1,
    expected_total INTEGER,
    status TEXT NOT NULL DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS center_items (
    point TEXT NOT NULL,
    organization_id TEXT NOT NULL,
    PRIMARY KEY (point, organization_id)
);
CREATE TABLE IF NOT EXISTS organizations (
    id TEXT PRIMARY KEY,
    name TEXT,
    lon REAL,
    lat REAL,
    general_rating REAL,
    general_review_count INTEGER
);
"""


@contextmanager
def open_checkpoint(path, schema=SCHEMA):
    path = Path(path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    with file_lock(path.with_suffix(path.suffix + ".lock")):
        with closing(sqlite3.connect(path)) as db:
            db.executescript(schema)
            yield db


def prepare_checkpoint(db, points, search):
    plan = json.dumps(
        {"version": 1, "url": PLACES_URL, "params": search.params(),
         "max_pages": search.max_pages, "points": points},
        ensure_ascii=False, sort_keys=True,
    )
    existing = db.execute("SELECT plan FROM run").fetchone()
    if existing and existing[0] != plan:
        raise CollectionStopped("План поиска изменился. Выберите новый файл прогресса.")
    if not existing:
        with db:
            db.execute("INSERT INTO run (id, plan) VALUES (1, ?)", (plan,))
            db.executemany("INSERT INTO centers (point) VALUES (?)", [(p,) for p in points])


def organization_row(item):
    point = item.get("point")
    lon, lat = (point.get("lon"), point.get("lat")) if isinstance(point, dict) else (None, None)
    if not (type(lon) in (int, float) and -180 <= lon <= 180 and type(lat) in (int, float) and -90 <= lat <= 90):
        lon, lat = None, None
    reviews = item.get("reviews")
    reviews = reviews if isinstance(reviews, dict) else {}
    rating = reviews.get("general_rating")
    count = reviews.get("general_review_count")
    return (
        item["id"], item.get("name") if isinstance(item.get("name"), str) else None,
        lon, lat,
        rating if type(rating) in (int, float) and 0 <= rating <= 1e6 else None,
        count if type(count) is int and 0 <= count < 2**63 else None,
    )


def fetch_coffee_shops(db, point, search, key, get, budget):
    page, expected_total, status = db.execute(
        "SELECT next_page, expected_total, status FROM centers WHERE point = ?", (point,)
    ).fetchone()
    while status == "pending":
        params = dict(search.params(), key=key, point=point, page=page)
        total, items = page_items(get_with_retry(get, params, budget), search.page_size)
        # Если выдача меняется во время сбора, учитываем наибольший заявленный total.
        expected_total = max(total, expected_total or 0)
        # Записи страницы и курсор фиксируются одной транзакцией.
        with db:
            db.executemany(
                """INSERT INTO organizations VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       name = coalesce(excluded.name, organizations.name),
                       lon = coalesce(excluded.lon, organizations.lon),
                       lat = coalesce(excluded.lat, organizations.lat),
                       general_rating = coalesce(excluded.general_rating, organizations.general_rating),
                       general_review_count = coalesce(excluded.general_review_count,
                                                       organizations.general_review_count)""",
                map(organization_row, items),
            )
            db.executemany("INSERT OR IGNORE INTO center_items VALUES (?, ?)", ((point, item["id"]) for item in items))
            seen = db.execute("SELECT count(*) FROM center_items WHERE point = ?", (point,)).fetchone()[0]
            if seen >= expected_total:
                status = "complete"
            elif not items or page * search.page_size >= expected_total:
                status = "short"
            elif page >= search.max_pages:
                status = "capped"
            db.execute(
                "UPDATE centers SET next_page = ?, expected_total = ?, status = ? WHERE point = ?",
                (page + 1, expected_total, status, point),
            )
        page += 1


def checkpoint_summary(db):
    statuses = {status: 0 for status in ("pending", "complete", "capped", "short")}
    statuses.update(db.execute("SELECT status, count(*) FROM centers GROUP BY status"))
    attempts = db.execute("SELECT attempts FROM run").fetchone()[0]
    total, missing = db.execute("SELECT count(*), count(*) - count(lon) FROM organizations").fetchone()
    return {"attempts": attempts, "centers": statuses, "organizations": total, "without_coordinates": missing}


def collect(points, search, checkpoint, *, key, max_requests_total, center_limit, get,
            interval=1.0, now=time.time, sleep=time.sleep):
    points = tuple(dict.fromkeys(points))
    if not key or not points:
        raise ValueError("Для сбора нужны ключ и центры поиска.")
    if type(max_requests_total) is not int or max_requests_total < 1:
        raise ValueError("Укажите положительный суммарный лимит HTTP-попыток.")
    if type(center_limit) is not int or not 1 <= center_limit <= len(points):
        raise ValueError("Укажите число центров от 1 до размера сетки.")
    if not math.isfinite(interval) or not 1 <= interval <= 60:
        raise ValueError("Пауза между запросами должна составлять от 1 до 60 секунд.")
    with open_checkpoint(checkpoint) as db:
        prepare_checkpoint(db, points, search)
        budget = RequestBudget(db, max_requests_total, interval, now=now, sleep=sleep)
        stop_reason = None
        for index, point in enumerate(points[:center_limit], start=1):
            try:
                fetch_coffee_shops(db, point, search, key, get, budget)
            except CollectionStopped as error:
                stop_reason = f"Центр {index}/{len(points)}. {error}"
                break
        return checkpoint_summary(db) | {"stop_reason": stop_reason}


# ---------------------------------------------------------------- дозагрузка полей по ID (/3.0/items/byid)

ENRICH_FIELDS = (
    "items.point",             # координаты (свежие)
    "items.reviews",           # рейтинг и число отзывов
    "items.rubrics",           # рубрики: настоящая кофейня или кондитерская/ресторан
    "items.org",               # организация и число её филиалов (сеть или одиночка)
    "items.brand",             # бренд и число его филиалов
    "items.schedule",          # часы работы: открытие утром, 24/7, выходные
    "items.flags",             # в т.ч. «филиал временно не работает»
    "items.address",           # адрес; название здания приходит не всегда
    "items.attribute_groups",  # атрибуты: средний чек, число мест, кофе с собой, Wi-Fi, «можно с ноутбуком»
    "items.dates",             # даты внесения и изменения карточки
)

ENRICH_SCHEMA = RUN_TABLE + """
CREATE TABLE IF NOT EXISTS batches (
    batch_no INTEGER PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS batch_ids (
    id TEXT PRIMARY KEY,
    batch_no INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS items (
    id TEXT PRIMARY KEY,
    batch_no INTEGER NOT NULL,
    json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS missing (
    id TEXT PRIMARY KEY,
    batch_no INTEGER NOT NULL
);
"""


def base_id(item_id):
    """ID филиала без хеша: '70000001012345678_abc…' → '70000001012345678'."""
    return str(item_id).strip().split("_")[0]


def check_ids(ids):
    ids = list(dict.fromkeys(base_id(i) for i in ids))
    bad = [i for i in ids if not (i.isascii() and i.isdigit())]
    if bad:
        raise ValueError(f"Некорректные ID 2ГИС (ожидаются цифры): {bad[:3]}")
    if not ids:
        raise ValueError("Нет ID для дозагрузки.")
    return ids


def prepare_enrichment(db, ids, fields, batch_size):
    """Новые ID дописываются новыми пакетами; уже запрошенные ID повторно не запрашиваются."""
    plan = json.dumps({"version": 1, "url": BYID_URL, "fields": list(fields)}, sort_keys=True)
    existing = db.execute("SELECT plan FROM run").fetchone()
    if existing and existing[0] != plan:
        raise CollectionStopped("Список полей изменился. Выберите новый файл прогресса.")
    known = {row[0] for row in db.execute("SELECT id FROM batch_ids")}
    new_ids = sorted((i for i in ids if i not in known), key=lambda s: (len(s), s))
    next_no = (db.execute("SELECT max(batch_no) FROM batches").fetchone()[0] or 0) + 1
    with db:
        if not existing:
            db.execute("INSERT INTO run (id, plan) VALUES (1, ?)", (plan,))
        for start in range(0, len(new_ids), batch_size):
            db.execute("INSERT INTO batches (batch_no) VALUES (?)", (next_no,))
            db.executemany("INSERT INTO batch_ids VALUES (?, ?)", [(i, next_no) for i in new_ids[start:start + batch_size]])
            next_no += 1
    return len(new_ids)


def byid_items(data):
    result = data.get("result")
    items = result.get("items", []) if isinstance(result, dict) else None
    if not isinstance(items, list):
        raise CollectionStopped("Некорректный список result.items.")
    for item in items:
        item_id = item.get("id") if isinstance(item, dict) else None
        if not isinstance(item_id, str) or not item_id.strip():
            raise CollectionStopped("В result.items есть объект без строкового ID.")
    return items


def fetch_batch(db, batch_no, fields, key, get, budget):
    ids = [row[0] for row in db.execute("SELECT id FROM batch_ids WHERE batch_no = ? ORDER BY id", (batch_no,))]
    params = {"id": ",".join(ids), "fields": ",".join(fields), "key": key}
    items = byid_items(get_with_retry(get, params, budget, url=BYID_URL))
    if not items and len(ids) > 1:
        # Документация не говорит, что 2ГИС делает с ненайденным ID в пакете. Если пакет пришёл пустым,
        # делим его пополам: один удалённый филиал не должен скрыть остальные.
        half = len(ids) // 2
        next_no = db.execute("SELECT max(batch_no) FROM batches").fetchone()[0] + 1
        with db:
            for offset, part in enumerate((ids[:half], ids[half:])):
                db.execute("INSERT INTO batches (batch_no) VALUES (?)", (next_no + offset,))
                db.executemany("UPDATE batch_ids SET batch_no = ? WHERE id = ?", [(next_no + offset, i) for i in part])
            db.execute("UPDATE batches SET status = 'split' WHERE batch_no = ?", (batch_no,))
        return
    returned = {base_id(item["id"]): item for item in items}
    with db:
        db.executemany(
            """INSERT INTO items VALUES (?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET batch_no = excluded.batch_no, json = excluded.json""",
            [(i, batch_no, json.dumps(item, ensure_ascii=False, sort_keys=True)) for i, item in returned.items()],
        )
        db.executemany("INSERT OR REPLACE INTO missing VALUES (?, ?)", [(i, batch_no) for i in ids if i not in returned])
        db.execute("DELETE FROM missing WHERE id IN (SELECT id FROM items)")
        db.execute("UPDATE batches SET status = 'done' WHERE batch_no = ?", (batch_no,))


def enrichment_summary(db):
    statuses = {status: 0 for status in ("pending", "done", "split")}
    statuses.update(db.execute("SELECT status, count(*) FROM batches GROUP BY status"))
    one = lambda sql: db.execute(sql).fetchone()[0]
    return {
        "attempts": one("SELECT attempts FROM run"),
        "batches": statuses,
        "ids_requested": one("SELECT count(*) FROM batch_ids"),
        "items": one("SELECT count(*) FROM items"),
        "missing": one("SELECT count(*) FROM missing"),
        "extra_items": one("SELECT count(*) FROM items WHERE id NOT IN (SELECT id FROM batch_ids)"),
    }


def enrich(ids, checkpoint, *, key, max_requests_total, get, fields=ENRICH_FIELDS, batch_size=BYID_MAX_IDS,
           batch_limit=None, interval=1.0, now=time.time, sleep=time.sleep):
    ids = check_ids(ids)
    if not key:
        raise ValueError("Для дозагрузки нужен ключ.")
    if type(batch_size) is not int or not 1 <= batch_size <= BYID_MAX_IDS:
        raise ValueError(f"Размер пакета должен быть от 1 до {BYID_MAX_IDS}.")
    if type(max_requests_total) is not int or max_requests_total < 1:
        raise ValueError("Укажите положительный суммарный лимит HTTP-попыток.")
    if batch_limit is not None and (type(batch_limit) is not int or batch_limit < 1):
        raise ValueError("Укажите положительное число пакетов или None.")
    if not math.isfinite(interval) or not 1 <= interval <= 60:
        raise ValueError("Пауза между запросами должна составлять от 1 до 60 секунд.")
    with open_checkpoint(checkpoint, ENRICH_SCHEMA) as db:
        prepare_enrichment(db, ids, fields, batch_size)
        budget = RequestBudget(db, max_requests_total, interval, now=now, sleep=sleep)
        stop_reason, processed = None, 0
        while batch_limit is None or processed < batch_limit:
            row = db.execute("SELECT batch_no FROM batches WHERE status = 'pending' ORDER BY batch_no LIMIT 1").fetchone()
            if row is None:
                break
            try:
                fetch_batch(db, row[0], fields, key, get, budget)
            except CollectionStopped as error:
                stop_reason = f"Пакет {row[0]}. {error}"
                break
            processed += 1
        return enrichment_summary(db) | {"stop_reason": stop_reason}


def load_enriched(checkpoint):
    """Сохранённые ответы byid: список словарей и множество ненайденных ID (только чтение)."""
    with closing(sqlite3.connect(Path(checkpoint).resolve().as_uri() + "?mode=ro", uri=True)) as db:
        items = [json.loads(row[0]) for row in db.execute("SELECT json FROM items ORDER BY id")]
        missing = {row[0] for row in db.execute("SELECT id FROM missing")}
        summary = enrichment_summary(db)
    return items, missing, summary


# ---------------------------------------------------------------- разбор ответа

WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri")
WEEKEND = ("Sat", "Sun")


def _minutes(value):
    try:
        hours, minutes = str(value).split(":")
        return int(hours) * 60 + int(minutes)
    except (ValueError, AttributeError):
        return None


def _dict(value):
    return value if isinstance(value, dict) else {}


def _int(value):
    return value if type(value) is int else None


def parse_schedule(schedule):
    """Часы работы 2ГИС: {"Mon": {"working_hours": [{"from": "08:00", "to": "22:00"}]}, …, "is_24x7": bool}."""
    schedule = _dict(schedule)
    is_24x7 = schedule.get("is_24x7") is True

    def opening(day):
        hours = _dict(schedule.get(day)).get("working_hours")
        starts = [_minutes(_dict(h).get("from")) for h in hours] if isinstance(hours, list) else []
        starts = [s for s in starts if s is not None]
        return min(starts) if starts else None

    weekday = [m for m in map(opening, WEEKDAYS) if m is not None]
    earliest = 0 if is_24x7 else (min(weekday) if weekday else None)
    return {
        "has_schedule": is_24x7 or bool(weekday) or any(opening(d) is not None for d in WEEKEND),
        "open_24x7": is_24x7,
        "weekday_open_earliest": None if earliest is None else f"{earliest // 60:02d}:{earliest % 60:02d}",
        "opens_by_8": earliest is not None and earliest <= 8 * 60,
        "open_weekend": is_24x7 or any(opening(d) is not None for d in WEEKEND),
    }


def temporary_closed(flags):
    """flags.temporary_closed: тип в документации не описан (в образце — строка), берём любое «истинное» значение."""
    value = _dict(flags).get("temporary_closed")
    if isinstance(value, str):
        return value.strip().lower() not in ("", "false", "0", "no")
    return bool(value)


# атрибуты 2ГИС, полезные для кофейни (теги из реальных ответов по Алматы)
ATTR_AVG_CHECK = "food_service_avg_price"          # «Чек 4500 ₸»
ATTR_CAPACITY = "food_service_capacity"            # «До 20 мест»
ATTR_FLAGS = {
    "coffee_to_go": "cocktail_bars_types_coffee_take_away",   # «Кофе с собой»
    "free_wifi": "additionally_services_freewifi",            # Wi-Fi
    "laptop_friendly": "food_service_cafe_working",           # «Можно с ноутбуком»
}


def attribute_number(attributes, tag):
    """Число из названия атрибута: «Чек 8 000 ₸» → 8000, «До 20 мест» → 20, «Чек 2000–4000 ₸» → 3000."""
    for attribute in attributes:
        name = attribute.get("name")
        if attribute.get("tag") == tag and isinstance(name, str):
            numbers = [int(re.sub(r"[\s\xa0]", "", n)) for n in re.findall(r"\d[\d\s\xa0]*\d|\d", name)]
            if numbers:
                return round(sum(numbers) / len(numbers))
    return None


def parse_item(item):
    """Одна организация из ответа byid → плоская запись."""
    point = _dict(item.get("point"))
    lon, lat = point.get("lon"), point.get("lat")
    rubrics = [r for r in item.get("rubrics") or [] if isinstance(r, dict)]
    primary = next((r for r in rubrics if r.get("kind") == "primary"), rubrics[0] if rubrics else {})
    org, brand = _dict(item.get("org")), _dict(item.get("brand"))
    reviews, flags = _dict(item.get("reviews")), _dict(item.get("flags"))
    address, dates = _dict(item.get("address")), _dict(item.get("dates"))
    attributes = [a for group in item.get("attribute_groups") or [] if isinstance(group, dict)
                  for a in group.get("attributes") or [] if isinstance(a, dict)]
    rating, count = reviews.get("general_rating"), reviews.get("general_review_count")
    building = address.get("building_name") or item.get("building_name")
    return {
        "id": base_id(item["id"]),
        "name": item.get("name") if isinstance(item.get("name"), str) else None,
        "lon": lon if type(lon) in (int, float) else None,
        "lat": lat if type(lat) in (int, float) else None,
        "rubric_primary": primary.get("name"),
        "rubric_names": ";".join(r["name"] for r in rubrics if isinstance(r.get("name"), str)),
        "rubric_ids": ";".join(str(r["id"]) for r in rubrics if r.get("id") is not None),
        "org_id": org.get("id"),
        "org_name": org.get("name"),
        "org_branch_count": _int(org.get("branch_count")),
        "brand_id": brand.get("id"),
        "brand_name": brand.get("name"),
        "brand_branch_count": _int(brand.get("branch_count")),
        # шкала 2ГИС — от 1 до 5; 0 встречается у карточек без оценок и означает «нет рейтинга»
        "general_rating": rating if type(rating) in (int, float) and rating > 0 else None,
        "general_review_count": _int(count),
        **parse_schedule(item.get("schedule")),
        "temporary_closed": temporary_closed(flags),
        "temporary_closed_raw": None if flags.get("temporary_closed") is None else str(flags.get("temporary_closed")),
        "building_name": building if isinstance(building, str) else None,
        "avg_check_kzt": attribute_number(attributes, ATTR_AVG_CHECK),
        "seats_max": attribute_number(attributes, ATTR_CAPACITY),
        **{name: any(a.get("tag") == tag for a in attributes) for name, tag in ATTR_FLAGS.items()},
        "attribute_tags":";".join(sorted({a["tag"] for a in attributes if isinstance(a.get("tag"), str)})),
        "attribute_names": ";".join(sorted({a["name"] for a in attributes if isinstance(a.get("name"), str)})),
        "created_at": dates.get("created_at"),
        "updated_at": dates.get("updated_at"),
    }


# ---------------------------------------------------------------- сбор по рубрикам с делением области

# Демо-ключ отдаёт не больше 5 страниц по 10 записей на запрос. Поэтому область (город + буфер) делим на квадраты.
# Для каждого квадрата запрашиваем описанный вокруг него круг. Если 2ГИС сообщает больше 50 записей,
# квадрат делится на 4 (квадродерево). Если 50 или меньше, выкачиваем все страницы. Каждый квадрат-лист
# со статусом complete собран полностью, потому что его круг целиком лежит внутри выдачи.

DGIS_MAX_RADIUS_M = 50_000  # предел radius при поиске по рубрике

AREA_SCHEMA = RUN_TABLE + """
CREATE TABLE IF NOT EXISTS cells (
    cell_id TEXT PRIMARY KEY,
    depth INTEGER NOT NULL,
    x0 REAL NOT NULL,
    y0 REAL NOT NULL,
    side REAL NOT NULL,
    point TEXT NOT NULL,
    radius_m INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    next_page INTEGER NOT NULL DEFAULT 1,
    expected_total INTEGER
);
CREATE TABLE IF NOT EXISTS cell_items (
    cell_id TEXT NOT NULL,
    id TEXT NOT NULL,
    PRIMARY KEY (cell_id, id)
);
CREATE TABLE IF NOT EXISTS items (
    id TEXT PRIMARY KEY,
    json TEXT NOT NULL
);
"""


@dataclass(frozen=True)
class RubricSearch:
    rubric_ids: tuple
    region_id: int
    page_size: int = 10
    max_pages: int = 5
    min_side_m: float = 250.0
    fields: tuple = ENRICH_FIELDS

    def __post_init__(self):
        if not self.rubric_ids or not all(type(r) is int and r > 0 for r in self.rubric_ids):
            raise ValueError("Укажите числовые ID рубрик.")
        if type(self.region_id) is not int or self.region_id <= 0:
            raise ValueError("Укажите числовой region_id.")
        if type(self.page_size) is not int or not 1 <= self.page_size <= 50 or type(self.max_pages) is not int \
                or self.max_pages < 1 or not self.min_side_m > 0:
            raise ValueError("Проверьте размер страницы, предел страниц и минимальный размер квадрата.")

    @property
    def capacity(self):
        return self.page_size * self.max_pages

    def params(self):
        return {"rubric_id": ",".join(map(str, self.rubric_ids)), "region_id": self.region_id, "type": "branch",
                "page_size": self.page_size, "fields": ",".join(self.fields)}


def _area_tools(area, crs):
    import shapely
    from pyproj import Transformer

    shapely.prepare(area)
    return area, Transformer.from_crs(crs, "EPSG:4326", always_xy=True)


def _add_cell(db, cell_id, depth, x0, y0, side, transformer):
    lon, lat = transformer.transform(x0 + side / 2, y0 + side / 2)
    # описанный круг + запас 0,1 %: 2ГИС меряет по сфере, а квадрат задан в UTM (масштаб тут ~1,0001)
    radius = math.ceil(side * math.sqrt(2) / 2 * 1.001) + 1
    if radius > DGIS_MAX_RADIUS_M:
        raise ValueError(f"Квадрат {side:.0f} м слишком велик: радиус круга {radius} м больше {DGIS_MAX_RADIUS_M}.")
    db.execute("INSERT INTO cells (cell_id, depth, x0, y0, side, point, radius_m) VALUES (?, ?, ?, ?, ?, ?, ?)",
               (cell_id, depth, x0, y0, side, f"{lon:.6f},{lat:.6f}", radius))


def _cell_box(x0, y0, side):
    import shapely
    return shapely.box(x0, y0, x0 + side, y0 + side)


def prepare_area(db, search, area, crs, transformer):
    import hashlib
    import shapely

    minx, miny, maxx, maxy = area.bounds
    side = max(maxx - minx, maxy - miny)
    plan = json.dumps({
        "version": 1, "url": PLACES_URL, "params": search.params(), "max_pages": search.max_pages,
        "min_side_m": search.min_side_m, "crs": str(crs),
        "area_sha256": hashlib.sha256(shapely.to_wkb(area, hex=False)).hexdigest(),
    }, ensure_ascii=False, sort_keys=True)
    existing = db.execute("SELECT plan FROM run").fetchone()
    if existing and existing[0] != plan:
        raise CollectionStopped("План сбора изменился (рубрики, поля, область или параметры). Выберите новый файл прогресса.")
    if not existing:
        with db:
            db.execute("INSERT INTO run (id, plan) VALUES (1, ?)", (plan,))
            _add_cell(db, "r", 0, minx, miny, side, transformer)


def fetch_cell(db, cell_id, search, key, get, budget, area, transformer):
    depth, x0, y0, side, point, radius, page, expected, status = db.execute(
        "SELECT depth, x0, y0, side, point, radius_m, next_page, expected_total, status FROM cells WHERE cell_id = ?",
        (cell_id,)).fetchone()
    while status == "pending":
        params = dict(search.params(), key=key, point=point, radius=radius, page=page)
        total, items = page_items(get_with_retry(get, params, budget), search.page_size)
        expected = max(total, expected or 0)
        with db:
            db.executemany(
                "INSERT INTO items VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET json = excluded.json",
                [(base_id(it["id"]), json.dumps(it, ensure_ascii=False, sort_keys=True)) for it in items])
            db.executemany("INSERT OR IGNORE INTO cell_items VALUES (?, ?)", [(cell_id, base_id(it["id"])) for it in items])
            seen = db.execute("SELECT count(*) FROM cell_items WHERE cell_id = ?", (cell_id,)).fetchone()[0]
            if page == 1 and expected > search.capacity and side / 2 >= search.min_side_m:
                status = "split"
                half = side / 2
                for k, (dx, dy) in enumerate(((0, 0), (half, 0), (0, half), (half, half))):
                    if area.intersects(_cell_box(x0 + dx, y0 + dy, half)):
                        _add_cell(db, f"{cell_id}{k}", depth + 1, x0 + dx, y0 + dy, half, transformer)
            elif seen >= expected:
                status = "complete"
            elif not items or page * search.page_size >= expected:
                status = "short"
            elif page >= search.max_pages:
                status = "capped"
            db.execute("UPDATE cells SET next_page = ?, expected_total = ?, status = ? WHERE cell_id = ?",
                       (page + 1, expected, status, cell_id))
        page += 1


def area_summary(db):
    statuses = {s: 0 for s in ("pending", "split", "complete", "capped", "short")}
    statuses.update(db.execute("SELECT status, count(*) FROM cells GROUP BY status"))
    one = lambda sql: db.execute(sql).fetchone()[0]
    return {"attempts": one("SELECT attempts FROM run"), "cells": statuses,
            "max_depth": one("SELECT max(depth) FROM cells"), "items": one("SELECT count(*) FROM items")}


def collect_area(search, checkpoint, *, area, crs, key, get, max_requests_total, cell_limit=None,
                 interval=1.0, now=time.time, sleep=time.sleep):
    """Сбор по рубрикам внутри области (shapely-геометрия в метрической CRS) с делением на квадраты."""
    if not key:
        raise ValueError("Для сбора нужен ключ.")
    if type(max_requests_total) is not int or max_requests_total < 1:
        raise ValueError("Укажите положительный суммарный лимит HTTP-попыток.")
    if cell_limit is not None and (type(cell_limit) is not int or cell_limit < 1):
        raise ValueError("Укажите положительное число квадратов или None.")
    if not math.isfinite(interval) or not 1 <= interval <= 60:
        raise ValueError("Пауза между запросами должна составлять от 1 до 60 секунд.")
    area, transformer = _area_tools(area, crs)
    with open_checkpoint(checkpoint, AREA_SCHEMA) as db:
        prepare_area(db, search, area, crs, transformer)
        budget = RequestBudget(db, max_requests_total, interval, now=now, sleep=sleep)
        stop_reason, processed = None, 0
        while cell_limit is None or processed < cell_limit:
            row = db.execute("SELECT cell_id FROM cells WHERE status = 'pending' ORDER BY depth, cell_id LIMIT 1").fetchone()
            if row is None:
                break
            try:
                fetch_cell(db, row[0], search, key, get, budget, area, transformer)
            except CollectionStopped as error:
                stop_reason = f"Квадрат {row[0]}. {error}"
                break
            processed += 1
        return area_summary(db) | {"stop_reason": stop_reason}


def load_area(checkpoint):
    """Сохранённые ответы и квадраты сбора по рубрикам (только чтение)."""
    with closing(sqlite3.connect(Path(checkpoint).resolve().as_uri() + "?mode=ro", uri=True)) as db:
        items = [json.loads(row[0]) for row in db.execute("SELECT json FROM items ORDER BY id")]
        columns = ("cell_id", "depth", "x0", "y0", "side", "point", "radius_m", "status", "next_page", "expected_total")
        cells = [dict(zip(columns, row)) for row in db.execute(f"SELECT {', '.join(columns)} FROM cells ORDER BY cell_id")]
        summary = area_summary(db)
    return items, cells, summary


# ---------------------------------------------------------------- признаки кандидатов

def classify(enriched, coffee_rubrics, chain_min_branches):
    """is_coffee — есть рубрика из coffee_rubrics; is_chain — у организации или бренда не меньше chain_min_branches филиалов."""
    out = enriched.copy()
    names = out["rubric_names"].fillna("").str.split(";")
    out["is_coffee"] = names.map(lambda rubrics: bool(set(coffee_rubrics) & set(rubrics)))
    branches = out[["org_branch_count", "brand_branch_count"]].astype("float").max(axis=1)
    out["is_chain"] = branches.fillna(0) >= chain_min_branches
    return out


def coffee_features(candidates, coffee, radius_m, prefix="dgis_coffee"):
    """Признаки в радиусе radius_m: число, сетевые, открытые к 8:00, сумма отзывов, средний рейтинг, медианный чек.

    candidates — candidate_id и geometry; coffee — точки в той же CRS со столбцами is_chain, opens_by_8,
    general_rating, general_review_count, avg_check_kzt.
    """
    import pandas as pd

    suffix = f"_{radius_m}m"
    cand_pos, coffee_pos = coffee.sindex.query(candidates.geometry, predicate="dwithin", distance=radius_m)
    columns = ["is_chain", "opens_by_8", "general_rating", "general_review_count", "avg_check_kzt"]
    pairs = coffee.iloc[coffee_pos][columns].astype(
        {"is_chain": int, "opens_by_8": int, "general_rating": float, "general_review_count": float,
         "avg_check_kzt": float})
    grouped = pairs.reset_index(drop=True).groupby(cand_pos)
    out = pd.DataFrame(index=pd.RangeIndex(len(candidates)))
    out[prefix + suffix] = grouped.size()
    out[f"{prefix}_chain{suffix}"] = grouped["is_chain"].sum()
    out[f"{prefix}_early{suffix}"] = grouped["opens_by_8"].sum()
    out[f"{prefix}_reviews{suffix}"] = grouped["general_review_count"].sum()
    out[f"{prefix}_rating_mean{suffix}"] = grouped["general_rating"].mean().round(3)
    out[f"{prefix}_avg_check_median{suffix}"] = grouped["avg_check_kzt"].median()
    counts = [c for c in out.columns if "rating_mean" not in c and "avg_check" not in c]
    out[counts] = out[counts].fillna(0).astype(int)
    out.insert(0, "candidate_id", candidates["candidate_id"].to_numpy())
    return out


def covered_by_squares(candidates, cells, need_m, statuses=("complete",)):
    """True, если круг need_m вокруг кандидата целиком внутри квадратов-листов с полной выдачей (допуск 1 м)."""
    import numpy as np
    import shapely

    boxes = [_cell_box(c["x0"], c["y0"], c["side"]) for c in cells if c["status"] in statuses]
    if not boxes:
        return np.zeros(len(candidates), dtype=bool)
    # +1 см к каждому квадрату: иначе на стыках соседних квадратов остаются щели шириной в погрешность float
    done = shapely.union_all(shapely.buffer(boxes, 0.01, join_style="mitre"))
    discs = shapely.buffer(candidates.geometry.to_numpy(), need_m - 1, quad_segs=16)
    return shapely.covers(done, discs)
