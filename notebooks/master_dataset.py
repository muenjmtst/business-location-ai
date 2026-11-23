"""Мастер-датасет проекта (этап 04): единая таблица кандидатов и справочники объектов — без потерь данных.

Слои данных
-----------
- raw — исходники, которые не меняются: `data/raw/…` и SQLite-файлы сборщиков 2ГИС с ответами API целиком.
- features — признаки по `candidate_id`. Их пишут ноутбуки 02–03 в `data/processed/features/` и `almaty_features.parquet`.
- master — то, что собирает этот модуль в `data/processed/master/`:
    - `candidates_master.parquet` — одна строка на кандидата, все признаки (GeoParquet, EPSG:32643);
    - `candidates_master.csv` — та же таблица без геометрии, только для просмотра (типы в CSV теряются);
    - `poi_2gis.parquet` — все объекты 2ГИС из сырых ответов: одна строка на id, полный JSON ответа в `raw_json`;
    - `districts.parquet` — районы со статистикой (если есть файлы этапа statgov);
    - `data_dictionary.csv` — каждый столбец: источник, ноутбук-владелец, тип, пропуски, описание;
    - `manifest.json` — входы и выходы с sha256 и числом строк, пропущенные источники с причиной.

Правила
-------
- Единица наблюдения — кандидат, ключ — `candidate_id` из 02. Объекты (кофейни, БЦ…) и районы — отдельные
  таблицы другой гранулярности. В таблицу кандидатов попадают только их агрегаты (признаки) и атрибуты района.
- Источник с `candidate_id` проверяется так: id уникальны, набор id совпадает с сеткой, x/y совпадают
  с геометрией сетки (`atol` 1 см). Источник без `candidate_id` сопоставляется по геометрии точки с тем же допуском.
- Одинаковое имя столбца в двух источниках — ошибка, а не тихое переименование.
- После сборки значения каждого источника сравниваются с исходными, столбец за столбцом: без потерь.
- Технические столбцы не переносятся в таблицу кандидатов, но остаются в справочниках. Каждый такой пропуск записан в отчёт.
"""
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import sqlite3
from contextlib import closing

import numpy as np
import pandas as pd
import geopandas as gpd
import shapely

CRS_METRIC = "EPSG:32643"
N_CANDIDATES = 2822
XY_ATOL_M = 0.01
MASTER_DIR = Path("data/processed/master")
BASE_COLUMNS = ("candidate_id", "x", "y", "lon", "lat", "geometry")


class MasterBuildError(ValueError):
    """Нарушена целостность при сборке мастер-датасета."""


@dataclass(frozen=True)
class FeatureSource:
    name: str                 # короткое имя источника
    path: str                 # путь относительно корня проекта
    owner: str                # ноутбук, который создаёт файл
    key: str = "candidate_id"  # "candidate_id" или "geometry"
    drop: tuple = ()          # столбцы, которые сознательно не переносим (с причиной в отчёте)
    description: str = ""


FEATURE_SOURCES = (
    FeatureSource("base02", "data/processed/almaty_features.parquet", "02_candidate_locations", key="geometry",
                  description="Базовые признаки 02: кафе, остановки, жильё, офисы, вузы, колледжи, школы (OSM)"),
    FeatureSource("osm", "data/processed/features/osm_commercial_features.csv", "03_osm_commercial_features",
                  description="Коммерческое окружение по OSM: магазины, ТРЦ, рестораны, метро, остановки, парковки, дороги"),
    FeatureSource("osm_context", "data/processed/features/osm_residential_schools.csv", "03_residential_schools_districts",
                  description="Жилая застройка и школы по OSM (снимок 261118), правила как в 02"),
    FeatureSource("dgis_coffee", "data/processed/features/dgis_coffee_features.csv", "03_2gis_all_coffee",
                  description="Конкуренты по 2ГИС: кофейни и точки кофе в 500 м"),
    FeatureSource("dgis_demand", "data/processed/features/dgis_demand_features.csv", "03_2gis_demand_layers",
                  description="Слои спроса по 2ГИС: БЦ, пекарни, фитнес, вузы, ТЦ, кофейные автоматы"),
)
DISTRICTS_PATH = "data/processed/almaty_districts.parquet"            # 03_district_spatial_join (границы)
DISTRICT_STATS_PATH = "data/processed/almaty_district_stats.csv"      # 03_statgov_exploration (статистика)
DISTRICT_TECHNICAL = ("OBJECTID", "GlobalID", "Shape__Area", "Shape__Length")
BOUNDARY_PATH = "data/processed/almaty_boundary.gpkg"
AREA_BUFFER_M = 1000

DGIS_AREA_CHECKPOINTS = {  # сбор по рубрикам (dgis_api.collect_area): полные ответы в JSON
    "coffee": "data/processed/2gis_coffee/coffee_progress.sqlite3",
    "offices": "data/processed/2gis_layers/offices_progress.sqlite3",
    "bakeries": "data/processed/2gis_layers/bakeries_progress.sqlite3",
    "fitness": "data/processed/2gis_layers/fitness_progress.sqlite3",
    "education": "data/processed/2gis_layers/education_progress.sqlite3",
    "malls": "data/processed/2gis_layers/malls_progress.sqlite3",
    "vending": "data/processed/2gis_layers/vending_progress.sqlite3",
}
DGIS_BYID_CHECKPOINTS = {"enrich": "data/processed/2gis_enriched/enrich_progress.sqlite3"}
DGIS_SEARCH_CHECKPOINTS = {  # текстовый поиск «кофейня»: только id, имя, координаты, рейтинг
    "gap": "data/processed/2gis_gap/gap_progress.sqlite3",
    "main": "data/processed/2gis_almaty_progress.sqlite3",           # основной сбор Давида
}
POI_LAYERS = {  # флаги слоёв по рубрикам 2ГИС
    "coffee_shop": {162}, "coffee_point": {58871}, "business_center": {13796}, "coworking": {104110},
    "bakery": {111594}, "cafe_confectionery": {112658}, "fitness": {268, 267},
    "education": {232, 230, 231, 246}, "mall": {611, 19499}, "coffee_vending": {112719},
}


# ---------------------------------------------------------------- общие функции

def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_table(path):
    path = Path(path)
    if path.suffix == ".csv":
        return pd.read_csv(path)
    if path.suffix == ".parquet":
        try:
            return gpd.read_parquet(path)
        except ValueError:  # parquet без геометрии
            return pd.read_parquet(path)
    if path.suffix == ".gpkg":
        return gpd.read_file(path)
    raise MasterBuildError(f"Неизвестный формат: {path}")


def load_candidates(root, expected=N_CANDIDATES):
    grid = gpd.read_file(Path(root) / "data/processed/candidate_locations.gpkg")
    if "candidate_id" not in grid.columns:
        raise MasterBuildError("В candidate_locations.gpkg нет candidate_id — пересоберите сетку через 02.")
    grid = grid[["candidate_id", "geometry"]].to_crs(CRS_METRIC)
    if expected is not None and len(grid) != expected:
        raise MasterBuildError(f"В сетке {len(grid)} точек, ожидалось {expected}.")
    if not grid["candidate_id"].is_unique or not (grid.geom_type == "Point").all():
        raise MasterBuildError("candidate_id должен быть уникален, геометрия — точки.")
    grid = grid.sort_values("candidate_id").reset_index(drop=True)
    grid["x"], grid["y"] = grid.geometry.x, grid.geometry.y
    lonlat = grid.geometry.to_crs("EPSG:4326")
    grid["lon"], grid["lat"] = lonlat.x, lonlat.y
    return grid[list(BASE_COLUMNS)]


# ---------------------------------------------------------------- признаки по candidate_id

def align_source(source, table, candidates):
    """Таблица источника → DataFrame с индексом candidate_id и только признаками; плюс отчёт о проверках."""
    report = {"name": source.name, "path": source.path, "owner": source.owner, "key": source.key,
              "rows": len(table), "dropped": {}}
    table = table.copy()
    for col in source.drop:
        if col in table.columns:
            report["dropped"][col] = "технический столбец (указан в FeatureSource.drop)"
            table = table.drop(columns=col)
    grid = candidates.set_index("candidate_id")

    if source.key == "candidate_id":
        if "candidate_id" not in table.columns:
            raise MasterBuildError(f"{source.name}: нет столбца candidate_id.")
        if not table["candidate_id"].is_unique:
            raise MasterBuildError(f"{source.name}: candidate_id повторяются.")
        extra = set(table["candidate_id"]) - set(grid.index)
        missing = set(grid.index) - set(table["candidate_id"])
        if extra or missing:
            raise MasterBuildError(f"{source.name}: лишних id {len(extra)}, потерянных {len(missing)}.")
        table = table.set_index("candidate_id")
        if {"x", "y"} <= set(table.columns):
            dx = (table["x"] - grid.loc[table.index, "x"]).abs()
            dy = (table["y"] - grid.loc[table.index, "y"]).abs()
            if (dx > XY_ATOL_M).any() or (dy > XY_ATOL_M).any():
                raise MasterBuildError(f"{source.name}: x/y расходятся с сеткой больше чем на {XY_ATOL_M} м.")
            report["xy_max_diff_m"] = float(max(dx.max(), dy.max()))
            report["dropped"]["x"] = report["dropped"]["y"] = "проверочные координаты: совпали с геометрией сетки"
            table = table.drop(columns=["x", "y"])
        if "geometry" in table.columns:
            raise MasterBuildError(f"{source.name}: геометрию источника с candidate_id не переносим — уберите её.")
    elif source.key == "geometry":
        if not isinstance(table, gpd.GeoDataFrame) or not (table.geom_type == "Point").all():
            raise MasterBuildError(f"{source.name}: для сопоставления по геометрии нужны точки.")
        points = table.to_crs(CRS_METRIC).reset_index(drop=True)
        matched = gpd.sjoin_nearest(points[["geometry"]], candidates[["candidate_id", "geometry"]],
                                    how="left", max_distance=XY_ATOL_M, distance_col="match_dist_m")
        if matched.index.duplicated().any():
            raise MasterBuildError(f"{source.name}: точка совпала с несколькими кандидатами.")
        if matched["candidate_id"].isna().any():
            raise MasterBuildError(f"{source.name}: {matched['candidate_id'].isna().sum()} точек не совпали с сеткой.")
        if not matched["candidate_id"].is_unique or len(matched) != len(candidates):
            raise MasterBuildError(f"{source.name}: соответствие точек и кандидатов не один к одному.")
        if "candidate_id" in points.columns and not (points["candidate_id"].to_numpy() == matched["candidate_id"].to_numpy()).all():
            raise MasterBuildError(f"{source.name}: candidate_id в файле не совпадает с геометрией.")
        report["xy_max_diff_m"] = float(matched["match_dist_m"].max())
        report["dropped"]["geometry"] = "совпала с геометрией сетки (сопоставление по координатам)"
        table = pd.DataFrame(points.drop(columns=["geometry"] + (["candidate_id"] if "candidate_id" in points else [])))
        table.index = pd.Index(matched["candidate_id"].astype("int64").to_numpy(), name="candidate_id")
    else:
        raise MasterBuildError(f"{source.name}: неизвестный ключ {source.key}.")

    table = table.sort_index()
    report["columns"] = list(table.columns)
    return table, report


def check_collisions(frames):
    seen = {c: "base" for c in BASE_COLUMNS}
    clashes = []
    for name, frame in frames.items():
        for col in frame.columns:
            if col in seen:
                clashes.append(f"{col}: {seen[col]} и {name}")
            seen.setdefault(col, name)
    if clashes:
        raise MasterBuildError("Одинаковые столбцы в разных источниках: " + "; ".join(clashes))


def assert_lossless(master, frames):
    """Значения каждого источника в мастер-таблице совпадают с исходными (значения, пропуски и типы)."""
    indexed = master.set_index("candidate_id").sort_index()
    for name, frame in frames.items():
        got = pd.DataFrame(indexed[frame.columns])
        try:
            pd.testing.assert_frame_equal(got, frame.sort_index(), check_dtype=True, check_exact=True,
                                          check_names=False)
        except AssertionError as error:
            raise MasterBuildError(f"{name}: данные изменились при сборке: {str(error)[:300]}") from None


def check_split_copies(root, combined_path, pattern="data/processed/features/*.csv"):
    """Отдельные CSV признаков (shops_500m.csv …) должны совпадать со столбцами сводного файла."""
    root = Path(root)
    combined = pd.read_csv(root / combined_path).set_index("candidate_id").sort_index()
    checked = []
    for path in sorted(root.glob(pattern)):
        if path.stem in combined.columns:
            single = pd.read_csv(path).set_index("candidate_id").sort_index()
            pd.testing.assert_series_equal(single[path.stem], combined[path.stem], check_exact=True)
            checked.append(path.name)
    return checked


# ---------------------------------------------------------------- районы

def normalize_district(name):
    text = str(name).strip().lower().replace("ё", "е").replace("э", "е")
    return re.sub(r"\s+район$", "", text).strip()


def assign_districts(candidates, districts, name_col="NAME"):
    """Район кандидата: внутри полигона, на границе двух (берём первый по имени) или ближайший (с расстоянием)."""
    polys = districts[[name_col, "geometry"]].to_crs(candidates.crs).rename(columns={name_col: "district"})
    polys["district"] = polys["district"].astype(str).str.strip()
    hits = gpd.sjoin(candidates[["candidate_id", "geometry"]], polys, how="left", predicate="intersects")
    n_hits = hits.groupby("candidate_id")["district"].count()
    hits = hits.sort_values(["candidate_id", "district"]).drop_duplicates("candidate_id").set_index("candidate_id")
    out = pd.DataFrame({"district": hits["district"]})
    out["district_method"] = np.where(n_hits.reindex(out.index) > 1, "boundary", "within")
    out["district_distance_m"] = 0.0
    lost = out["district"].isna()
    if lost.any():
        far = candidates[candidates["candidate_id"].isin(out.index[lost])][["candidate_id", "geometry"]]
        near = gpd.sjoin_nearest(far, polys, how="left", distance_col="d").sort_values(["candidate_id", "district"])
        near = near.drop_duplicates("candidate_id").set_index("candidate_id")
        out.loc[near.index, "district"] = near["district"]
        out.loc[near.index, "district_method"] = "nearest"
        out.loc[near.index, "district_distance_m"] = near["d"]
    out.index.name = "candidate_id"
    return out.sort_index()


def build_district_table(districts, stats=None, name_col="NAME"):
    """Справочник районов: все столбцы границ + статистика по нормализованному названию (1:1, без потерь).

    Без статистики (stats=None) справочник состоит из одних границ и их атрибутов.
    """
    dim = districts.copy()
    dim["district"] = dim[name_col].astype(str).str.strip()
    dim["district_key"] = dim["district"].map(normalize_district)
    if not dim["district_key"].is_unique:
        raise MasterBuildError("Названия районов в границах повторяются.")
    if stats is None:
        return dim
    stats = stats.copy()
    stats["district_key"] = stats["district"].map(normalize_district)
    stats = stats.rename(columns={"district": "district_name_statgov"})
    merged = dim.merge(stats, on="district_key", how="outer", validate="one_to_one", indicator=True)
    if not (merged["_merge"] == "both").all():
        bad = merged.loc[merged["_merge"] != "both", ["district", "district_name_statgov"]].to_dict("records")
        raise MasterBuildError(f"Районы границ и статистики не совпали: {bad}")
    return gpd.GeoDataFrame(merged.drop(columns="_merge"), geometry="geometry", crs=districts.crs)


def district_features(assigned, dim, name_col="NAME"):
    """Атрибуты района для кандидатов: всё, кроме геометрии, ключей и технических столбцов, с префиксом district_."""
    keep = [c for c in dim.columns if c not in ("geometry", "district", "district_key", name_col) + DISTRICT_TECHNICAL]
    attrs = pd.DataFrame(dim[["district"] + keep]).rename(
        columns={c: c if c.startswith("district_") else f"district_{c}" for c in keep})
    merged = assigned.reset_index().merge(attrs, on="district", how="left", validate="many_to_one")
    if merged["district"].notna().sum() != assigned["district"].notna().sum():
        raise MasterBuildError("Не у всех назначенных районов нашлись атрибуты.")
    return merged.set_index("candidate_id").sort_index()


# ---------------------------------------------------------------- объекты 2ГИС

def build_poi_2gis(root, area=None):
    """Все объекты 2ГИС из сырых ответов: одна строка на id, флаги слоёв, источник(и) и полный JSON."""
    import sys
    sys.path.insert(0, str(Path(root) / "notebooks"))
    from dgis_api import base_id, load_area, load_enriched, parse_item

    root = Path(root)
    records, used = {}, {}
    for name, rel in list(DGIS_AREA_CHECKPOINTS.items()) + list(DGIS_BYID_CHECKPOINTS.items()):
        path = root / rel
        if not path.is_file():
            continue
        items = load_area(path)[0] if name in DGIS_AREA_CHECKPOINTS else load_enriched(path)[0]
        used[name] = {"path": rel, "items": len(items)}
        for item in items:
            rec = records.setdefault(base_id(item["id"]), {"item": item, "sources": set()})
            rec["sources"].add(name)
    for name, rel in DGIS_SEARCH_CHECKPOINTS.items():
        path = root / rel
        if not path.is_file():
            continue
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            rows = pd.read_sql_query("SELECT * FROM organizations", db)
        used[name] = {"path": rel, "items": len(rows)}
        for row in rows.itertuples(index=False):
            rec = records.setdefault(base_id(row.id), {"item": None, "basic": row._asdict(), "sources": set()})
            rec["sources"].add(name)

    rows = []
    for pid, rec in records.items():
        if rec["item"] is not None:
            row = parse_item(rec["item"])
            row["raw_json"] = json.dumps(rec["item"], ensure_ascii=False, sort_keys=True)
        else:  # только из текстового поиска: базовые поля
            b = rec["basic"]
            row = {"id": pid, "name": b["name"], "lon": b["lon"], "lat": b["lat"],
                   "general_rating": b["general_rating"], "general_review_count": b["general_review_count"],
                   "raw_json": None}
        row["sources"] = ";".join(sorted(rec["sources"]))
        rows.append(row)
    poi = pd.DataFrame(rows).sort_values("id").reset_index(drop=True)
    rubric_ids = poi.get("rubric_ids", pd.Series([""] * len(poi))).fillna("").map(
        lambda s: {int(r) for r in s.split(";") if r.isdigit()})
    for layer, ids in POI_LAYERS.items():
        poi[f"is_{layer}"] = rubric_ids.map(lambda r, ids=ids: bool(r & ids))
    for col in ("org_branch_count", "brand_branch_count", "general_review_count", "avg_check_kzt", "seats_max"):
        if col in poi:
            poi[col] = poi[col].astype("Int64")
    for col in poi.columns:  # bool с пропусками (объекты только из текстового поиска) → nullable boolean
        values = poi[col].dropna()
        if poi[col].isna().any() and len(values) and values.map(lambda v: isinstance(v, (bool, np.bool_))).all():
            poi[col] = poi[col].astype("boolean")
    has_xy = poi["lon"].notna() & poi["lat"].notna()
    geometry = gpd.points_from_xy(poi["lon"].where(has_xy, 0), poi["lat"].where(has_xy, 0), crs="EPSG:4326")
    poi = gpd.GeoDataFrame(poi, geometry=geometry).to_crs(CRS_METRIC)
    poi.loc[~has_xy, "geometry"] = None
    poi["x"], poi["y"] = poi.geometry.x, poi.geometry.y
    poi["in_area"] = shapely.covers(area, poi.geometry.to_numpy()) if area is not None else pd.NA
    if not poi["id"].is_unique:
        raise MasterBuildError("В poi_2gis повторяются id.")
    return poi, used


# ---------------------------------------------------------------- словарь данных

DESCRIPTIONS = {
    "candidate_id": "ID точки-кандидата (из 02, сетка 500 м)",
    "x": "Координата X, м (EPSG:32643)", "y": "Координата Y, м (EPSG:32643)",
    "lon": "Долгота (WGS84)", "lat": "Широта (WGS84)",
    "shops_500m": "Магазины OSM в 500 м (после склейки дублей)",
    "malls_1000m": "ТРЦ OSM в 1000 м", "restaurants_500m": "Рестораны/кафе/фастфуд OSM в 500 м",
    "metro_dist": "Расстояние до станции метро, м", "transit_stops_500m": "Остановки ОТ в 500 м (без дублей)",
    "parking_500m": "Парковки OSM в 500 м", "major_roads_distance": "Расстояние до крупной дороги, м",
    "attractions_1000m": "Достопримечательности OSM в 1000 м",
    "dgis_coffee_500m": "Кофейни 2ГИС (рубрика «Кофейни») в 500 м",
    "dgis_coffee_chain_500m": "Из них сетевые (≥3 филиалов)", "dgis_coffee_early_500m": "Из них открыты к 8:00 в будни",
    "dgis_coffee_reviews_500m": "Сумма отзывов кофеен в 500 м", "dgis_coffee_rating_mean_500m": "Средний рейтинг кофеен в 500 м",
    "dgis_coffee_avg_check_median_500m": "Медианный средний чек кофеен в 500 м, ₸",
    "dgis_coffee_points_500m": "«Точки кофе» 2ГИС (без рубрики «Кофейни») в 500 м",
    "dgis_search_complete_500m": "Круг 500 м целиком покрыт полным сбором кофеен 2ГИС",
    "dgis_coworking_1000m": "Коворкинги 2ГИС в 1000 м",
    "district": "Район города", "district_method": "Как назначен район: within / boundary / nearest",
    "district_distance_m": "Расстояние до полигона района, если назначен ближайший, м",
    "district_RAION": "Код района (ArcGIS)", "district_Population_est": "Оценка населения района (ArcGIS)",
    "district_Schools_capacity": "Вместимость школ района (ArcGIS)",
    "district_Students_to_population": "Доля учащихся в населении района (ArcGIS)",
    "osm_houses_500m": "Частные дома OSM (house, detached) в 500 м",
    "osm_apartments_500m": "Многоквартирные дома OSM (apartments) в 500 м",
    "osm_residential_unknown_500m": "Жилые здания OSM без подтипа (residential) в 500 м",
    "osm_residential_buildings_500m": "Все жилые здания OSM в 500 м (сумма трёх групп)",
    "osm_schools_500m": "Уникальные школы OSM в 500 м (полигон, точки и вложенные полигоны — одна школа)",
}
LAYER_NAMES = {"offices": "БЦ и коворкинги", "bakeries": "пекарни и кафе-кондитерские", "fitness": "фитнес-клубы и залы",
               "education": "вузы и колледжи", "malls": "ТЦ и ТРЦ", "vending": "кофейные автоматы"}


def describe(col):
    if col in DESCRIPTIONS:
        return DESCRIPTIONS[col]
    m = re.fullmatch(r"dgis_(\w+?)_(500|1000)m", col)
    if m and m.group(1) in LAYER_NAMES:
        return f"2ГИС: {LAYER_NAMES[m.group(1)]} в {m.group(2)} м"
    m = re.fullmatch(r"dgis_(\w+?)_complete_1000m", col)
    if m and m.group(1) in LAYER_NAMES:
        return f"Круг 1000 м целиком покрыт полным сбором слоя «{LAYER_NAMES[m.group(1)]}»"
    if col.startswith("district_"):
        return "Атрибут района (statgov / границы районов)"
    return ""


def data_dictionary(master, column_sources, owners):
    rows = []
    for col in master.columns:
        if col == "geometry":
            continue
        s = master[col]
        src = column_sources.get(col, "base")
        rows.append({"column": col, "source": src, "owner": owners.get(src, "02_candidate_locations"),
                     "dtype": str(s.dtype), "non_null": int(s.notna().sum()), "null": int(s.isna().sum()),
                     "description": describe(col)})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------- сборка

def build_master(root, *, sources=FEATURE_SOURCES, expected=N_CANDIDATES, write=True, out_dir=MASTER_DIR,
                 with_poi=True):
    """Собирает мастер-датасет. Возвращает dict с таблицами и отчётом; при write=True пишет файлы в out_dir."""
    root = Path(root)
    candidates = load_candidates(root, expected)
    frames, reports, skipped, inputs = {}, [], {}, {}
    inputs["data/processed/candidate_locations.gpkg"] = {"sha256": sha256_file(root / "data/processed/candidate_locations.gpkg"),
                                                         "rows": len(candidates)}
    for source in sources:
        path = root / source.path
        if not path.is_file():
            skipped[source.name] = f"нет файла {source.path} (создаёт {source.owner})"
            continue
        table, report = align_source(source, read_table(path), candidates)
        frames[source.name], inputs[source.path] = table, {"sha256": sha256_file(path), "rows": report["rows"]}
        reports.append(report)

    district_frame, dim_district = None, None
    if (root / DISTRICTS_PATH).is_file():
        districts = gpd.read_parquet(root / DISTRICTS_PATH)
        inputs[DISTRICTS_PATH] = {"sha256": sha256_file(root / DISTRICTS_PATH), "rows": len(districts)}
        stats = None
        if (root / DISTRICT_STATS_PATH).is_file():
            stats = pd.read_csv(root / DISTRICT_STATS_PATH)
            inputs[DISTRICT_STATS_PATH] = {"sha256": sha256_file(root / DISTRICT_STATS_PATH), "rows": len(stats)}
        else:
            skipped["district_stats"] = f"нет {DISTRICT_STATS_PATH} (создаёт 03_statgov_exploration из Excel stat.gov.kz)"
        dim_district = build_district_table(districts, stats)
        district_frame = district_features(assign_districts(candidates, districts), dim_district)
        frames["district"] = district_frame
    else:
        skipped["district"] = (f"нет {DISTRICTS_PATH} (создают 03_residential_schools_districts или 03_district_spatial_join)")

    check_collisions(frames)
    master = candidates.set_index("candidate_id")
    for frame in frames.values():
        master = master.join(frame, how="left", validate="one_to_one")
    master = gpd.GeoDataFrame(master.reset_index(), geometry="geometry", crs=CRS_METRIC)
    if len(master) != len(candidates) or not master["candidate_id"].is_unique:
        raise MasterBuildError("После соединения изменилось число кандидатов.")
    assert_lossless(master, frames)

    column_sources = {c: name for name, f in frames.items() for c in f.columns}
    owners = {s.name: s.owner for s in sources} | {"district": "03_district_spatial_join + 03_statgov_exploration"}
    dictionary = data_dictionary(master, column_sources, owners)

    poi, poi_sources = None, {}
    if with_poi:
        boundary = gpd.read_file(root / BOUNDARY_PATH).to_crs(CRS_METRIC)
        area = shapely.union_all(boundary.buffer(AREA_BUFFER_M).to_numpy())
        poi, poi_sources = build_poi_2gis(root, area)
        for name, info in poi_sources.items():
            inputs[info["path"]] = {"sha256": sha256_file(root / info["path"]), "rows": info["items"]}

    result = {"master": master, "dictionary": dictionary, "poi": poi, "districts": dim_district,
              "reports": reports, "skipped": skipped, "inputs": inputs, "poi_sources": poi_sources}
    if write:
        result["outputs"] = write_outputs(root, out_dir, result)
    return result


def write_outputs(root, out_dir, result):
    import geopandas, pyarrow, shapely as shp

    out = Path(root) / out_dir
    out.mkdir(parents=True, exist_ok=True)
    files = {"candidates_master.parquet": result["master"]}
    if result["poi"] is not None:
        files["poi_2gis.parquet"] = result["poi"]
    if result["districts"] is not None:
        files["districts.parquet"] = result["districts"]
    outputs = {}
    for name, frame in files.items():
        frame.to_parquet(out / name, index=False)
        outputs[name] = {"rows": len(frame), "columns": frame.shape[1], "sha256": sha256_file(out / name)}
    pd.DataFrame(result["master"].drop(columns="geometry")).to_csv(out / "candidates_master.csv", index=False,
                                                                   lineterminator="\n")
    result["dictionary"].to_csv(out / "data_dictionary.csv", index=False, lineterminator="\n")
    for name in ("candidates_master.csv", "data_dictionary.csv"):
        outputs[name] = {"sha256": sha256_file(out / name)}
    manifest = {
        "inputs": result["inputs"], "outputs": outputs, "skipped_sources": result["skipped"],
        "feature_sources": [{k: v for k, v in r.items()} for r in result["reports"]],
        "poi_sources": result["poi_sources"],
        "versions": {"pandas": pd.__version__, "geopandas": geopandas.__version__, "shapely": shp.__version__,
                     "pyarrow": pyarrow.__version__},
    }
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                                       encoding="utf-8")
    return outputs


def load_master(root="..", with_geometry=True):
    """Мастер-таблица кандидатов для EDA и модели: load_master() из папки notebooks."""
    path = Path(root) / MASTER_DIR / "candidates_master.parquet"
    if not path.is_file():
        raise FileNotFoundError("Нет мастер-датасета: выполните 04_master_dataset.ipynb.")
    master = gpd.read_parquet(path)
    return master if with_geometry else pd.DataFrame(master.drop(columns="geometry"))
