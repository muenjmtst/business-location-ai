# business-location-ai

Подбор места для кофейни в Алматы (CoffeeSpot AI). Единица наблюдения — точка сетки 500 × 500 м внутри границы города: 2 822 кандидата в `EPSG:32643`.

## Порядок запуска

1. `notebooks/01_data_exploration.ipynb` → `data/processed/cafes_points.gpkg`.
2. `notebooks/02_candidate_locations.ipynb` → граница `almaty_boundary.gpkg`, сетка `candidate_locations.gpkg` со столбцом `candidate_id`, признаки 02 в `almaty_features.parquet`.
3. `notebooks/03_osm_commercial_features.ipynb` → признаки OSM: магазины, ТРЦ, рестораны, метро, остановки, парковки, крупные дороги, достопримечательности.

03 читает границу и сетку из файлов 02. Поэтому **02 запускается раньше 03**, и при любой пересборке сетки 03 нужно перезапустить. Ноутбуки `03_2gis_data*.ipynb` и `03_statgov_exploration.ipynb` — отдельные этапы, для 03_osm они не нужны.

Папка `data/` в git не хранится: входные файлы каждый кладёт к себе сам.

## Входные файлы

| файл | откуда | кто читает |
|---|---|---|
| `data/raw/kazakhstan-261118.osm.pbf` | снимок OSM Geofabrik от 2026-11-18, 226 496 253 байт, md5 `57fd297ad55df539e0ec233312922ca0` | 03 |
| `data/processed/almaty_boundary.gpkg` | 02, ячейка «ID кандидатов, граница и сетка» | 03 |
| `data/processed/candidate_locations.gpkg` | 02, там же; столбец `candidate_id` = 0…2 821 | 03, объединение признаков |
| `data/processed/cafes_points.gpkg` | 01 | 02 |
| `data/processed/almaty_residential.gpkg`, `almaty_offices.gpkg`, `almaty_office_pois.gpkg`, `almaty_office_points.gpkg`, `almaty_universities_polygons.gpkg`, `almaty_universities_points.gpkg`, `almaty_colleges_polygons.gpkg`, `almaty_colleges_points.gpkg`, `almaty_schools_polygons.gpkg`, `almaty_schools_points.gpkg` | подготовленные слои OSM | 02 |

**PBF.** Имя и md5 снимка заданы в 03 явно (`PBF_NAME`, `PBF_MD5`). Если md5 файла другой, 03 останавливается: значит, это другой снимок.

- Geofabrik хранит дневные снимки только последние 7 дней (долго хранятся лишь снимки на 1-е число месяца). После 2026-11-25 берите `kazakhstan-261118.osm.pbf` у команды, а не по ссылке.
- Если файла нет, 03 пробует скачать его с `https://download.geofabrik.de/asia/kazakhstan/`.
- Опубликованный Geofabrik файл `kazakhstan-261118.osm.pbf.md5` (`e7568a2f…`) не совпадает с файлом, который они раздают. Две независимые загрузки дали побайтно одинаковый файл с md5 `57fd297a…`, и GDAL читает его полностью без ошибок. В 03 зафиксирован md5 фактического файла.

## Что пишет 03

- `data/raw/almaty_osm_extract.gpkg` — вырезка из PBF. Слои `points`, `lines`, `multipolygons` берутся в зоне «город + 2 км» для счётчиков в радиусе 500–1000 м. `major_roads` и `metro_stations` — в зоне «город + 30 км» для расстояний. Таблица `extract_meta` хранит md5 PBF, хеш границы, буферы и версию кода вырезки. Вырезка пересобирается сама, если изменился PBF, граница, буфер или список крупных дорог либо в файле не хватает слоёв. Удалять её вручную не нужно.
- `data/processed/features/<признак>.csv` — `candidate_id, x, y, <признак>` для каждого из 8 признаков; `osm_commercial_features.csv` — все 8 сразу.
- `data/processed/qa/<слой>_duplicate_pairs.csv` — все пары-кандидаты в дубли. В отчёте: OSM ID обеих записей (объект открывается по `https://www.openstreetmap.org/<osm_1>`), решение, причина и `map_url` — ссылка на карту в середине пары. `qa/stop_positions.csv` — статус каждой `stop_position`.

В конце 03 печатает sha256 файла `osm_commercial_features.csv`, md5 PBF и хеш границы. Два прогона на одинаковых входах дают одинаковый sha256: по нему удобно сверить свои признаки с командой.

Эталон для `kazakhstan-261118.osm.pbf` (md5 `57fd297a…`) и границы из 02 (sha256 `e5f3da403469…`) на версиях из раздела «Зависимости»: sha256 признаков `bc82df51da6cd84e3cb134180fcf745f477a66be5de89b9da476e07e76669a79`.

## Догрузка 2ГИС

`notebooks/03_2gis_gap_fill.ipynb` дополняет основной сбор 2ГИС (`03_2gis_data_safe.ipynb`). Он ставит дополнительные центры поиска для кандидатов, которые остались дальше 1 км от центров основного сбора, и ищет тем же запросом. Каждый такой кандидат оказывается не дальше 500 м от нового центра, поэтому его круг 500 м целиком входит в поиск радиусом 1 км.

- Вход — `data/raw/2gis_coverage_gap_candidates.csv` и файлы 02.
- Ключ — `DGIS_API_KEY` в окружении или строкой `DGIS_API_KEY=...` в `.env`.
- С настройками по умолчанию ноутбук к API не обращается.
- `candidate_id` берётся по координатам: `candidate_row` в CSV на 1 больше.
- Результат лежит в `data/processed/2gis_gap/`:
  - `gap_organizations.csv` — найденные заведения. ID 2ГИС записан текстом, читайте файл с `dtype={"id": str}`.
  - `gap_centers_status.csv` — статус каждого центра.
  - `gap_summary.json` — сводка, в том числе число HTTP-попыток.

## Дополнительные поля 2ГИС

`notebooks/03_2gis_enrich.ipynb` догружает поля уже найденных заведений через `/3.0/items/byid`. Один запрос берёт до 100 ID, поэтому на ~1 300 заведений уходит около 13–15 запросов. Догружаются:

- рубрики;
- организация и бренд с числом филиалов;
- часы работы;
- флаг «временно не работает»;
- здание, если 2ГИС его указывает;
- атрибуты: средний чек, число мест, кофе с собой, Wi-Fi, «можно с ноутбуком»;
- даты карточки.

- Входы: файлы прогресса поиска `data/processed/2gis_almaty_progress.sqlite3` (из `03_2gis_data_safe.ipynb`) и `data/processed/2gis_gap/gap_progress.sqlite3`.
- Результат: `data/processed/2gis_enriched/organizations_enriched.csv` (`id` записан текстом).
- Признаки кандидатов здесь не считаются, их считает `03_2gis_all_coffee.ipynb`: там те же поля приходят сразу при поиске.

## Все кофейни Алматы из 2ГИС

`notebooks/03_2gis_all_coffee.ipynb` собирает все заведения рубрик 2ГИС «Кофейни» (ID 162) и «Точки кофе» (ID 58871) в области «граница города + 1 км» (`region_id = 67`). В 2ГИС их 1 100 и 570; кофейные автоматы и розничная продажа кофе не собираются.

Демо-ключ отдаёт не больше 50 записей на запрос, поэтому область делится на квадраты (квадродерево):
- если в круге квадрата больше 50 заведений, квадрат делится на 4;
- если 50 или меньше, выкачиваются все страницы, и квадрат получает статус `complete`.

Все поля (рубрики, сеть, часы, средний чек и т. п.) приходят в тех же запросах. Лимит демо-ключа — 1 000 запросов на весь срок ключа.

Сбор 2026-11-21 занял 384 запроса: 175 квадратов, глубина деления до 6. Внутри области нашлось 1 461 заведение — 971 кофейня и 490 точек кофе. Все квадраты получили статус `complete`, кроме одного `short`: 2ГИС заявил 44 заведения, а вернул 43. Из-за него у 11 кандидатов `dgis_search_complete_500m = False`.

- Результат:
  - `data/processed/2gis_coffee/almaty_coffee_2gis.csv` — заведения, `id` записан текстом;
  - `cells.csv` и `summary.json` — квадраты, их статусы и число попыток;
  - `data/processed/features/dgis_coffee_features.csv` — признаки кандидатов в радиусе 500 м: кофейни, из них сетевые и открытые к 8:00, сумма отзывов, средний рейтинг, медианный средний чек, точки кофе и флаг `dgis_search_complete_500m`.
- Код клиента 2ГИС общий для всех 2ГИС-ноутбуков и лежит в `notebooks/dgis_api.py`.

## Слои спроса и конкурентов из 2ГИС

`notebooks/03_2gis_demand_layers.ipynb` тем же способом собирает другие рубрики 2ГИС и идёт по ним в порядке ценности, пока не кончится лимит:

1. `offices` — бизнес-центры и коворкинги;
2. `bakeries` — пекарни и кафе-кондитерские;
3. `fitness` — фитнес-клубы и тренажёрные залы;
4. `education` — вузы и колледжи;
5. `malls` — торговые центры и ТРЦ;
6. `vending` — кофейные автоматы.

У всех слоёв общий бюджет `GLOBAL_BUDGET`. Если лимит ключа кончится, последний слой останется неполным. Это видно по флагу `dgis_<слой>_complete_1000m`.

Сбор 2026-11-21/22 занял 628 запросов. Внутри области (город + 1 км), без временно закрытых:

| слой | объектов | собран полностью |
|---|---|---|
| `offices` | 373 (бизнес-центров 305, коворкингов 70) | да |
| `bakeries` | 512 | да |
| `fitness` | 443 | да |
| `education` | 266 | да |
| `malls` | 194 | да |
| `vending` | 485 | нет: осталось 32 квадрата |

На кофейных автоматах кончился лимит демо-ключа, и 2ГИС ответил `403 key is blocked`. Круг 1 000 м собран полностью у 2 251 кандидата из 2 822. Слой `bakeries` частично пересекается с кофейнями, потому что у некоторых заведений есть обе рубрики.

- Объекты слоёв: `data/processed/2gis_layers/<слой>.csv`.
- Сводка запросов: `summary.json` там же.
- Признаки кандидатов: `data/processed/features/dgis_demand_features.csv`. По каждому слою там число объектов в 500 и 1 000 м и флаг полноты.

## Объединение признаков

Признаки объединяются с сеткой только по `candidate_id`, с двумя проверками. Первая — `validate="one_to_one"`: нет ни потерянных, ни лишних точек. Вторая — `x/y` из CSV совпадают с геометрией сетки (`rtol=0`, `atol=0.01` м). В 03 это делает функция `attach_features`:

```python
grid = gpd.read_file("../data/processed/candidate_locations.gpkg")[["candidate_id", "geometry"]]
table = pd.read_csv("../data/processed/features/osm_commercial_features.csv")
merged = grid.merge(table, on="candidate_id", how="outer", validate="one_to_one", indicator=True)
assert (merged["_merge"] == "both").all()
assert np.isclose(merged.geometry.x, merged["x"], rtol=0, atol=0.01).all()
assert np.isclose(merged.geometry.y, merged["y"], rtol=0, atol=0.01).all()
```

## Зависимости

Python 3.11+. Версии, на которых проверен этап 02 → 03:

```
geopandas==1.2.0 pandas==3.1.1 shapely==2.2.0 pyogrio==0.13.0 numpy==2.4.6
networkx==3.6.1 matplotlib==3.11.2 osmnx==2.1.1 pyproj==3.7.2 ipykernel
```

- `pyogrio` из PyPI идёт с GDAL 3.12, в котором есть драйвер OSM для чтения `.osm.pbf`.
- `osmnx` нужен в 01–02 (граница города, слои OSM).
- `pyarrow` нужен в 02 для `almaty_features.parquet`.

```bash
python -m venv .venv
.venv/Scripts/python -m pip install geopandas==1.2.0 pandas==3.1.1 shapely==2.2.0 pyogrio==0.13.0 numpy==2.4.6 networkx==3.6.1 matplotlib==3.11.2 osmnx==2.1.1 pyproj==3.7.2 pyarrow ipykernel
```

(на macOS/Linux — `.venv/bin/python`).
