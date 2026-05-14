# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Python CLI (`nasr`) that turns the FAA's 28-day NASR CSV subscription plus
the daily DOF obstacle file into three SQLite/SpatiaLite databases:

- `nasr.sqlite` — one TEXT-column table per NASR CSV, plus POINT geometry columns and spatial indexes added in-place by `build-spatial`.
- `class_airspace_spatialite.sqlite` — Class B/C/D/E polygons from `Class_Airspace.shp`.
- `special_use_airspace_spatialite.sqlite` — MOA / restricted / prohibited areas from the SAA AIXM XML.

The project was rewritten from a Perl pipeline in early 2026; the input format
also moved from the legacy fixed-width NASR files to the CSV subscription, so
column names and table layouts now follow FAA's CSV conventions
(`LAT_DECIMAL`, `LONG_DECIMAL`, `ARPT_ID`, etc.) rather than the old
Perl-driven `<file>_<recordType>` naming.

## Common commands

```sh
uv sync                          # install dev + runtime deps
uv run nasr --help               # show CLI
uv run pytest -q                 # run tests

container build -t faa-nasr .    # build container image (or `docker build`)
container run --rm -v "$PWD/out":/data faa-nasr build --out /data --work-dir /data/work
```

CLI subcommands (see `nasr <cmd> --help`):

- `fetch` — download the current/next NASR subscription and (optionally) DAILY_DOF_CSV.ZIP.
- `build-tables <csv-dir>` — load every `*.csv` (skipping `*_CSV_DATA_STRUCTURE.csv`) into a fresh SQLite DB.
- `build-spatial <db>` — add SpatiaLite geometry + spatial indexes to an existing NASR DB in place. Idempotent.
- `build-airspace <nasr-dir>` — convert the class-airspace shapefile and SAA AIXM XML.
- `fetch-edai` / `build-edai` — download FAA EDAI shapefile datasets and build `edai_spatialite.sqlite`.
- `fetch-weather` — pull current METAR / TAF / PIREP / AIRMET-SIGMET / international-SIGMET into `weather.sqlite`. Realtime, not cycle-bound.
- `fetch-tfrs` — pull active TFR polygons (WFS) + metadata (JSON list API) into `tfrs.sqlite`.
- `build` — end-to-end (fetch → tables → spatial → airspace → edai). Does **not** include `fetch-weather` / `fetch-tfrs`, which are realtime feeds run on their own cadence.

## Architecture

Each pipeline stage lives in one `src/faa_nasr/*.py` module, all wired through
`cli.py` (typer). Modules are independent: each takes filesystem paths in,
writes outputs out, and has no shared state.

- `fetch.py` — calls `https://external-api.faa.gov/apra/nfdc/nasr/chart?edition=current` to get the SUBSCRIBER product URL, downloads it, and recursively extracts the inner `CSV_Data/<DD_MMM_YYYY>_CSV.zip`. Also pulls `https://aeronav.faa.gov/Obst_Data/DAILY_DOF_CSV.ZIP` when `--obstacles` is set.
- `tables.py` — generic CSV loader. For each `*.csv` (excluding the schema-describing `*_CSV_DATA_STRUCTURE.csv`), reads the header, creates a TEXT-column table named after the file stem, and bulk-inserts in one transaction. The DOF.CSV becomes table `OBSTACLE`.
- `geometry.py` — opens the SQLite DB *in place*, calls `enable_load_extension(True)`, tries multiple paths for `mod_spatialite` (Debian multi-arch first, then Homebrew), then applies a static table-to-geometry mapping (`_POINT_GEOMS`) to add `geometry POINT/4326` columns + spatial indexes for the tables that have `LAT_DECIMAL`/`LONG_DECIMAL` (or `LATDEC`/`LONDEC` for OBSTACLE). Idempotent: skips tables that already have a registered geometry column. Two-pass: populate all geometries first, then build R-tree indexes (avoids per-row trigger cost during the bulk UPDATE).
- `airspace.py` — uses `pyogrio.raw.read`/`write` (low-level numpy API; no geopandas dep) to copy each shapefile/AIXM layer into a SpatiaLite DB. Forces `MultiPolygon Z` (etc.) + `promote_to_multi=True` because the class-airspace shapefile mixes single and multi geometries in one layer. The SAA file is a zip-of-zips (outer `SaaSubscriberFile.zip` contains the AIXM schema bundle plus an inner `Saa_Sub_File.zip` with the per-airspace XML feature files), so `_extract_recursive` is used.
- `coords.py` — DMS↔decimal helpers. Rarely needed because the CSV subscription already provides decimal columns; kept as a fallback.
- `weather.py` — fetches the 5 aviationweather.gov GeoJSON feeds (`/api/data/metar`, `/taf`, `/pirep`, `/airsigmet`, `/isigmet`), caches each to `weather_cache/<layer>.geojson`, then writes one SpatiaLite layer per feed in `weather.sqlite`. METAR/TAF/PIREP require a bbox or station list -- a global bbox (`15,-180,75,-60`) is hardcoded; airsigmet/isigmet accept it harmlessly. Port of the jlmcgraw/aviationMapMetarSigmetsAndTFRs Perl scraper (the original repo's ADDS URLs are retired).
- `tfr.py` — fetches active TFR polygons from `tfr.faa.gov/geoserver/TFR/ows` (WFS, `TFR:V_TFR_LOC`, GeoJSON in EPSG:4326), enriches each feature with metadata from `tfrapi/getTfrList` joined on the leading `notam_id` segment of `NOTAM_KEY`, then writes a `tfrs` POLYGON layer + an attribute-only `tfrs_no_shape` table (TFRs whose polygon hasn't been published yet, from `tfrapi/noShapeTfrList`). The enrichment dedupes case-insensitively because SQLite identifiers are case-insensitive (WFS `STATE` vs list-API `state`).

To add support for a new NASR CSV (the FAA occasionally publishes new subjects):
nothing to do — `tables.py` discovers files by glob and creates tables from
their headers. If the new file has decimal lon/lat that should become spatial,
add a `PointGeom(...)` entry to `_POINT_GEOMS` in `geometry.py`.

## Container notes (Docker + Apple `container`)

The `Dockerfile` is intentionally vanilla — no BuildKit-only mounts, no
platform-specific RUN options — so the same file works with both Docker and
Apple's `container` CLI. Key points:

- Base: `python:3.12-slim-bookworm`. Apt only installs `ca-certificates` and `libsqlite3-mod-spatialite`. **No `gdal-bin` / `libgdal-dev`** — pyogrio's wheel bundles its own GDAL, and adding the system packages roughly doubles image size.
- `uv` is copied from `ghcr.io/astral-sh/uv:0.11` to avoid `curl | sh` install patterns that have inconsistent behavior across OCI builders.
- `ENTRYPOINT ["nasr"]`, default workdir `/data`. Mount your output dir at `/data` and the CLI writes there.

The Debian package puts `mod_spatialite.so` at `/usr/lib/aarch64-linux-gnu/` (or `x86_64-linux-gnu/` on Intel). Both paths are in `_MOD_SPATIALITE_CANDIDATES` in `geometry.py`.

## Non-obvious gotchas

- **Schema vs data CSVs**: the CSV bundle ships `*_CSV_DATA_STRUCTURE.csv` files that describe the schema (column name, max length, type, nullable). `tables.py` skips those by suffix; if you ever want typed columns, parse them.
- **SpatiaLite trusted_schema**: SpatiaLite 5+ uses `RTreeAlign()` inside `CreateSpatialIndex`. SQLite's untrusted-schema guard otherwise rejects it, leaving spatial indexes structurally present but with NULL data. `geometry.py` sets `PRAGMA trusted_schema=ON` before any `CreateSpatialIndex` call.
- **macOS system sqlite3** is built with `SQLITE_OMIT_LOAD_EXTENSION` and cannot load `mod_spatialite`. Python's stdlib `sqlite3` (which the CLI uses) does support extensions — just not the system `sqlite3` shell. Don't try to debug spatialite issues with `/usr/bin/sqlite3`.
- **Apple `container` storage**: each rebuild + run cycle adds 0.5–1 GB of layer cache to `~/Library/Application Support/com.apple.container/`. `container image prune -a` is the safe way to reclaim it (only removes images with no running container reference; bases get re-pulled lazily).

## Tests

Just `tests/test_coords.py` for now — the CSV/spatialite/airspace stages are
validated empirically against a real NASR cycle (see commit history for the
verification queries).
