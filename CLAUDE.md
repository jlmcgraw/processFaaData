# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Python CLI (`nasr`) that turns aviation-data-mirror's local copy of the 28-day NASR
CSV subscription plus the daily DOF obstacle file into SQLite/SpatiaLite
databases:

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
uv run aviation-data-mirror manifest --root ./aviation_data --out ./aviation_data/manifest.json

container build -t faa-nasr .    # build container image (or `docker build`)
container run --rm -v "$PWD/out":/data -v "$PWD/aviation_data":/aviation_data:ro faa-nasr build --out /data --mirror-root /aviation_data
```

CLI subcommands (see `nasr <cmd> --help`):

- `build-tables <csv-dir>` — load every `*.csv` (skipping `*_CSV_DATA_STRUCTURE.csv`) into a fresh SQLite DB.
- `build-spatial <db>` — add SpatiaLite geometry + spatial indexes to an existing NASR DB in place. Idempotent.
- `build-airspace <nasr-dir>` — convert the class-airspace shapefile and SAA AIXM XML.
- `build-edai` — build `edai_spatialite.sqlite` from extracted EDAI shapefiles in aviation-data-mirror.
- `build-cifp <FAACIFP18>` — parse aviation-data-mirror's extracted CIFP file into `cifp.sqlite`.
- `build-cifp-spatial <cifp.sqlite>` — add LINESTRING procedure-path geometries to an existing `cifp.sqlite`. Requires SpatiaLite; run inside the container.
- `fetch-weather` — pull current METAR / TAF / PIREP / AIRMET-SIGMET / international-SIGMET into `weather.sqlite`. Realtime, not cycle-bound.
- `fetch-tfrs` — pull active TFR polygons (WFS) + metadata (JSON list API) into `tfrs.sqlite`.
- `build` — end-to-end from `aviation_data/manifest.json` (tables → spatial → airspace → edai → cifp). Does **not** include `fetch-weather` / `fetch-tfrs`, which are realtime feeds run on their own cadence.

## Architecture

Each pipeline stage lives in one `src/faa_nasr/*.py` module, all wired through
`cli.py` (typer). Modules are independent: each takes filesystem paths in,
writes outputs out, and has no shared state.

- `_log.py` — thin wrapper around stderr logging; `set_quiet()` suppresses tqdm progress bars when `--quiet` is passed.
- `mirror.py` — reads `aviation_data/manifest.json`, selects downloaded artifacts, and resolves extracted NASR CSV, DOF CSV, CIFP, and EDAI paths. Cycle-based acquisition is intentionally owned by the separate `aviation-data-mirror` tool. For obstacle data it only selects artifacts whose name/format/filename mention "csv" (i.e. the `daily_dof` product with `csv_zip` format containing `DOF.CSV`); the legacy fixed-width `dof` `dat_zip` artifact is silently skipped.
- `tables.py` — generic CSV loader. For each `*.csv` (excluding the schema-describing `*_CSV_DATA_STRUCTURE.csv`), reads the header, creates a TEXT-column table named after the file stem, and bulk-inserts in one transaction. The DOF.CSV becomes table `OBSTACLE`.
- `geometry.py` — opens the SQLite DB *in place*, calls `enable_load_extension(True)`, tries multiple paths for `mod_spatialite` (Debian multi-arch first, then Homebrew), then applies a static table-to-geometry mapping (`_POINT_GEOMS`) to add `geometry POINT/4326` columns + spatial indexes for the tables that have `LAT_DECIMAL`/`LONG_DECIMAL` (or `LATDEC`/`LONDEC` for OBSTACLE). Idempotent: skips tables that already have a registered geometry column. Two-pass: populate all geometries first, then build R-tree indexes (avoids per-row trigger cost during the bulk UPDATE).
- `airspace.py` — uses `pyogrio.raw.read`/`write` (low-level numpy API; no geopandas dep) to copy each shapefile/AIXM layer into a SpatiaLite DB. Forces `MultiPolygon Z` (etc.) + `promote_to_multi=True` because the class-airspace shapefile mixes single and multi geometries in one layer. The SAA file is a zip-of-zips (outer `SaaSubscriberFile.zip` contains the AIXM schema bundle plus an inner `Saa_Sub_File.zip` with the per-airspace XML feature files), so `_extract_recursive` is used.
- `coords.py` — DMS↔decimal helpers. Rarely needed because the CSV subscription already provides decimal columns; kept as a fallback.
- `cifp.py` — parses the ARINC 424-format `FAACIFP18` file into SQLite. `build()` creates one table per record section/subsection using field specs from `cifp_records.py`. `build_spatial()` adds a `procedure_paths` SpatiaLite table with LINESTRING geometries for IAP/SID/STAR legs (turn-anticipation arcs computed from design speed).
- `cifp_records.py` — static field layout specs for every ARINC 424 section/subsection handled by the parser (primary and continuation records).
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

- **DOF product vs daily_dof product**: the FAA mirror may download two obstacle artifacts. The `dof` product (`dat_zip` format) contains per-state fixed-width `.Dat` files and is not parseable by `tables.py`. The `daily_dof` product (`csv_zip` format, APRA path `ddof/chart`) extracts to `DOF.CSV` and is what becomes the `OBSTACLE` table. `mirror.py` only selects artifacts whose name/format mention "csv", so the `dat_zip` artifact is silently skipped. If the `OBSTACLE` table is missing, check that `aviation-data-mirror` is configured to download `daily_dof` with `apra_editions: ["current"]`.
- **Schema vs data CSVs**: the CSV bundle ships `*_CSV_DATA_STRUCTURE.csv` files that describe the schema (column name, max length, type, nullable). `tables.py` skips those by suffix; if you ever want typed columns, parse them.
- **SpatiaLite trusted_schema**: SpatiaLite 5+ uses `RTreeAlign()` inside `CreateSpatialIndex`. SQLite's untrusted-schema guard otherwise rejects it, leaving spatial indexes structurally present but with NULL data. `geometry.py` sets `PRAGMA trusted_schema=ON` before any `CreateSpatialIndex` call.
- **macOS system sqlite3** is built with `SQLITE_OMIT_LOAD_EXTENSION` and cannot load `mod_spatialite`. Python's stdlib `sqlite3` (which the CLI uses) does support extensions — just not the system `sqlite3` shell. Don't try to debug spatialite issues with `/usr/bin/sqlite3`.
- **Apple `container` storage**: each rebuild + run cycle adds 0.5–1 GB of layer cache to `~/Library/Application Support/com.apple.container/`. `container image prune -a` is the safe way to reclaim it (only removes images with no running container reference; bases get re-pulled lazily).

## Tests

`uv run pytest -q` runs the full suite. Coverage spans:

- `test_coords.py` — DMS↔decimal helpers
- `test_tables.py`, `test_tables_helpers.py` — CSV loader and column-sanitisation helpers
- `test_geometry_helpers.py`, `test_geometry_loader.py`, `test_init_spatialite_db.py` — spatialite geometry builder
- `test_airspace_helpers.py`, `test_airspace_io.py`, `test_airspace_orchestrators.py`, `test_airspace_pyogrio_mocked.py` — class-airspace and SAA AIXM pipeline
- `test_cifp_coords.py`, `test_cifp_fields.py`, `test_cifp_parse.py` — CIFP/ARINC 424 parser
- `test_edai.py` — EDAI shapefile builder
- `test_mirror.py` — manifest artifact resolution (NASR, daily_dof, CIFP, EDAI)
- `test_weather.py`, `test_tfr.py` — weather and TFR fetch pipelines
- `test_cli.py` — CLI entry-point wiring
- `test_log.py`, `test_merge_chunks.py`, `test_xlinks.py` — utilities

The spatialite stages are also validated empirically against a real NASR cycle.

### Code Intelligence

Prefer LSP over Grep/Read for code navigation — it's faster, precise, and avoids reading entire files:
- `workspaceSymbol` to find where something is defined
- `findReferences` to see all usages across the codebase
- `goToDefinition` / `goToImplementation` to jump to source
- `hover` for type info without reading the file

Use Grep only when LSP isn't available or for text/pattern searches (comments, strings, config).

After writing or editing code, check LSP diagnostics and fix errors before proceeding.
