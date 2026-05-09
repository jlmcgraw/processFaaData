# faa-nasr

Build SQLite and SpatiaLite databases from the FAA's 28-day NASR CSV
subscription. The output databases can be queried directly or used as a data
source by Electronic Flight Bag (EFB) software, mapping projects, etc.

The pipeline produces four files:

| File | Contents |
|---|---|
| `nasr.sqlite` | One SQLite table per NASR CSV (e.g. `APT_BASE`, `APT_RWY`, `NAV_BASE`, `FIX_BASE`, `OBSTACLE`). |
| `spatialite_nasr.sqlite` | Same as above plus SpatiaLite POINT geometry columns and spatial indexes for the airport, navaid, fix, AWOS, ILS, FSS, ATC, holding-pattern, and obstacle tables. |
| `controlled_airspace_spatialite.sqlite` | Class B/C/D/E controlled airspace polygons, from `Class_Airspace.shp`. |
| `special_use_airspace_spatialite.sqlite` | Special use airspace (MOAs, restricted/prohibited areas, etc.) from the SAA AIXM XML. |

## Quick start (recommended: containerized)

The easiest way to run this is in a container — it brings its own
`mod_spatialite` and bundled GDAL via `pyogrio`, so you don't need to install
anything on the host.

Works with both Apple's `container` CLI and Docker:

```sh
# Build the image
container build -t faa-nasr .       # or: docker build -t faa-nasr .

# Download + build everything into ./out (takes a few minutes; ~250 MB download)
mkdir -p out
container run --rm -v "$PWD/out":/data faa-nasr build --out /data --work-dir /data/work
```

After it finishes, `out/` contains the four `.sqlite` files.

## CLI

```sh
nasr fetch          [--out DIR] [--edition current|next] [--obstacles/--no-obstacles]
nasr build-tables   <csv-dir>     [--db nasr.sqlite] [--obstacle-csv DOF.CSV]
nasr build-spatial  <src.sqlite>  [--db spatialite_nasr.sqlite]
nasr build-airspace <nasr-dir>    [--out DIR]
nasr build                        [--out DIR] [--work-dir DIR] [--edition current|next]
```

`build` is the end-to-end pipeline (fetch → build-tables → build-spatial →
build-airspace). The intermediate subcommands let you run from already-extracted
data when iterating.

## Local (non-container) install

Requires Python 3.12+, [`uv`](https://docs.astral.sh/uv/), and (for
`build-spatial`) the `mod_spatialite` SQLite extension installed somewhere
the loader can find it.

```sh
uv sync
uv run nasr --help
```

On macOS, install spatialite with `brew install libspatialite`. Note that the
system `/usr/bin/sqlite3` is built without `--enable-loadable-sqlite-extensions`,
so the spatial step requires Python's stdlib `sqlite3` (which supports it) or
Homebrew's sqlite — but Python is what this CLI uses, so that's automatic.

## Disclaimer

This software and the data it produces come with no guarantees about accuracy
or usefulness whatsoever! Don't use it when your life may be on the line.

— Jesse McGraw, jlmcgraw@gmail.com
