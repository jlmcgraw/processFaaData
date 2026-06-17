# faa-nasr

Build SQLite and SpatiaLite databases from aviation-data-mirror data for the 28-day
NASR CSV subscription and Coded Instrument Flight Procedures (CIFP). The output
databases can be queried directly or used as a data source by Electronic
Flight Bag (EFB) software, mapping projects, etc.

This is a heavily AI-assisted conversion from the original perl-based parser.  It will process the CSV data from NASR 
along with data sets that were previously in separate projects (eg EDAI and METARs/TFRs)

First you will need to download all data locally using [aviation-data-mirror](https://github.com/jlmcgraw/aviation-data-mirror)

The [AviationMap](https://github.com/jlmcgraw/aviationMap) project will be updated to use this data

## Quick start

Before running the 28-day build, use [aviation-data-mirror](https://github.com/jlmcgraw/aviation-data-mirror) to download the source
products and generate its manifest

Make a link to where that data was downloaded:
```shell
ln -s -f /aviation_data_mirror/aviation_data ./aviation_data
```

Then run `make container`.  This should work with both Apple's `container` CLI and Docker:
```shell
make container
```

Data acquisition for cycle-based FAA products is handled by the separate
[aviation-data-mirror](https://github.com/jlmcgraw/aviation-data-mirror) tool. This project consumes `aviation_data/manifest.json` and the
local paths it describes; it no longer downloads NASR, CIFP, DOF, or EDAI
cycle artifacts itself.

The pipeline produces several files:

| File | Contents | Cadence |
|---|---|---|
| `nasr.sqlite` | One SQLite table per NASR CSV (e.g. `APT_BASE`, `APT_RWY`, `NAV_BASE`, `FIX_BASE`, `OBSTACLE`), plus SpatiaLite POINT geometry columns and spatial indexes for the airport, navaid, fix, AWOS, ILS, FSS, ATC, holding-pattern, and obstacle tables. | 28-day cycle |
| `class_airspace_spatialite.sqlite` | Class B/C/D/E airspace polygons, from `Class_Airspace.shp`. | 28-day cycle |
| `special_use_airspace_spatialite.sqlite` | Special use airspace (MOAs, restricted/prohibited areas, etc.) from the SAA AIXM XML. | 28-day cycle |
| `cifp.sqlite` | Instrument procedures (SIDs, STARs, approaches), navaids, airways, waypoints, airspace boundaries, and more — parsed from the FAA's CIFP (ARINC 424 format). One table per record type. | 28-day cycle |
| `edai_spatialite.sqlite` *(optional)* | Built from aviation-data-mirror's extracted EDAI shapefile artifacts. Parallel to NASR data and included in `nasr build` by default. | 28-day cycle |
| `weather.sqlite` | Current METARs, TAFs, PIREPs, AIRMETs/SIGMETs, and international SIGMETs as SpatiaLite layers. Realtime feed, not cycle-bound. | On demand |
| `tfrs.sqlite` | Active TFR polygons and metadata from FAA's TFR WFS + list API. Realtime feed, not cycle-bound. | On demand |


## CIFP — Coded Instrument Flight Procedures

The FAA publishes the CIFP as a fixed-width text file (`FAACIFP18`) in
ARINC 424-18
format on the same 28-day cycle as NASR. It contains the authoritative
definition of every published instrument procedure in the US national airspace,
as well as the underlying infrastructure they reference.

### What's in `cifp.sqlite`

Each ARINC 424 section/subsection becomes its own SQLite table. Table names
follow the convention `primary_{SectionCode}_{SubSectionCode}_base_{Description}`.
The main tables are:

| Table | Contents |
|---|---|
| `primary_P_A_base_Airport - Reference Points` | Airport reference points (lat/lon, elevation, name) |
| `primary_P_G_base_Airport - Runways` | Runway geometry, bearing, length, threshold data |
| `primary_P_F_base_Airport - Approach Procedures` | Approach procedure legs (IAP, VOR, ILS, RNAV/RNP, LPV…) |
| `primary_P_D_base_Airport - SIDs` | Standard Instrument Departure legs |
| `primary_P_E_base_Airport - STARs` | Standard Terminal Arrival Route legs |
| `primary_P_I_base_Airport - Localizer/Glide Slope` | ILS localizer and glideslope data |
| `primary_P_C_base_Airport - Terminal Waypoints` | Terminal area fixes |
| `primary_P_S_base_Airport - MSA` | Minimum Safe Altitude sectors |
| `primary_P_V_base_Airport - Communications` | Airport communication frequencies |
| `primary_D__base_Navaid - VHF Navaid` | VORs and VOR/DMEs |
| `primary_D_B_base_Navaid - NDB Navaid` | NDB beacons |
| `primary_E_A_base_Enroute - Grid Waypoints` | Enroute fixes |
| `primary_E_R_base_Enroute - Airways and Routes` | Victor and jet airway segments |
| `primary_E_P_base_Enroute - Holding Patterns` | Published holding patterns |
| `primary_U_C_base_Airspace - Controlled Airspace` | Class B/C/D/E boundary points |
| `primary_U_R_base_Airspace - Restrictive Airspace` | MOA/restricted/prohibited boundary points |
| `primary_H_*_base_Heliport - *` | Heliport equivalents of the airport tables above |

Every field whose name ends in `latitude` or `longitude` gets a companion
`*_WGS84` column containing the pre-converted decimal-degree value, so you
can query coordinates without parsing the raw ARINC 424 DMS format yourself.

Continuation records (e.g. SBAS authorization data for LPV approaches, or
restrictive airspace time-of-use schedules) land in separate tables prefixed
with `continuation_`.

### Example queries

```sql
-- All airports with their coordinates
SELECT LandingFacilityIcaoIdentifier, AirportName,
       AirportReferencePtLatitude_WGS84,
       AirportReferencePtLongitude_WGS84
FROM "primary_P_A_base_Airport - Reference Points"
WHERE AirportName LIKE '%SEATTLE%';

-- All legs of the ILS RWY 28L approach at KSEA
SELECT SequenceNumber, FixIdentifier, PathAndTermination,
       AltitudeDescription, Altitude_1, Altitude_2
FROM "primary_P_F_base_Airport - Approach Procedures"
WHERE LandingFacilityIcaoIdentifier = 'KSEA'
  AND SIDSTARApproachIdentifier LIKE 'I28L%'
ORDER BY CAST(SequenceNumber AS INTEGER);

-- VOR positions
SELECT VORIdentifier, VORName,
       DMELatitude_WGS84, DMELongitude_WGS84,
       DMEElevation
FROM "primary_D__base_Navaid - VHF Navaid"
WHERE VORIdentifier = 'SEA';

-- Enroute airways through a fix
SELECT RouteIdentifier, SequenceNumber, FixIdentifier,
       MinimumAltitude_1, MaximumAltitude
FROM "primary_E_R_base_Enroute - Airways and Routes"
WHERE FixIdentifier = 'HISKU'
ORDER BY RouteIdentifier, CAST(SequenceNumber AS INTEGER);
```

## CLI

```sh
nasr build-tables   <csv-dir>  [--db nasr.sqlite] [--obstacle-csv DOF.CSV]
nasr build-spatial  <db>                                  # adds geometry in place
nasr build-airspace <nasr-dir> [--out DIR]
nasr build-edai                [--out DIR] [--edai-dir DIR] [--mirror-root DIR]
nasr build                     [--out DIR] [--work-dir DIR] [--mirror-root DIR] [--mirror-manifest FILE]

# CIFP — instrument procedures (run alongside or independently of NASR build)
nasr build-cifp     <FAACIFP18> [--db cifp.sqlite]        # parse → cifp.sqlite

# Realtime feeds (run independently of the 28-day build cycle)
nasr fetch-weather  [--out DIR]   # METARs, TAFs, PIREPs, AIRMETs/SIGMETs → weather.sqlite
nasr fetch-tfrs     [--out DIR]   # active TFR polygons + metadata → tfrs.sqlite
```

`build` is the end-to-end processing pipeline
(`aviation_data/manifest.json` → build-tables → build-spatial → build-airspace →
build-edai → build-cifp). It resolves these mirror products when present:

| Mirror product | Used for |
|---|---|
| `nasr` | NASR CSV tables and class/SAA airspace inputs |
| `daily_dof` or `dof` | Optional `OBSTACLE` table input |
| `cifp` | `FAACIFP18` input for `cifp.sqlite` |
| `edai` | EDAI shapefiles for `edai_spatialite.sqlite` |

The mirror must actually include a downloaded NASR subscriber artifact. If
your `aviation-data-mirror` configuration only mirrors chart products such as `cifp`,
`daily_dof`, `edai`, `ifr`, and `vfr`, add/sync the NASR subscriber product in
`aviation-data-mirror` first or pass explicit `--nasr-dir` and `--csv-dir` paths.

The manifest should expose extracted archive paths when `aviation-data-mirror` has them.
When a downloaded mirror artifact only has `paths.product.path`, `nasr build`
can expand ZIP inputs into its derived extraction cache at `<out>/work` (or
`--work-dir`). This is useful in containers because mirror manifests may
contain host-absolute paths while the mirror itself is mounted at
`--mirror-root`. The same work directory is used for derived SAA/AIXM
extraction so the mirror can remain mounted read-only.

The intermediate subcommands let you run from explicit local paths when
iterating. For example, pass `--csv-dir`, `--nasr-dir`, `--edai-dir`, or
`--cifp-file` to `build` to override the mirror-resolved paths.

`fetch-weather` and `fetch-tfrs` are **not** part of `build` - they are
realtime feeds that can be refreshed at any cadence (e.g. every few minutes
for TFRs, every 5–15 minutes for weather). Each overwrites its output file
on every run.

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
Homebrew's sqlite - but Python is what this CLI uses, so that's automatic.

## Disclaimer

This software and the data it produces come with no guarantees about accuracy
or usefulness whatsoever! Don't use it when your life may be on the line.

- Jesse McGraw, jlmcgraw@gmail.com
