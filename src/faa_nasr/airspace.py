"""Convert controlled-airspace shapefiles and SAA AIXM XML into spatialite databases.

Uses pyogrio's low-level numpy-based read/write API rather than read_dataframe so
we don't need geopandas + pandas at runtime.
"""

from __future__ import annotations

import re
import sqlite3
import warnings
import zipfile
from collections import defaultdict
from pathlib import Path

import numpy as np
import pyogrio
import pyogrio.raw
from tqdm import tqdm

from faa_nasr import _log

# OGR emits "Non closed ring detected" warnings on some SAA AIXM polygons.
# We can't fix the source data and pyogrio still returns the geometry, so the
# warning is just noise -- silence it instead of polluting every CLI run.
warnings.filterwarnings("ignore", message="Non closed ring detected", module="pyogrio")

# Reuse the spatialite extension loader from the geometry module.
from faa_nasr.geometry import _load_mod_spatialite  # noqa: E402

CONTROLLED_DB = "controlled_airspace_spatialite.sqlite"
SUA_DB = "special_use_airspace_spatialite.sqlite"


def build(nasr_dir: Path, out_dir: Path) -> None:
    """Build both airspace spatialite databases from an extracted NASR directory."""
    nasr_dir = nasr_dir.resolve()
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    _build_controlled(nasr_dir=nasr_dir, dst=out_dir / CONTROLLED_DB)
    _build_sua(nasr_dir=nasr_dir, dst=out_dir / SUA_DB)


def _build_controlled(nasr_dir: Path, dst: Path) -> None:
    """Convert all *.shp under Additional_Data/Shape_Files/ into a spatialite DB."""
    _log.step(f"build-airspace controlled -> {dst}")
    shape_dir = nasr_dir / "Additional_Data" / "Shape_Files"
    if not shape_dir.is_dir():
        _log.info(f"  no Shape_Files directory under {nasr_dir} -- skipping")
        return
    if dst.exists():
        dst.unlink()
    shapefiles = sorted(shape_dir.glob("*.shp"))
    total_features = 0
    bar = tqdm(shapefiles, desc="  shapefiles", unit="file", disable=_log.is_quiet(), leave=True)
    for shp in bar:
        bar.set_postfix_str(shp.name, refresh=False)
        total_features += _copy_shapefile(src=shp, dst=dst, layer_name=_safe_name(shp.stem))
    _log.info(f"  wrote {len(shapefiles)} shapefiles / {total_features:,} features")


def _build_sua(nasr_dir: Path, dst: Path) -> None:
    """Convert the SAA AIXM XML feature files into a spatialite DB.

    Every AIXM XML for a single airspace ships the same set of layers
    (Airspace, OrganisationAuthority, Unit, AirspaceUsage, ...). Rather than
    create one DB layer per (xml, source-layer) pair (~hundreds of tables, with
    pyogrio's per-write overhead growing linearly as the DB fills up), we
    merge same-named layers across all XMLs into one DB layer each.

    Each merged row gets a `_source_xml` column carrying the originating
    airspace name, so per-airspace queries are still possible.
    """
    _log.step(f"build-airspace special-use -> {dst}")
    saa_zip = nasr_dir / "Additional_Data" / "AIXM" / "SAA-AIXM_5_Schema" / "SaaSubscriberFile.zip"
    if not saa_zip.is_file():
        _log.info(f"  no SaaSubscriberFile.zip at {saa_zip} -- skipping")
        return

    extract_dir = saa_zip.parent / "extracted"
    extract_dir.mkdir(exist_ok=True)
    _extract_recursive(saa_zip, extract_dir)

    if dst.exists():
        dst.unlink()
    xml_files = [p for p in sorted(extract_dir.rglob("*.xml")) if "xsd" not in p.parts]

    # Pre-init SpatiaLite metadata in dst so subsequent writes (which may go
    # through raw sqlite3 first if the alphabetically-first layer is attribute-
    # only) don't end up with a plain SQLite DB that pyogrio later appends to
    # without setting up geometry_columns.
    _init_spatialite_db(dst)

    # Pass 1: discover layers across all XMLs, group by source-layer name.
    layer_buckets: dict[str, list[tuple[Path, str]]] = defaultdict(list)
    for xml in tqdm(
        xml_files, desc="  scanning XMLs", unit="file", disable=_log.is_quiet(), leave=False
    ):
        try:
            for source_layer, _ in pyogrio.list_layers(xml):
                layer_buckets[str(source_layer)].append((xml, str(source_layer)))
        except Exception:
            continue

    # Pass 2: read + concat + write each merged layer (no spatial index yet).
    geometry_layers: list[str] = []
    total_features = 0
    bar = tqdm(
        sorted(layer_buckets.items()),
        desc="  merged layers",
        unit="layer",
        disable=_log.is_quiet(),
        leave=True,
    )
    for source_name, sources in bar:
        bar.set_postfix_str(source_name, refresh=False)
        target = _safe_name(source_name)
        n, has_geom = _merge_and_write_layer(dst=dst, sources=sources, target_layer=target)
        if n > 0:
            total_features += n
            if has_geom:
                geometry_layers.append(target)

    # Pass 3: build spatial indexes on the merged geometry layers.
    if geometry_layers:
        conn = sqlite3.connect(dst)
        try:
            conn.enable_load_extension(True)
            _load_mod_spatialite(conn)
            conn.execute("PRAGMA trusted_schema = ON")
            for layer in tqdm(
                geometry_layers,
                desc="  spatial indexes",
                unit="layer",
                disable=_log.is_quiet(),
                leave=True,
            ):
                conn.execute("SELECT CreateSpatialIndex(?, ?)", (layer, "GEOMETRY"))
            conn.commit()
        finally:
            conn.close()

    _log.info(f"  wrote {len(layer_buckets)} layers / {total_features:,} features")


def _extract_recursive(zip_path: Path, dest: Path) -> None:
    """Extract `zip_path` into `dest`, then recursively extract any nested .zip files."""
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    for nested in list(dest.rglob("*.zip")):
        _extract_recursive(nested, nested.parent / nested.stem)


def _copy_shapefile(src: Path, dst: Path, layer_name: str) -> int:
    """Copy one shapefile's single layer into the spatialite DB."""
    try:
        meta, _fids, geometry, field_data = pyogrio.raw.read(src)
    except pyogrio.errors.DataSourceError:
        return 0
    if geometry is None or len(geometry) == 0:
        return 0
    pyogrio.raw.write(
        dst,
        geometry=geometry,
        field_data=field_data,
        fields=meta["fields"],
        geometry_type=_promote_geom_type(meta["geometry_type"]),
        crs=meta.get("crs"),
        layer=layer_name,
        driver="SQLite",
        dataset_options={"SPATIALITE": "YES"},
        layer_options={"SPATIAL_INDEX": "YES", "LAUNDER": "NO"},
        promote_to_multi=True,
        append=dst.exists(),
    )
    return len(geometry)


def _merge_and_write_layer(
    dst: Path, sources: list[tuple[Path, str]], target_layer: str
) -> tuple[int, bool]:
    """Read every (xml, source_layer) source, concat in memory, write once.

    Returns (row_count, has_geometry). AIXM XMLs for the same source layer name
    can differ in their field set (some sources include optional fields that
    others omit), so we accumulate per-field-name and pad missing fields with
    None for sources that don't supply them.

    For layers with no geometry across all sources, we fall back to a plain
    sqlite3 INSERT path since pyogrio.raw.write requires a valid geometry array.
    """
    geom_chunks: list[np.ndarray] = []
    field_chunks: dict[str, list[np.ndarray]] = defaultdict(list)
    source_row_counts: list[int] = []  # rows per source, parallel to sources iter
    source_xmls: list[str] = []
    geom_type: str | None = None
    crs = None
    sources_seen: list[Path] = []
    field_order: list[str] = []  # preserve first-seen field order for determinism
    field_seen: set[str] = set()

    for xml, source_layer in sources:
        try:
            meta, _fids, geometry, field_data = pyogrio.raw.read(xml, layer=source_layer)
        except (pyogrio.errors.DataSourceError, IndexError):
            continue
        names = list(meta["fields"])
        n_rows = len(field_data[0]) if field_data else (len(geometry) if geometry is not None else 0)
        if n_rows == 0:
            continue

        if geom_type is None:
            geom_type = meta.get("geometry_type")
            crs = meta.get("crs")

        for name in names:
            if name not in field_seen:
                field_order.append(name)
                field_seen.add(name)

        # Pad earlier sources for any newly-seen fields; the new source itself
        # supplies all the names already in field_order it has, missing ones
        # get filled below.
        for new_name in field_order:
            chunks = field_chunks[new_name]
            if len(chunks) < len(sources_seen):
                # Backfill: this field appeared after earlier sources were read.
                for prior_n in source_row_counts[len(chunks) :]:
                    chunks.append(np.array([None] * prior_n, dtype=object))

        present = dict(zip(names, field_data, strict=True))
        for name in field_order:
            if name in present:
                field_chunks[name].append(present[name])
            else:
                field_chunks[name].append(np.array([None] * n_rows, dtype=object))

        if geometry is not None and len(geometry) == n_rows:
            geom_chunks.append(geometry)
        else:
            geom_chunks.append(np.array([None] * n_rows, dtype=object))
        source_xmls.extend([xml.stem] * n_rows)
        source_row_counts.append(n_rows)
        sources_seen.append(xml)

    if not source_xmls:
        return 0, False

    geom = np.concatenate(geom_chunks)
    field_data = [np.concatenate(field_chunks[name]) for name in field_order]
    has_geometry = bool(geom_type) and any(g is not None for g in geom)

    # Append the per-row source-XML stem so callers can look up which airspace
    # each row came from (the AIXM `identifier` field is a UUID, not the name).
    fields_with_src = field_order + ["_source_xml"]
    field_data_with_src = field_data + [np.array(source_xmls, dtype=object)]

    if has_geometry:
        pyogrio.raw.write(
            dst,
            geometry=geom,
            field_data=field_data_with_src,
            fields=fields_with_src,
            geometry_type=_promote_geom_type(geom_type),
            crs=crs,
            layer=target_layer,
            driver="SQLite",
            dataset_options={"SPATIALITE": "YES"},
            layer_options={"SPATIAL_INDEX": "NO", "LAUNDER": "NO"},
            promote_to_multi=True,
            append=dst.exists(),
        )
    else:
        _write_attribute_only_table(dst, target_layer, fields_with_src, field_data_with_src)

    return len(source_xmls), has_geometry


def _init_spatialite_db(dst: Path) -> None:
    """Create `dst` as an empty SpatiaLite-initialized SQLite database."""
    conn = sqlite3.connect(dst)
    try:
        conn.enable_load_extension(True)
        _load_mod_spatialite(conn)
        conn.execute("PRAGMA trusted_schema = ON")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        conn.execute("SELECT InitSpatialMetadata(1)")
        conn.commit()
    finally:
        conn.close()


def _write_attribute_only_table(
    dst: Path, table: str, fields: list[str], field_data: list[np.ndarray]
) -> None:
    """Write a non-spatial table via raw sqlite3 (pyogrio.raw.write requires geometry)."""
    conn = sqlite3.connect(dst)
    try:
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        cols = ", ".join(f'"{c}" TEXT' for c in fields)
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" ({cols})')
        placeholders = ",".join("?" * len(fields))
        rows = (
            tuple("" if v is None else str(v) for v in row)
            for row in zip(*field_data, strict=True)
        )
        conn.executemany(f'INSERT INTO "{table}" VALUES ({placeholders})', rows)
        conn.commit()
    finally:
        conn.close()


def _promote_geom_type(geom_type: str | None) -> str | None:
    """Return the Multi* equivalent of a geometry type, preserving any 'Z'/'M' suffix."""
    if not geom_type:
        return geom_type
    base, _, suffix = geom_type.partition(" ")
    if base in ("Polygon", "LineString", "Point"):
        base = "Multi" + base
    return f"{base} {suffix}".strip()


_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9_]+")


def _safe_name(name: str) -> str:
    """Normalize a string for use as a SQL/SpatiaLite table name.

    SpatiaLite's geometry_columns table forbids single quotes (and other
    metadata constraints get touchy with spaces, commas, parens, hyphens).
    Source XML filenames like "O'NEILL MOA, NE.xml" become "O_NEILL_MOA_NE".
    """
    cleaned = _UNSAFE_CHARS.sub("_", name).strip("_")
    return cleaned or "layer"
