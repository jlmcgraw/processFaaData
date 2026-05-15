"""Convert class-airspace shapefiles and SAA AIXM XML into spatialite databases.

Uses pyogrio's low-level numpy-based read/write API rather than read_dataframe so
we don't need geopandas + pandas at runtime.
"""

from __future__ import annotations

import contextlib
import re
import sqlite3
import warnings
import xml.etree.ElementTree as ET
import zipfile
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from operator import attrgetter
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pyogrio
import pyogrio.errors
import pyogrio.raw
from tqdm import tqdm

from faa_nasr import _log


class FeatureRef(NamedTuple):
    """Identifies one AIXM feature within a single XML.

    Used as the key in `FkLookup`: a (feature_type, gml_id) pair like
    ("AirTrafficControlService", "ATC1") locates exactly one entity in
    one XML. Tuple semantics so it remains hashable / dict-key-friendly.
    """

    feature_type: str
    gml_id: str


class LayerSource(NamedTuple):
    """One (xml-file, source-layer-name) pair to read with pyogrio."""

    xml: Path
    source_layer: str


# {FeatureRef: {relationship_name: target_uuid}} -- the resolved
# XLink graph for a single AIXM XML. e.g.
# FeatureRef("AirTrafficControlService", "ATC1") -> {"clientAirspace": "uuid-..."}.
type FkLookup = dict[FeatureRef, dict[str, str]]

# Mapping from XML path to that XML's resolved FK lookup.
type PerXmlFkLookup = dict[Path, FkLookup]

# A getter that pulls one of the column dicts off a `_SourceChunk`. Used by
# `_stack_column` so callers can stack either the regular field columns or
# the FK columns through the same routine without a stringly-typed selector.
type ColumnGetter = Callable[[_SourceChunk], dict[str, np.ndarray]]

# AIXM top-level feature element names that pyogrio surfaces as separate layers.
# Used to identify entity boundaries when walking the XML for FK extraction.
_AIXM_FEATURES = frozenset(
    {
        "Airspace",
        "Unit",
        "OrganisationAuthority",
        "AirTrafficControlService",
        "AirspaceUsage",
        "RadioCommunicationChannel",
        "InformationService",
    }
)
_GML_NS = "{http://www.opengis.net/gml/3.2}"
_XLINK_NS = "{http://www.w3.org/1999/xlink}"

# OGR emits "Non closed ring detected" warnings on some SAA AIXM polygons.
# We can't fix the source data and pyogrio still returns the geometry, so the
# warning is just noise -- silence it instead of polluting every CLI run.
warnings.filterwarnings("ignore", message="Non closed ring detected", module="pyogrio")

# Reuse the spatialite extension loader from the geometry module.
from faa_nasr.geometry import _load_mod_spatialite  # noqa: E402

CLASS_AIRSPACE_DB = "class_airspace_spatialite.sqlite"
SUA_DB = "special_use_airspace_spatialite.sqlite"


def build(nasr_dir: Path, out_dir: Path) -> None:
    """Build both airspace spatialite databases from an extracted NASR directory."""
    nasr_dir = nasr_dir.resolve()
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    _build_class_airspace(nasr_dir=nasr_dir, dst=out_dir / CLASS_AIRSPACE_DB)
    _build_sua(nasr_dir=nasr_dir, dst=out_dir / SUA_DB)


def _build_class_airspace(nasr_dir: Path, dst: Path) -> None:
    """Convert all *.shp under Additional_Data/Shape_Files/ into a spatialite DB.

    The FAA ships Class B/C/D/E airspace as a single Class_Airspace shapefile.
    """
    _log.step(f"build-airspace class -> {dst}")
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

    # Pass 1: discover layers AND extract XLink relationships per XML in one walk.
    layer_buckets: dict[str, list[LayerSource]] = defaultdict(list)
    fk_per_xml: PerXmlFkLookup = {}
    for xml in tqdm(
        xml_files, desc="  scanning XMLs", unit="file", disable=_log.is_quiet(), leave=False
    ):
        try:
            for source_layer, _ in pyogrio.list_layers(xml):
                name = str(source_layer)
                layer_buckets[name].append(LayerSource(xml=xml, source_layer=name))
        except Exception:
            continue
        try:
            fk_per_xml[xml] = _extract_xlinks(xml)
        except ET.ParseError:
            fk_per_xml[xml] = {}

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
        n, has_geom = _merge_and_write_layer(
            dst=dst, sources=sources, target_layer=target, fk_per_xml=fk_per_xml
        )
        if n > 0:
            total_features += n
            if has_geom:
                geometry_layers.append(target)

    # Pass 3: build spatial indexes on the merged geometry layers.
    if geometry_layers:
        with contextlib.closing(sqlite3.connect(dst)) as conn:
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


def _copy_geojson_layer(src: Path, dst: Path, layer_name: str, geometry_type: str) -> int:
    """Copy one GeoJSON file's features into a SpatiaLite layer.

    Returns the number of features written. Empty feeds produce no layer --
    pyogrio.raw.write refuses an empty payload, and a schemaless empty layer
    is more confusing than an absent one.
    """
    safe = _safe_name(layer_name)
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
        geometry_type=_promote_geom_type(geometry_type),
        crs=meta.get("crs") or "EPSG:4326",
        layer=safe,
        driver="SQLite",
        dataset_options={"SPATIALITE": "YES"},
        layer_options={"SPATIAL_INDEX": "YES", "LAUNDER": "NO"},
        promote_to_multi=True,
        append=dst.exists(),
    )
    return len(geometry)


@dataclass(frozen=True)
class _SourceChunk:
    """One pyogrio read of one (xml, source_layer) pair, ready to be merged.

    `fields` and `fks` map column names to numpy arrays of length `n_rows`.
    `geometry` is a numpy array of WKB bytes (or all-None for layers without
    geometry). `xml_stem` is the airspace name we propagate as `_source_xml`.
    """

    xml_stem: str
    n_rows: int
    geometry: np.ndarray
    fields: dict[str, np.ndarray]
    fks: dict[str, np.ndarray]
    geom_type: str | None
    crs: str | None  # pyogrio returns CRS strings like "EPSG:4326"


@dataclass(frozen=True)
class _MergedLayer:
    """Result of merging multiple `_SourceChunk`s into one layer-shaped payload."""

    geometry: np.ndarray
    fields: list[str]  # field columns + FK columns + ["_source_xml"]
    field_data: list[np.ndarray]  # parallel to `fields`
    geom_type: str | None
    crs: str | None
    has_geometry: bool


def _read_layer_source(
    xml: Path,
    source_layer: str,
    fk_lookup: FkLookup,
) -> _SourceChunk | None:
    """Read one source via pyogrio and resolve its per-row XLink FKs.

    Returns `None` if the source is unreadable or empty. `fk_lookup` is the
    resolved-XLink map for the source XML (i.e. `fk_per_xml[xml]`).
    """
    try:
        meta, _fids, geometry, field_data = pyogrio.raw.read(xml, layer=source_layer)
    except (pyogrio.errors.DataSourceError, IndexError):
        return None
    n_rows = len(field_data[0]) if field_data else (len(geometry) if geometry is not None else 0)
    if n_rows == 0:
        return None

    fields = dict(zip(meta["fields"], field_data, strict=True))

    # Resolve per-row FKs by looking up each row's gml_id in fk_lookup.
    fks: dict[str, list[object]] = {}
    gml_ids = fields.get("gml_id")
    for i in range(n_rows):
        gml = str(gml_ids[i]) if gml_ids is not None else ""
        ref = FeatureRef(feature_type=source_layer, gml_id=gml)
        for rel_name, target_uuid in fk_lookup.get(ref, {}).items():
            fks.setdefault(rel_name, [None] * n_rows)[i] = target_uuid
    fk_arrays = {name: np.array(values, dtype=object) for name, values in fks.items()}

    geom = (
        geometry
        if geometry is not None and len(geometry) == n_rows
        else np.array([None] * n_rows, dtype=object)
    )

    return _SourceChunk(
        xml_stem=xml.stem,
        n_rows=n_rows,
        geometry=geom,
        fields=fields,
        fks=fk_arrays,
        geom_type=meta.get("geometry_type"),
        crs=meta.get("crs"),
    )


def _ordered_union(*key_iterables: Iterable[str]) -> list[str]:
    """Union of keys preserving first-seen insertion order across all iterables."""
    return list(dict.fromkeys(k for keys in key_iterables for k in keys))


def _stack_column(name: str, chunks: list[_SourceChunk], getter: ColumnGetter) -> np.ndarray:
    """Concatenate `name` across chunks, padding chunks that lack it with None.

    `getter` extracts the relevant column dict from each chunk -- typically
    `attrgetter("fields")` or `attrgetter("fks")`.
    """
    parts: list[np.ndarray] = []
    for c in chunks:
        col = getter(c).get(name)
        if col is None:
            col = np.array([None] * c.n_rows, dtype=object)
        parts.append(col)
    return np.concatenate(parts)


def _merge_chunks(chunks: list[_SourceChunk]) -> _MergedLayer | None:
    """Pure: combine chunks into one merged layer payload (None if empty).

    AIXM XMLs for the same source layer can differ in field set, so we take
    the union of field/FK names and pad missing columns with None per chunk.
    `geom_type` and `crs` come from the first chunk that has them.
    """
    if not chunks:
        return None

    get_fields: ColumnGetter = attrgetter("fields")
    get_fks: ColumnGetter = attrgetter("fks")

    field_order = _ordered_union(*(c.fields.keys() for c in chunks))
    fk_order = _ordered_union(*(c.fks.keys() for c in chunks))

    field_data = [_stack_column(n, chunks, get_fields) for n in field_order]
    fk_data = [_stack_column(n, chunks, get_fks) for n in fk_order]
    source_xmls = np.array([c.xml_stem for c in chunks for _ in range(c.n_rows)], dtype=object)
    geometry = np.concatenate([c.geometry for c in chunks])

    geom_type = next((c.geom_type for c in chunks if c.geom_type), None)
    crs = next((c.crs for c in chunks if c.crs is not None), None)
    has_geometry = bool(geom_type) and any(g is not None for g in geometry)

    return _MergedLayer(
        geometry=geometry,
        fields=field_order + fk_order + ["_source_xml"],
        field_data=field_data + fk_data + [source_xmls],
        geom_type=geom_type,
        crs=crs,
        has_geometry=has_geometry,
    )


def _write_merged_layer(dst: Path, merged: _MergedLayer, target_layer: str) -> None:
    """Write a merged layer to `dst`. Spatial layers go through pyogrio;
    attribute-only layers go through raw sqlite3 (pyogrio requires a geometry).
    """
    if merged.has_geometry:
        pyogrio.raw.write(
            dst,
            geometry=merged.geometry,
            field_data=merged.field_data,
            fields=merged.fields,
            geometry_type=_promote_geom_type(merged.geom_type),
            crs=merged.crs,
            layer=target_layer,
            driver="SQLite",
            dataset_options={"SPATIALITE": "YES"},
            layer_options={"SPATIAL_INDEX": "NO", "LAUNDER": "NO"},
            promote_to_multi=True,
            append=dst.exists(),
        )
    else:
        _write_attribute_only_table(dst, target_layer, merged.fields, merged.field_data)


def _merge_and_write_layer(
    dst: Path,
    sources: list[LayerSource],
    target_layer: str,
    fk_per_xml: PerXmlFkLookup,
) -> tuple[int, bool]:
    """Orchestrator: read all sources, merge them, write once. Returns
    (row_count, has_geometry)."""
    chunks = [
        chunk
        for src in sources
        if (chunk := _read_layer_source(src.xml, src.source_layer, fk_per_xml.get(src.xml, {})))
        is not None
    ]
    merged = _merge_chunks(chunks)
    if merged is None:
        return 0, False
    _write_merged_layer(dst=dst, merged=merged, target_layer=target_layer)
    return sum(c.n_rows for c in chunks), merged.has_geometry


def _local_name(elem: ET.Element) -> str:
    """Return an element's local tag name with any XML namespace stripped."""
    return elem.tag.rsplit("}", 1)[-1]


def _build_gml_to_uuid_map(root: ET.Element) -> dict[str, str]:
    """Map every top-level AIXM feature's gml:id to its <identifier> UUID.

    Only elements whose local name is in `_AIXM_FEATURES` are considered;
    nested non-feature elements are ignored even if they happen to carry a
    gml:id (those are GML internal IDs like surface IDs).
    """
    out: dict[str, str] = {}
    for elem in root.iter():
        if _local_name(elem) not in _AIXM_FEATURES:
            continue
        gml_id = elem.get(f"{_GML_NS}id")
        if not gml_id:
            continue
        for child in elem:
            if _local_name(child) == "identifier" and child.text:
                out[gml_id] = child.text.strip()
                break
    return out


def _resolve_xlinks(root: ET.Element, gml_to_uuid: dict[str, str]) -> FkLookup:
    """Walk `root` and return resolved XLink relationships.

    For each xlink:href encountered, the result records
    FeatureRef(feature_type, feature_gml_id) -> {parent_element_tag: target_uuid},
    where `parent_element_tag` is the local tag of the element carrying the
    xlink (e.g. `serviceProvider` in `<serviceProvider xlink:href="#Unit1"/>`).
    """
    fk_map: FkLookup = {}

    def walk(elem: ET.Element, top: FeatureRef | None) -> None:
        local = _local_name(elem)
        if local in _AIXM_FEATURES:
            top = FeatureRef(feature_type=local, gml_id=elem.get(f"{_GML_NS}id") or "")
        href = elem.get(f"{_XLINK_NS}href")
        if href and top is not None:
            target_uuid = gml_to_uuid.get(href.lstrip("#"))
            if target_uuid:
                fk_map.setdefault(top, {})[local] = target_uuid
        for child in elem:
            walk(child, top)

    walk(root, None)
    return fk_map


def _extract_xlinks(xml_path: Path) -> FkLookup:
    """Walk an AIXM XML once and resolve XLink references to UUIDs.

    Thin orchestrator: parses the file, then defers to the pure-on-Element
    helpers `_build_gml_to_uuid_map` and `_resolve_xlinks`. Returns
    {FeatureRef(feature_type, gml_id): {relationship_name: target_uuid}}.
    """
    root = ET.parse(xml_path).getroot()
    gml_to_uuid = _build_gml_to_uuid_map(root)
    return _resolve_xlinks(root, gml_to_uuid)


def _init_spatialite_db(dst: Path) -> None:
    """Create `dst` as an empty SpatiaLite-initialized SQLite database."""
    with contextlib.closing(sqlite3.connect(dst)) as conn:
        conn.enable_load_extension(True)
        _load_mod_spatialite(conn)
        conn.execute("PRAGMA trusted_schema = ON")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        conn.execute("SELECT InitSpatialMetadata(1)")
        conn.commit()


def _write_attribute_only_table(
    dst: Path, table: str, fields: list[str], field_data: list[np.ndarray]
) -> None:
    """Write a non-spatial table via raw sqlite3 (pyogrio.raw.write requires geometry)."""
    with contextlib.closing(sqlite3.connect(dst)) as conn:
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = MEMORY")
        cols = ", ".join(f'"{c}" TEXT' for c in fields)
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" ({cols})')
        placeholders = ",".join("?" * len(fields))
        rows = (
            tuple("" if v is None else str(v) for v in row) for row in zip(*field_data, strict=True)
        )
        conn.executemany(f'INSERT INTO "{table}" VALUES ({placeholders})', rows)
        conn.commit()


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
