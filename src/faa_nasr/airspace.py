"""Convert controlled-airspace shapefiles and SAA AIXM XML into spatialite databases.

Uses pyogrio's low-level numpy-based read/write API rather than read_dataframe so
we don't need geopandas + pandas at runtime.
"""

from __future__ import annotations

import re
import warnings
import zipfile
from pathlib import Path

import pyogrio
import pyogrio.raw
from tqdm import tqdm

from faa_nasr import _log

# OGR emits "Non closed ring detected" warnings on some SAA AIXM polygons.
# We can't fix the source data and pyogrio still returns the geometry, so the
# warning is just noise -- silence it instead of polluting every CLI run.
warnings.filterwarnings("ignore", message="Non closed ring detected", module="pyogrio")

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
        total_features += _copy_layer(src=shp, dst=dst, layer_name=_safe_name(shp.stem), layer=None)
    _log.info(f"  wrote {len(shapefiles)} shapefiles / {total_features:,} features")


def _build_sua(nasr_dir: Path, dst: Path) -> None:
    """Convert the SAA AIXM XML feature files into a spatialite DB.

    The outer SaaSubscriberFile.zip ships an AIXM schema bundle plus a nested
    `Saa_Sub_File.zip` whose entries are the per-airspace XML feature files
    (e.g. "ADA EAST MOA, KS.xml"). We recursively unzip and process those.
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
    xml_files = [
        p for p in sorted(extract_dir.rglob("*.xml")) if "xsd" not in p.parts
    ]
    total_layers = 0
    total_features = 0
    bar = tqdm(
        xml_files,
        desc="  AIXM XML",
        unit="file",
        disable=_log.is_quiet(),
        leave=True,
    )
    for xml in bar:
        bar.set_postfix_str(xml.name, refresh=False)
        try:
            layers = pyogrio.list_layers(xml)
        except Exception:
            continue
        for layer_row in layers:
            layer = str(layer_row[0])
            n = _copy_layer(
                src=xml,
                dst=dst,
                layer_name=_safe_name(f"{xml.stem}_{layer}"),
                layer=layer,
            )
            if n > 0:
                total_layers += 1
                total_features += n
    _log.info(f"  wrote {total_layers} layers / {total_features:,} features")


def _extract_recursive(zip_path: Path, dest: Path) -> None:
    """Extract `zip_path` into `dest`, then recursively extract any nested .zip files."""
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    for nested in list(dest.rglob("*.zip")):
        _extract_recursive(nested, nested.parent / nested.stem)


def _copy_layer(src: Path, dst: Path, layer_name: str, layer: str | None) -> int:
    """Read all features from `src` (optionally one layer) and append to spatialite `dst`."""
    try:
        meta, _fids, geometry, field_data = pyogrio.raw.read(src, layer=layer)
    except (pyogrio.errors.DataSourceError, IndexError):
        # AIXM bundles include schema/index XML files with no readable layer.
        return 0
    if geometry is None or len(geometry) == 0:
        return 0
    pyogrio.raw.write(
        dst,
        geometry=geometry,
        field_data=field_data,
        fields=meta["fields"],
        # Force a MULTI* geometry type so a layer that mixes Polygon and
        # MultiPolygon (or LineString and MultiLineString, etc.) can be stored
        # in a single SpatiaLite column. promote_to_multi upgrades the singletons.
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
