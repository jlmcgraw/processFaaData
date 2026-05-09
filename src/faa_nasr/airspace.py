"""Convert controlled-airspace shapefiles and SAA AIXM XML into spatialite databases.

Uses pyogrio's low-level numpy-based read/write API rather than read_dataframe so
we don't need geopandas + pandas at runtime.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pyogrio
import pyogrio.raw

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
    shape_dir = nasr_dir / "Additional_Data" / "Shape_Files"
    if not shape_dir.is_dir():
        return
    if dst.exists():
        dst.unlink()
    for shp in sorted(shape_dir.glob("*.shp")):
        _copy_layer(src=shp, dst=dst, layer_name=shp.stem, layer=None)


def _build_sua(nasr_dir: Path, dst: Path) -> None:
    """Convert the SAA AIXM XML feature files into a spatialite DB.

    The outer SaaSubscriberFile.zip ships an AIXM schema bundle plus a nested
    `Saa_Sub_File.zip` whose entries are the per-airspace XML feature files
    (e.g. "ADA EAST MOA, KS.xml"). We recursively unzip and process those.
    """
    saa_zip = nasr_dir / "Additional_Data" / "AIXM" / "SAA-AIXM_5_Schema" / "SaaSubscriberFile.zip"
    if not saa_zip.is_file():
        return

    extract_dir = saa_zip.parent / "extracted"
    extract_dir.mkdir(exist_ok=True)
    _extract_recursive(saa_zip, extract_dir)

    if dst.exists():
        dst.unlink()
    for xml in sorted(extract_dir.rglob("*.xml")):
        # Skip XSD-style schema files that happen to use .xml; they live under xsd/.
        if "xsd" in xml.parts:
            continue
        try:
            layers = pyogrio.list_layers(xml)
        except Exception:
            continue
        for layer_row in layers:
            layer = str(layer_row[0])
            _copy_layer(src=xml, dst=dst, layer_name=f"{xml.stem}_{layer}", layer=layer)


def _extract_recursive(zip_path: Path, dest: Path) -> None:
    """Extract `zip_path` into `dest`, then recursively extract any nested .zip files."""
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    for nested in list(dest.rglob("*.zip")):
        _extract_recursive(nested, nested.parent / nested.stem)


def _copy_layer(src: Path, dst: Path, layer_name: str, layer: str | None) -> None:
    """Read all features from `src` (optionally one layer) and append to spatialite `dst`."""
    try:
        meta, _fids, geometry, field_data = pyogrio.raw.read(src, layer=layer)
    except (pyogrio.errors.DataSourceError, IndexError):
        # AIXM bundles include schema/index XML files with no readable layer.
        return
    if geometry is None or len(geometry) == 0:
        return
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


def _promote_geom_type(geom_type: str | None) -> str | None:
    """Return the Multi* equivalent of a geometry type, preserving any 'Z'/'M' suffix."""
    if not geom_type:
        return geom_type
    base, _, suffix = geom_type.partition(" ")
    if base in ("Polygon", "LineString", "Point"):
        base = "Multi" + base
    return f"{base} {suffix}".strip()
