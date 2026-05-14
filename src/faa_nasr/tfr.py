"""Fetch active TFR polygons from the FAA and build a SpatiaLite database.

This module is a port of the jlmcgraw/aviationMapMetarSigmetsAndTFRs Perl
TFR scraper, updated for the modern tfr.faa.gov stack. The original site
served per-TFR XNOTAM-Update XML detail pages; the current site exposes
TFR polygons through a GeoServer WFS endpoint and richer notam metadata
through a separate JSON API.

Output is two layers in `tfrs.sqlite`:

  - `tfrs`         POLYGON  active TFR polygons (joined to the metadata
                            below by notam_key, where available)
  - `tfrs_no_shape` (attribute-only) TFRs the FAA hasn't published a
                            polygon for yet (newly-issued, polygon pending)
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pyogrio
import pyogrio.errors
import pyogrio.raw
import sqlite3

from faa_nasr import _log
from faa_nasr.airspace import _init_spatialite_db, _promote_geom_type, _safe_name

TFR_WFS_URL = "https://tfr.faa.gov/geoserver/TFR/ows"
TFR_LIST_URL = "https://tfr.faa.gov/tfrapi/getTfrList"
TFR_NOSHAPE_URL = "https://tfr.faa.gov/tfrapi/noShapeTfrList"
TFR_OUTPUT_DB = "tfrs.sqlite"


def fetch(out_dir: Path) -> Path:
    """Download active TFRs and build `tfrs.sqlite` in `out_dir`.

    Returns the path to the written database.
    """
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / TFR_OUTPUT_DB
    _log.step(f"fetch-tfrs -> {dst}")

    if dst.exists():
        dst.unlink()
    _init_spatialite_db(dst)

    cache_dir = out_dir / "tfr_cache"
    cache_dir.mkdir(exist_ok=True)

    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        polygons = _fetch_wfs_polygons(client)
        tfr_list = _fetch_json(client, TFR_LIST_URL)
        no_shape = _fetch_json(client, TFR_NOSHAPE_URL)

    polygons = _enrich_with_tfr_list(polygons, tfr_list)
    geojson_path = cache_dir / "tfrs.geojson"
    geojson_path.write_text(json.dumps(polygons))

    n_poly = _copy_geojson_layer(
        src=geojson_path, dst=dst, layer_name="tfrs", geometry_type="Polygon"
    )
    n_noshape = _write_no_shape_table(dst=dst, items=no_shape)

    _log.info(f"  wrote {n_poly:,} polygon TFRs / {n_noshape:,} no-shape TFRs")
    return dst


def _fetch_wfs_polygons(client: httpx.Client) -> dict:
    """Fetch the active TFR polygons as a GeoJSON FeatureCollection in EPSG:4326."""
    resp = client.get(
        TFR_WFS_URL,
        params={
            "service": "WFS",
            "version": "1.1.0",
            "request": "GetFeature",
            "typeName": "TFR:V_TFR_LOC",
            "maxFeatures": 1000,
            "outputFormat": "application/json",
            "srsname": "EPSG:4326",
        },
    )
    resp.raise_for_status()
    return resp.json()


def _fetch_json(client: httpx.Client, url: str) -> list[dict]:
    resp = client.get(url)
    resp.raise_for_status()
    return resp.json()


def _enrich_with_tfr_list(polygons: dict, tfr_list: list[dict]) -> dict:
    """Merge per-TFR metadata from `getTfrList` into the WFS FeatureCollection.

    WFS gives polygons + NOTAM_KEY but lacks type/description/mod_date that
    the list API returns. Join key is the leading "N/NNNN" notam id, which
    is `notam_id` in the list API and the first hyphen-delimited segment of
    `NOTAM_KEY` in the WFS payload (e.g. "6/0092-1-FDC-F" -> "6/0092").

    Skips keys that case-insensitively collide with existing WFS columns
    (e.g. list-API `state` vs WFS `STATE`) -- SQLite treats column names
    case-insensitively, so writing both would fail at INSERT time.
    """
    by_notam_id = {item.get("notam_id", ""): item for item in tfr_list}
    for feat in polygons.get("features", []):
        props = feat.setdefault("properties", {})
        notam_key = props.get("NOTAM_KEY", "")
        notam_id = notam_key.split("-", 1)[0] if notam_key else ""
        extra = by_notam_id.get(notam_id)
        if not extra:
            continue
        existing_lower = {k.lower() for k in props}
        for k, v in extra.items():
            if k.lower() in existing_lower:
                continue
            props[k] = v
            existing_lower.add(k.lower())
    return polygons


def _copy_geojson_layer(src: Path, dst: Path, layer_name: str, geometry_type: str) -> int:
    """Copy one GeoJSON file's features into a SpatiaLite layer."""
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


def _write_no_shape_table(dst: Path, items: list[dict]) -> int:
    """Write the no-shape TFR list as a plain (non-spatial) table.

    The FAA's `noShapeTfrList` endpoint returns TFRs whose polygon hasn't
    been published yet. We keep them in their own table so a downstream
    consumer can flag "active TFR, geometry pending" rather than silently
    dropping them.
    """
    if not items:
        return 0
    columns = sorted({k for item in items for k in item.keys()})
    conn = sqlite3.connect(dst)
    try:
        cols_ddl = ", ".join(f'"{c}" TEXT' for c in columns)
        conn.execute('DROP TABLE IF EXISTS "tfrs_no_shape"')
        conn.execute(f'CREATE TABLE "tfrs_no_shape" ({cols_ddl})')
        placeholders = ",".join("?" * len(columns))
        rows = [
            tuple("" if item.get(c) is None else str(item.get(c)) for c in columns) for item in items
        ]
        conn.executemany(f'INSERT INTO "tfrs_no_shape" VALUES ({placeholders})', rows)
        conn.commit()
    finally:
        conn.close()
    return len(items)
