"""Tests for the TFR fetch pipeline (offline, no real API calls)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import httpx
import pytest

from faa_nasr import tfr


def _polygon_feature(notam_key: str, state: str = "CA") -> dict:
    return {
        "type": "Feature",
        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
        "properties": {
            "GID": 1,
            "CNS_LOCATION_ID": "ZOA",
            "NOTAM_KEY": notam_key,
            "TITLE": "Test TFR",
            "LAST_MODIFICATION_DATETIME": "202605131600",
            "STATE": state,
            "LEGAL": "SECURITY",
        },
    }


def test_enrich_with_tfr_list_joins_on_notam_id_prefix():
    """WFS NOTAM_KEY "6/0092-1-FDC-F" should join to list-API notam_id "6/0092"."""
    fc = {"features": [_polygon_feature("6/0092-1-FDC-F")]}
    listing = [
        {"notam_id": "6/0092", "type": "SECURITY", "description": "Calexico, CA"},
        {"notam_id": "9/9999", "type": "OTHER", "description": "irrelevant"},
    ]

    out = tfr._enrich_with_tfr_list(fc, listing)

    props = out["features"][0]["properties"]
    assert props["type"] == "SECURITY"
    assert props["description"] == "Calexico, CA"


def test_enrich_skips_case_insensitive_collisions():
    """WFS gives `STATE`; list API gives `state`. SQLite is case-insensitive
    on column names, so emitting both would fail at INSERT. We must keep the
    WFS-cased column and drop the list-API duplicate."""
    fc = {"features": [_polygon_feature("6/0092-1-FDC-F", state="NV")]}
    listing = [{"notam_id": "6/0092", "state": "CA", "facility": "ZLA"}]

    out = tfr._enrich_with_tfr_list(fc, listing)

    props = out["features"][0]["properties"]
    # WFS-cased key preserved with WFS value
    assert props["STATE"] == "NV"
    # lowercase `state` from list-API not added
    assert "state" not in props
    # other non-colliding list-API keys are still merged in
    assert props["facility"] == "ZLA"


def test_enrich_no_match_leaves_feature_unchanged():
    fc = {"features": [_polygon_feature("6/0092-1-FDC-F")]}
    out = tfr._enrich_with_tfr_list(fc, [{"notam_id": "9/9999", "type": "OTHER"}])
    assert "type" not in out["features"][0]["properties"]


def test_write_no_shape_table_creates_attribute_only_rows(tmp_path: Path):
    db = tmp_path / "tfrs.sqlite"
    sqlite3.connect(db).close()  # create empty DB

    n = tfr._write_no_shape_table(
        dst=db,
        items=[
            {"notam_id": "6/0001", "state": "NV", "title": "alpha"},
            {"notam_id": "6/0002", "state": "CA", "title": "beta", "extra_col": "x"},
        ],
    )

    assert n == 2
    conn = sqlite3.connect(db)
    cols = {row[1] for row in conn.execute('PRAGMA table_info("tfrs_no_shape")')}
    # Union of keys across both items, sorted.
    assert cols == {"notam_id", "state", "title", "extra_col"}
    (count,) = conn.execute('SELECT COUNT(*) FROM "tfrs_no_shape"').fetchone()
    assert count == 2


def test_write_no_shape_table_empty_list_is_noop(tmp_path: Path):
    db = tmp_path / "tfrs.sqlite"
    sqlite3.connect(db).close()
    assert tfr._write_no_shape_table(dst=db, items=[]) == 0
    conn = sqlite3.connect(db)
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "tfrs_no_shape" not in tables


def test_fetch_wfs_polygons_hits_geoserver_with_geojson_output(monkeypatch: pytest.MonkeyPatch):
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"type": "FeatureCollection", "features": []})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    out = tfr._fetch_wfs_polygons(client)

    assert out == {"type": "FeatureCollection", "features": []}
    url = seen["url"]
    assert "tfr.faa.gov/geoserver/TFR/ows" in url
    assert "outputFormat=application%2Fjson" in url or "outputFormat=application/json" in url
    assert "srsname=EPSG%3A4326" in url or "srsname=EPSG:4326" in url
    assert "typeName=TFR%3AV_TFR_LOC" in url or "typeName=TFR:V_TFR_LOC" in url
