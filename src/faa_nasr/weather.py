"""Fetch current US aviation weather feeds and build a SpatiaLite database.

This module is a port of the jlmcgraw/aviationMapMetarSigmetsAndTFRs Perl
pipeline, updated for the modern aviationweather.gov JSON API. The legacy
ADDS XML/CSV feeds that the original repo scraped have been retired.

Output is one SpatiaLite layer per data type in `weather.sqlite`:

  - `metars`     POINT     surface observations (last N hours)
  - `tafs`       POINT     terminal aerodrome forecasts (next N hours)
  - `pireps`     POINT     pilot reports (last N hours, CONUS+OCONUS bbox)
  - `airsigmets` POLYGON   domestic AIRMETs and SIGMETs
  - `isigmets`   POLYGON   international SIGMETs

Each layer carries the full attribute payload from the upstream API as
TEXT columns, plus a geometry column with a SpatiaLite spatial index.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import httpx
from tqdm import tqdm

from faa_nasr import _log
from faa_nasr.airspace import _copy_geojson_layer, _init_spatialite_db

WEATHER_API_BASE = "https://aviationweather.gov/api/data"
WEATHER_OUTPUT_DB = "weather.sqlite"

# Generous bbox covering CONUS + Alaska + Hawaii + adjacent oceans.
# `metar`, `taf`, and `pirep` all reject requests without a station list or
# bounding box; airsigmet/isigmet accept it harmlessly. Use one bbox
# everywhere so the request shape stays uniform.
_WEATHER_BBOX = "15,-180,75,-60"


@dataclass(frozen=True)
class WeatherFeed:
    """One aviationweather.gov endpoint we ingest as a SpatiaLite layer."""

    layer: str
    path: str  # e.g. "metar"
    params: dict[str, str | int]
    # GeoJSON feature geometry types live under feature.geometry.type. We
    # could let pyogrio sniff, but feeds occasionally contain features with
    # null geometry that confuse the sniffer, so we tell it explicitly.
    geometry_type: str


WEATHER_FEEDS: tuple[WeatherFeed, ...] = (
    WeatherFeed(
        "metars", "metar", {"format": "geojson", "hours": 2, "bbox": _WEATHER_BBOX}, "Point"
    ),
    WeatherFeed("tafs", "taf", {"format": "geojson", "hours": 12, "bbox": _WEATHER_BBOX}, "Point"),
    WeatherFeed(
        "pireps", "pirep", {"format": "geojson", "age": 2, "bbox": _WEATHER_BBOX}, "Point"
    ),
    WeatherFeed("airsigmets", "airsigmet", {"format": "geojson"}, "Polygon"),
    WeatherFeed("isigmets", "isigmet", {"format": "geojson"}, "Polygon"),
)


def fetch(out_dir: Path) -> Path:
    """Download every weather feed and build `weather.sqlite` in `out_dir`.

    Returns the path to the written database.
    """
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / WEATHER_OUTPUT_DB
    _log.step(f"fetch-weather -> {dst}")

    if dst.exists():
        dst.unlink()
    _init_spatialite_db(dst)

    cache_dir = out_dir / "weather_cache"
    cache_dir.mkdir(exist_ok=True)

    total = 0
    bar = tqdm(WEATHER_FEEDS, desc="  feeds", unit="feed", disable=_log.is_quiet(), leave=True)
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        for feed in bar:
            bar.set_postfix_str(feed.layer, refresh=False)
            payload = _fetch_geojson(client, feed)
            geojson_path = cache_dir / f"{feed.layer}.geojson"
            geojson_path.write_text(json.dumps(payload))
            total += _copy_geojson_layer(
                src=geojson_path,
                dst=dst,
                layer_name=feed.layer,
                geometry_type=feed.geometry_type,
            )
    _log.info(f"  wrote {len(WEATHER_FEEDS)} layers / {total:,} features")
    return dst


def _fetch_geojson(client: httpx.Client, feed: WeatherFeed) -> dict:
    """Fetch one feed's GeoJSON FeatureCollection."""
    url = f"{WEATHER_API_BASE}/{feed.path}"
    resp = client.get(url, params=feed.params)
    resp.raise_for_status()
    return resp.json()
