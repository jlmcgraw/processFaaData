"""Tests for the weather fetch pipeline (offline, no real API calls)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from faa_nasr import weather


def test_weather_feeds_cover_all_five_data_types():
    """The 5 feeds in the original Perl repo + 1 modern addition.

    Domestic AIRMET/SIGMET in the original repo is one feed; we also pull
    international SIGMETs because the modern API exposes them separately.
    """
    layers = {f.layer for f in weather.WEATHER_FEEDS}
    assert layers == {"metars", "tafs", "pireps", "airsigmets", "isigmets"}


def test_every_feed_includes_required_bbox():
    """METAR/TAF/PIREP all 400 without a bbox or station list. We never set
    a station list, so every feed must include a bbox -- otherwise a single
    misconfigured feed silently breaks the whole fetch."""
    for feed in weather.WEATHER_FEEDS:
        if feed.path in ("airsigmet", "isigmet"):
            # These accept (and ignore) bbox or none. Either is fine.
            continue
        assert "bbox" in feed.params, f"{feed.layer} feed missing bbox param"


def test_fetch_geojson_passes_params(monkeypatch: pytest.MonkeyPatch):
    """`_fetch_geojson` should hit `<base>/<feed.path>` with the feed's params."""
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"type": "FeatureCollection", "features": []})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    feed = weather.WeatherFeed(
        "metars", "metar", {"format": "geojson", "hours": 2, "bbox": "1,2,3,4"}, "Point"
    )
    out = weather._fetch_geojson(client, feed)

    assert out == {"type": "FeatureCollection", "features": []}
    assert "aviationweather.gov/api/data/metar" in seen["url"]
    assert "format=geojson" in seen["url"]
    assert "hours=2" in seen["url"]
    assert "bbox=" in seen["url"]


def test_fetch_writes_geojson_cache_per_feed(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """End-to-end with mocked HTTP: each feed should land as a cached
    .geojson file in `weather_cache/` so the pipeline is reproducible/debuggable.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        # Return an empty FeatureCollection so the layer-write code path is
        # exercised but pyogrio doesn't have to deal with real data.
        return httpx.Response(200, json={"type": "FeatureCollection", "features": []})

    real_client = httpx.Client

    def fake_client_ctor(*args, **kwargs):
        kwargs.pop("timeout", None)
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(weather.httpx, "Client", fake_client_ctor)
    # Skip the SpatiaLite init -- we're only testing the fetch+cache layer here.
    monkeypatch.setattr(weather, "_init_spatialite_db", lambda dst: None)
    # And skip the actual layer copy -- empty FeatureCollection would no-op
    # anyway, but stubbing makes the test independent of pyogrio.
    monkeypatch.setattr(weather, "_copy_geojson_layer", lambda **kwargs: 0)

    weather.fetch(out_dir=tmp_path)

    cache_dir = tmp_path / "weather_cache"
    for feed in weather.WEATHER_FEEDS:
        cached = cache_dir / f"{feed.layer}.geojson"
        assert cached.exists(), f"{feed.layer}.geojson not cached"
        assert json.loads(cached.read_text())["type"] == "FeatureCollection"
