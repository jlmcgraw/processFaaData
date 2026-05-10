"""Tests for faa_nasr.edai (downloads + spatialite build)."""

from __future__ import annotations

import email.utils
import io
import os
import zipfile
from pathlib import Path
from typing import Any, cast

import httpx
import pytest

from faa_nasr import edai


def _as_client(c: object) -> httpx.Client:
    """Cast a duck-typed mock to httpx.Client for type checkers (e.g. ty)
    that don't honor `# type: ignore` comments."""
    return cast(httpx.Client, c)


# ---------------------------------------------------------------------------
# Fake httpx for download tests
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(
        self, *, status_code: int = 200, body: bytes = b"", headers: dict[str, str] | None = None
    ):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_bytes(self) -> Any:
        yield self._body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class _FakeClient:
    """Records every stream() call and returns the configured response.

    `stream_responses` maps URLs (without query string) to a list of
    responses, popped in order. `requests` records (url, params, headers)
    tuples for assertion."""

    def __init__(self) -> None:
        self.stream_responses: dict[str, list[_FakeResponse]] = {}
        self.requests: list[tuple[str, dict[str, Any], dict[str, str]]] = []

    def stream(
        self,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> _FakeResponse:
        self.requests.append((url, params or {}, headers or {}))
        responses = self.stream_responses.get(url)
        assert responses, f"No response queued for {url}"
        return responses.pop(0)

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


# ---------------------------------------------------------------------------
# _fetch_one
# ---------------------------------------------------------------------------


def test_fetch_one_downloads_when_dest_missing(tmp_path):
    client = _FakeClient()
    url = f"{edai.EDAI_BASE_URL}/test-guid_0/downloads/data"
    client.stream_responses[url] = [
        _FakeResponse(body=b"ZIPDATA", headers={"Last-Modified": "Mon, 01 Jan 2024 00:00:00 GMT"})
    ]

    dest = tmp_path / "out.zip"
    edai._fetch_one(_as_client(client), "test-guid_0", dest)

    assert dest.read_bytes() == b"ZIPDATA"
    # Server's Last-Modified is reflected as the file's mtime.
    expected_mtime = email.utils.parsedate_to_datetime("Mon, 01 Jan 2024 00:00:00 GMT").timestamp()
    assert abs(dest.stat().st_mtime - expected_mtime) < 1.0


def test_fetch_one_sends_if_modified_since_when_dest_exists(tmp_path):
    """If the local copy exists, send its mtime as If-Modified-Since."""
    client = _FakeClient()
    url = f"{edai.EDAI_BASE_URL}/abc_0/downloads/data"
    client.stream_responses[url] = [_FakeResponse(status_code=304)]

    dest = tmp_path / "abc.zip"
    dest.write_bytes(b"OLD CONTENT")
    known_mtime = email.utils.parsedate_to_datetime("Mon, 01 Jan 2024 00:00:00 GMT").timestamp()
    os.utime(dest, (known_mtime, known_mtime))

    edai._fetch_one(_as_client(client), "abc_0", dest)

    # Old content survived; one request was sent with If-Modified-Since.
    assert dest.read_bytes() == b"OLD CONTENT"
    sent_url, _, sent_headers = client.requests[0]
    assert sent_url == url
    assert "If-Modified-Since" in sent_headers
    assert "2024" in sent_headers["If-Modified-Since"]


def test_fetch_one_overwrites_when_remote_is_newer(tmp_path):
    """200 with new body replaces the local copy."""
    client = _FakeClient()
    url = f"{edai.EDAI_BASE_URL}/abc_0/downloads/data"
    client.stream_responses[url] = [
        _FakeResponse(
            body=b"NEW CONTENT", headers={"Last-Modified": "Wed, 01 Jan 2025 00:00:00 GMT"}
        )
    ]

    dest = tmp_path / "abc.zip"
    dest.write_bytes(b"OLD")

    edai._fetch_one(_as_client(client), "abc_0", dest)

    assert dest.read_bytes() == b"NEW CONTENT"


def test_fetch_one_passes_format_and_spatial_ref(tmp_path):
    client = _FakeClient()
    url = f"{edai.EDAI_BASE_URL}/g_0/downloads/data"
    client.stream_responses[url] = [_FakeResponse(body=b"")]

    edai._fetch_one(_as_client(client), "g_0", tmp_path / "g.zip")

    _, params, _ = client.requests[0]
    assert params == {"format": "shp", "spatialRefId": edai.EDAI_SPATIAL_REF_ID}


# ---------------------------------------------------------------------------
# _extract_all
# ---------------------------------------------------------------------------


def _make_shapefile_zip(path: Path, layer_name: str) -> None:
    """Build a tiny zip pretending to be a shapefile bundle."""
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(f"{layer_name}.shp", b"FAKE_SHP")
        zf.writestr(f"{layer_name}.dbf", b"FAKE_DBF")
        zf.writestr(f"{layer_name}.shx", b"FAKE_SHX")


def test_extract_all_creates_per_zip_subdir(tmp_path):
    download_dir = tmp_path / "dl"
    extract_dir = tmp_path / "ex"
    download_dir.mkdir()
    extract_dir.mkdir()

    _make_shapefile_zip(download_dir / "Airports.zip", "Airports")
    _make_shapefile_zip(download_dir / "Runways.zip", "Runways")

    edai._extract_all(download_dir, extract_dir)

    assert (extract_dir / "Airports" / "Airports.shp").exists()
    assert (extract_dir / "Runways" / "Runways.shp").exists()


def test_extract_all_skips_non_zip_files(tmp_path):
    download_dir = tmp_path / "dl"
    extract_dir = tmp_path / "ex"
    download_dir.mkdir()
    extract_dir.mkdir()

    (download_dir / "junk.txt").write_text("not a zip")
    _make_shapefile_zip(download_dir / "Real.zip", "Real")

    edai._extract_all(download_dir, extract_dir)

    assert (extract_dir / "Real" / "Real.shp").exists()
    assert not list((extract_dir / "junk").glob("*")) if (extract_dir / "junk").exists() else True


# ---------------------------------------------------------------------------
# fetch() orchestration
# ---------------------------------------------------------------------------


def test_fetch_downloads_each_dataset_then_extracts(monkeypatch: pytest.MonkeyPatch, tmp_path):
    fake = _FakeClient()
    # Queue a body for every dataset GUID so all 21 fetches succeed.
    for guid in edai.EDAI_DATASETS:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            zf.writestr(f"{edai.EDAI_DATASETS[guid]}.shp", b"FAKE")
        fake.stream_responses[f"{edai.EDAI_BASE_URL}/{guid}/downloads/data"] = [
            _FakeResponse(body=zip_buf.getvalue())
        ]

    class _Module:
        Client = lambda *a, **kw: fake  # noqa: E731

    monkeypatch.setattr(edai, "httpx", _Module)

    result = edai.fetch(out_dir=tmp_path)

    assert isinstance(result, edai.EdaiFetchResult)
    # All 21 datasets requested.
    requested_urls = [u for u, _, _ in fake.requests]
    assert len(requested_urls) == len(edai.EDAI_DATASETS)
    # Each landed on disk as <description>.zip.
    for description in edai.EDAI_DATASETS.values():
        assert (result.download_dir / f"{description}.zip").exists()


# ---------------------------------------------------------------------------
# build()
# ---------------------------------------------------------------------------


def test_build_calls_copy_shapefile_for_every_shp(monkeypatch: pytest.MonkeyPatch, tmp_path):
    extract_dir = tmp_path / "extracted"
    (extract_dir / "Airports").mkdir(parents=True)
    (extract_dir / "Runways").mkdir()
    (extract_dir / "Airports" / "Airports.shp").touch()
    (extract_dir / "Runways" / "Runways.shp").touch()

    init_called: list[Path] = []
    copy_calls: list[tuple[Path, str]] = []

    def fake_init(d: Path) -> None:
        init_called.append(d)

    def fake_copy(src: Path, dst: Path, layer_name: str) -> int:
        copy_calls.append((src, layer_name))
        return 100

    monkeypatch.setattr(edai, "_init_spatialite_db", fake_init)
    monkeypatch.setattr(edai, "_copy_shapefile", fake_copy)

    out_dir = tmp_path / "out"
    edai.build(out_dir=out_dir, extract_dir=extract_dir)

    expected_db = (out_dir / edai.EDAI_OUTPUT_DB).resolve()
    assert init_called == [expected_db]
    assert sorted(name for _, name in copy_calls) == ["Airports", "Runways"]


def test_build_unlinks_existing_db(monkeypatch: pytest.MonkeyPatch, tmp_path):
    extract_dir = tmp_path / "extracted"
    extract_dir.mkdir()

    seen_dst_at_init: list[bool] = []

    def fake_init(d: Path) -> None:
        seen_dst_at_init.append(d.exists())

    monkeypatch.setattr(edai, "_init_spatialite_db", fake_init)
    monkeypatch.setattr(edai, "_copy_shapefile", lambda src, dst, layer_name: 0)

    out_dir = tmp_path / "out"
    out_dir.mkdir()
    stale = out_dir / edai.EDAI_OUTPUT_DB
    stale.write_text("stale leftover")

    edai.build(out_dir=out_dir, extract_dir=extract_dir)

    # The stale DB was deleted before _init_spatialite_db ran.
    assert seen_dst_at_init == [False]
