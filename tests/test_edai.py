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
        self,
        *,
        status_code: int = 200,
        body: bytes = b"",
        headers: dict[str, str] | None = None,
        json_payload: Any = None,
    ):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {}
        self._json = json_payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return self._json

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
    tuples for assertion. `get_responses` is consulted by `.get()` for
    catalog fetches.
    """

    def __init__(self) -> None:
        self.stream_responses: dict[str, list[_FakeResponse]] = {}
        self.get_responses: dict[str, _FakeResponse] = {}
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

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.requests.append((url, {}, {}))
        return self.get_responses[url]

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


# A minimal DCAT catalog payload for orchestration tests -- two effective
# datasets and one Pending one, plus a non-shapefile entry that should be
# filtered out.
def _catalog_payload(*titles_with_id_and_pending: tuple[str, str, bool]) -> dict[str, Any]:
    """Build a fake DCAT-US payload from (title, guid, is_pending) tuples.
    All entries get a ZIP distribution so they survive the catalog filter.
    """
    return {
        "dataset": [
            {
                "title": title,
                "identifier": f"https://www.arcgis.com/home/item.html?id={guid}&sublayer=0",
                "distribution": [
                    {"format": "ZIP"},
                    {"format": "Web Page"},
                ],
            }
            for title, guid, _pending in titles_with_id_and_pending
        ]
    }


def test_fetch_uses_dynamic_catalog(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Fetches the DCAT catalog at runtime, then downloads each non-Pending
    dataset it advertises with a ZIP distribution."""
    catalog_entries = [
        ("Airports", "airports-guid", False),
        ("Runways", "runways-guid", False),
        ("Pending Airports", "pending-airports-guid", True),
    ]
    fake = _FakeClient()
    fake.get_responses[edai.EDAI_CATALOG_URL] = _FakeResponse(
        json_payload=_catalog_payload(*catalog_entries)
    )
    for _title, guid, _ in catalog_entries:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            zf.writestr(f"{guid}.shp", b"FAKE")
        fake.stream_responses[f"{edai.EDAI_BASE_URL}/{guid}_0/downloads/data"] = [
            _FakeResponse(body=zip_buf.getvalue())
        ]

    class _Module:
        Client = lambda *a, **kw: fake  # noqa: E731

    monkeypatch.setattr(edai, "httpx", _Module)

    result = edai.fetch(out_dir=tmp_path)

    assert isinstance(result, edai.EdaiFetchResult)
    # Pending dataset was filtered out; only the two effective ones downloaded.
    download_urls = [u for u, _, _ in fake.requests if "downloads/data" in u]
    assert len(download_urls) == 2
    assert (result.download_dir / "Airports.zip").exists()
    assert (result.download_dir / "Runways.zip").exists()
    assert not (result.download_dir / "Pending_Airports.zip").exists()


def test_fetch_include_pending_downloads_drafts(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """With include_pending=True, the Pending datasets are also downloaded."""
    catalog_entries = [
        ("Airports", "airports-guid", False),
        ("Pending Airports", "pending-airports-guid", True),
    ]
    fake = _FakeClient()
    fake.get_responses[edai.EDAI_CATALOG_URL] = _FakeResponse(
        json_payload=_catalog_payload(*catalog_entries)
    )
    for _, guid, _ in catalog_entries:
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w") as zf:
            zf.writestr(f"{guid}.shp", b"FAKE")
        fake.stream_responses[f"{edai.EDAI_BASE_URL}/{guid}_0/downloads/data"] = [
            _FakeResponse(body=zip_buf.getvalue())
        ]

    class _Module:
        Client = lambda *a, **kw: fake  # noqa: E731

    monkeypatch.setattr(edai, "httpx", _Module)

    edai.fetch(out_dir=tmp_path, include_pending=True)

    download_urls = [u for u, _, _ in fake.requests if "downloads/data" in u]
    assert len(download_urls) == 2  # both effective AND pending


# ---------------------------------------------------------------------------
# fetch_catalog / _parse_catalog
# ---------------------------------------------------------------------------


def test_parse_catalog_extracts_guid_and_sublayer():
    payload = {
        "dataset": [
            {
                "title": "Airports",
                "identifier": "https://www.arcgis.com/home/item.html?id=abc123&sublayer=0",
                "distribution": [{"format": "ZIP"}],
            },
            {
                "title": "VFR Terminal",
                # No sublayer -- defaults to 0.
                "identifier": "https://www.arcgis.com/home/item.html?id=def456",
                "distribution": [{"format": "ZIP"}],
            },
        ]
    }
    result = edai._parse_catalog(payload)
    assert [(r.title, r.guid, r.sublayer, r.hub_id) for r in result] == [
        ("Airports", "abc123", 0, "abc123_0"),
        ("VFR Terminal", "def456", 0, "def456_0"),
    ]


def test_parse_catalog_skips_non_shapefile_datasets():
    """VFR Sectional, ADDS-readme, etc. don't have a ZIP distribution and
    must be filtered out -- pyogrio can't read rasters or HTML."""
    payload = {
        "dataset": [
            {
                "title": "Has Shapefile",
                "identifier": "https://example/?id=a&sublayer=0",
                "distribution": [{"format": "ZIP"}],
            },
            {
                "title": "VFR Sectional",
                "identifier": "https://example/?id=b&sublayer=0",
                "distribution": [{"format": "Web Page"}],  # raster only
            },
            {
                "title": "ADDS-readme",
                "identifier": "https://example/?id=c",
                "distribution": [{"format": "Web Page"}],
            },
        ]
    }
    titles = [r.title for r in edai._parse_catalog(payload)]
    assert titles == ["Has Shapefile"]


def test_parse_catalog_marks_pending_datasets():
    """FAA uses both "Pending X" (prefix) and "XPending" (suffix) styles --
    is_pending must catch both."""
    payload = {
        "dataset": [
            {
                "title": "Pending Airports",  # prefix form
                "identifier": "https://example/?id=a",
                "distribution": [{"format": "ZIP"}],
            },
            {
                "title": "RoutePortionPending",  # suffix form
                "identifier": "https://example/?id=b",
                "distribution": [{"format": "ZIP"}],
            },
            {
                "title": "Airports",  # not pending
                "identifier": "https://example/?id=c",
                "distribution": [{"format": "ZIP"}],
            },
        ]
    }
    by_title = {r.title: r for r in edai._parse_catalog(payload)}
    assert by_title["Pending Airports"].is_pending is True
    assert by_title["RoutePortionPending"].is_pending is True
    assert by_title["Airports"].is_pending is False


def test_parse_catalog_skips_entries_missing_title_or_id():
    payload = {
        "dataset": [
            {
                "title": "",  # missing title
                "identifier": "https://example/?id=a",
                "distribution": [{"format": "ZIP"}],
            },
            {
                "title": "No ID",
                "identifier": "https://example/no-id-here",
                "distribution": [{"format": "ZIP"}],
            },
            {
                "title": "Good",
                "identifier": "https://example/?id=z",
                "distribution": [{"format": "ZIP"}],
            },
        ]
    }
    titles = [r.title for r in edai._parse_catalog(payload)]
    assert titles == ["Good"]


def test_parse_catalog_handles_invalid_sublayer_gracefully():
    payload = {
        "dataset": [
            {
                "title": "X",
                "identifier": "https://example/?id=g&sublayer=not-an-int",
                "distribution": [{"format": "ZIP"}],
            }
        ]
    }
    result = edai._parse_catalog(payload)
    assert result[0].sublayer == 0


def test_fetch_catalog_uses_provided_client():
    """When passed a client, fetch_catalog reuses it instead of opening a new one."""
    fake = _FakeClient()
    fake.get_responses[edai.EDAI_CATALOG_URL] = _FakeResponse(
        json_payload={
            "dataset": [
                {
                    "title": "Foo",
                    "identifier": "https://example/?id=foo",
                    "distribution": [{"format": "ZIP"}],
                }
            ]
        }
    )
    result = edai.fetch_catalog(_as_client(fake))
    assert [r.title for r in result] == ["Foo"]


def test_fetch_catalog_opens_its_own_client_when_none_provided(
    monkeypatch: pytest.MonkeyPatch,
):
    """No client passed -> fetch_catalog opens one via httpx.Client()."""
    fake = _FakeClient()
    fake.get_responses[edai.EDAI_CATALOG_URL] = _FakeResponse(
        json_payload={
            "dataset": [
                {
                    "title": "Bar",
                    "identifier": "https://example/?id=bar",
                    "distribution": [{"format": "ZIP"}],
                }
            ]
        }
    )

    class _Module:
        Client = lambda *a, **kw: fake  # noqa: E731

    monkeypatch.setattr(edai, "httpx", _Module)

    result = edai.fetch_catalog()
    assert [r.title for r in result] == ["Bar"]


def test_fetch_continues_when_individual_dataset_500s(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """One bad dataset shouldn't block the rest -- the failure is logged and
    fetch keeps going. (Regression for the real-world Frequency 500.)"""
    catalog_entries = [
        ("Good", "good-guid", False),
        ("Bad", "bad-guid", False),
    ]
    fake = _FakeClient()
    fake.get_responses[edai.EDAI_CATALOG_URL] = _FakeResponse(
        json_payload=_catalog_payload(*catalog_entries)
    )
    # "Good" succeeds, "Bad" returns 500.
    good_zip = io.BytesIO()
    with zipfile.ZipFile(good_zip, "w") as zf:
        zf.writestr("good.shp", b"FAKE")
    fake.stream_responses[f"{edai.EDAI_BASE_URL}/good-guid_0/downloads/data"] = [
        _FakeResponse(body=good_zip.getvalue())
    ]

    # Make the bad URL raise httpx.HTTPStatusError when raise_for_status() is called.
    class _ErrResp(_FakeResponse):
        def raise_for_status(self) -> None:
            req = httpx.Request("GET", "https://example/")
            resp = httpx.Response(500, request=req)
            raise httpx.HTTPStatusError("500", request=req, response=resp)

    fake.stream_responses[f"{edai.EDAI_BASE_URL}/bad-guid_0/downloads/data"] = [_ErrResp()]

    class _Module:
        Client = lambda *a, **kw: fake  # noqa: E731
        HTTPStatusError = httpx.HTTPStatusError

    monkeypatch.setattr(edai, "httpx", _Module)

    result = edai.fetch(out_dir=tmp_path)

    # Good dataset extracted; bad one logged and skipped without crashing.
    assert (result.download_dir / "Good.zip").exists()
    assert not (result.download_dir / "Bad.zip").exists()


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
