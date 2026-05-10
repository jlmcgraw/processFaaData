"""Tests for faa_nasr.fetch.

The pure-filesystem helpers (`_extract_zip`, `_extract_inner_csv_bundle`)
are exercised directly. The HTTP-touching helpers are tested by injecting
a fake `httpx.Client` via `monkeypatch.setattr(fetch, "httpx", ...)`.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pytest

from faa_nasr import fetch


def _make_zip(path: Path, files: dict[str, bytes]) -> None:
    """Write a zip file containing each (name, bytes) entry."""
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)


# ---------------------------------------------------------------------------
# _extract_zip
# ---------------------------------------------------------------------------


def test_extract_zip_writes_files_to_dest(tmp_path):
    src = tmp_path / "src.zip"
    _make_zip(src, {"a.txt": b"hello", "sub/b.txt": b"world"})
    dest = tmp_path / "out"

    result = fetch._extract_zip(src, dest)

    assert result == dest
    assert (dest / "a.txt").read_bytes() == b"hello"
    assert (dest / "sub" / "b.txt").read_bytes() == b"world"


def test_extract_zip_creates_dest_if_missing(tmp_path):
    src = tmp_path / "src.zip"
    _make_zip(src, {"a.txt": b"x"})
    dest = tmp_path / "deep" / "dest"  # parent doesn't exist

    fetch._extract_zip(src, dest)

    assert (dest / "a.txt").exists()


# ---------------------------------------------------------------------------
# _extract_inner_csv_bundle
# ---------------------------------------------------------------------------


def test_extract_inner_csv_bundle_finds_and_extracts_bundle(tmp_path):
    nasr_dir = tmp_path / "nasr"
    csv_data = nasr_dir / "CSV_Data"
    csv_data.mkdir(parents=True)
    bundle = csv_data / "16_Apr_2026_CSV.zip"
    _make_zip(bundle, {"APT_BASE.csv": b"ARPT_ID\nIAD\n"})

    csv_dir = fetch._extract_inner_csv_bundle(nasr_dir)

    assert csv_dir == csv_data / "extracted"
    assert (csv_dir / "APT_BASE.csv").read_bytes() == b"ARPT_ID\nIAD\n"


def test_extract_inner_csv_bundle_skips_delta_change_report_zips(tmp_path):
    """Filenames like 19_Mar_..._CSV-16_Apr_..._CSV.zip are change reports
    -- they have a '-<digit>' marker in the stem and should be filtered out."""
    nasr_dir = tmp_path / "nasr"
    csv_data = nasr_dir / "CSV_Data"
    csv_data.mkdir(parents=True)
    real = csv_data / "16_Apr_2026_CSV.zip"
    delta = csv_data / "19_Mar_2026_CSV-16_Apr_2026_CSV.zip"
    _make_zip(real, {"a.csv": b"x"})
    _make_zip(delta, {"delta_marker.csv": b"y"})

    csv_dir = fetch._extract_inner_csv_bundle(nasr_dir)

    # Only the real bundle's contents should appear.
    assert (csv_dir / "a.csv").exists()
    assert not (csv_dir / "delta_marker.csv").exists()


def test_extract_inner_csv_bundle_raises_when_no_bundle(tmp_path):
    nasr_dir = tmp_path / "nasr"
    (nasr_dir / "CSV_Data").mkdir(parents=True)
    with pytest.raises(FileNotFoundError, match="no CSV bundle found"):
        fetch._extract_inner_csv_bundle(nasr_dir)


# ---------------------------------------------------------------------------
# HTTP helpers, mocked via a fake httpx
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(
        self, *, status_code: int = 200, payload: dict[str, Any] | None = None, body: bytes = b""
    ):
        self.status_code = status_code
        self._payload = payload
        self._body = body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return self._payload

    def iter_bytes(self) -> Any:
        # Simulate streaming chunks.
        yield self._body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


class _FakeClient:
    """Records the get/stream URLs and returns canned responses."""

    def __init__(
        self,
        *,
        get_responses: dict[str, _FakeResponse] | None = None,
        stream_responses: dict[str, _FakeResponse] | None = None,
    ) -> None:
        self.get_responses = get_responses or {}
        self.stream_responses = stream_responses or {}
        self.requested_urls: list[str] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:  # pragma: no cover
        super().__init_subclass__(**kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> _FakeClient:
        # fetch.py does `httpx.Client(timeout=...)` -- swallow init args.
        return self

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.requested_urls.append(url)
        return self.get_responses[url]

    def stream(self, method: str, url: str) -> _FakeResponse:
        self.requested_urls.append(url)
        return self.stream_responses[url]


@pytest.fixture
def fake_httpx(monkeypatch: pytest.MonkeyPatch) -> _FakeClient:
    """Monkeypatch fetch.httpx.Client with a recording fake. Tests configure
    its responses by mutating the returned _FakeClient instance."""
    fake = _FakeClient()

    class _Module:
        Client = fake

    monkeypatch.setattr(fetch, "httpx", _Module)
    return fake


def test_download_nasr_resolves_url_via_api_then_streams(fake_httpx, tmp_path):
    """Happy path: hit the FAA API for the URL, then stream the zip to disk."""
    api_payload = {
        "edition": [{"product": {"url": "https://nfdc.faa.gov/webContent/28DaySub/cycle.zip"}}]
    }
    fake_httpx.get_responses = {
        fetch.NASR_API_URL: _FakeResponse(payload=api_payload),
    }
    fake_httpx.stream_responses = {
        "https://nfdc.faa.gov/webContent/28DaySub/cycle.zip": _FakeResponse(body=b"ZIPDATA"),
    }

    dest = fetch._download_nasr(out_dir=tmp_path, edition="current")

    assert dest == tmp_path / "cycle.zip"
    assert dest.read_bytes() == b"ZIPDATA"


def test_download_nasr_uses_cached_file_if_present(fake_httpx, tmp_path):
    """If the destination already exists, no streaming download happens."""
    api_payload = {"edition": [{"product": {"url": "https://example.com/cycle.zip"}}]}
    fake_httpx.get_responses = {fetch.NASR_API_URL: _FakeResponse(payload=api_payload)}
    # No stream_responses configured -- if streaming were attempted it'd KeyError.

    cached = tmp_path / "cycle.zip"
    cached.write_bytes(b"OLD CONTENT")

    dest = fetch._download_nasr(out_dir=tmp_path, edition="current")

    assert dest == cached
    assert dest.read_bytes() == b"OLD CONTENT"  # not overwritten
    assert "https://example.com/cycle.zip" not in fake_httpx.requested_urls


def test_download_nasr_passes_edition_param(fake_httpx, tmp_path):
    """The 'edition' arg flows through to the API call as a query param."""
    api_payload = {"edition": [{"product": {"url": "https://example.com/x.zip"}}]}
    fake_httpx.get_responses = {fetch.NASR_API_URL: _FakeResponse(payload=api_payload)}
    fake_httpx.stream_responses = {"https://example.com/x.zip": _FakeResponse(body=b"")}

    # Override .get to capture kwargs.
    captured: dict[str, Any] = {}
    original_get = fake_httpx.get

    def recording_get(url: str, **kwargs: Any) -> _FakeResponse:
        captured.update(kwargs)
        return original_get(url, **kwargs)

    fake_httpx.get = recording_get  # type: ignore[method-assign]

    fetch._download_nasr(out_dir=tmp_path, edition="next")

    assert captured["params"] == {"edition": "next"}


def test_download_obstacles_streams_zip_then_extracts_csv(fake_httpx, tmp_path):
    # Build a zip in memory containing DOF.CSV.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("DOF.CSV", b"OAS,LATDEC\n01-001,30.0\n")
    fake_httpx.stream_responses = {fetch.DOF_CSV_URL: _FakeResponse(body=buf.getvalue())}

    csv_path = fetch._download_obstacles(out_dir=tmp_path)

    assert csv_path.name == "DOF.CSV"
    assert csv_path.read_text() == "OAS,LATDEC\n01-001,30.0\n"


# ---------------------------------------------------------------------------
# fetch() end-to-end
# ---------------------------------------------------------------------------


def test_fetch_orchestrates_full_pipeline(fake_httpx, tmp_path):
    """End-to-end: hit API, download NASR zip, extract, extract inner CSV
    bundle, optionally download DOF, return FetchResult with paths."""
    # Build the inner CSV bundle and the outer NASR zip in memory.
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as zf:
        zf.writestr("APT_BASE.csv", b"ARPT_ID\nIAD\n")

    outer = io.BytesIO()
    with zipfile.ZipFile(outer, "w") as zf:
        zf.writestr("CSV_Data/16_Apr_2026_CSV.zip", inner.getvalue())

    dof = io.BytesIO()
    with zipfile.ZipFile(dof, "w") as zf:
        zf.writestr("DOF.CSV", b"OAS,LATDEC\n01-001,30.0\n")

    api_payload = {
        "edition": [{"product": {"url": "https://nfdc.faa.gov/webContent/28DaySub/cycle.zip"}}]
    }
    fake_httpx.get_responses = {fetch.NASR_API_URL: _FakeResponse(payload=api_payload)}
    fake_httpx.stream_responses = {
        "https://nfdc.faa.gov/webContent/28DaySub/cycle.zip": _FakeResponse(body=outer.getvalue()),
        fetch.DOF_CSV_URL: _FakeResponse(body=dof.getvalue()),
    }

    result = fetch.fetch(out_dir=tmp_path, edition="current", include_obstacles=True)

    assert isinstance(result, fetch.FetchResult)
    assert result.nasr_dir == (tmp_path / "cycle").resolve()
    assert (result.csv_dir / "APT_BASE.csv").exists()
    assert result.obstacle_csv is not None
    assert result.obstacle_csv.name == "DOF.CSV"


def test_fetch_skips_obstacles_when_disabled(fake_httpx, tmp_path):
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as zf:
        zf.writestr("APT_BASE.csv", b"x")

    outer = io.BytesIO()
    with zipfile.ZipFile(outer, "w") as zf:
        zf.writestr("CSV_Data/16_Apr_2026_CSV.zip", inner.getvalue())

    api_payload = {"edition": [{"product": {"url": "https://example.com/cycle.zip"}}]}
    fake_httpx.get_responses = {fetch.NASR_API_URL: _FakeResponse(payload=api_payload)}
    fake_httpx.stream_responses = {
        "https://example.com/cycle.zip": _FakeResponse(body=outer.getvalue())
    }

    result = fetch.fetch(out_dir=tmp_path, edition="current", include_obstacles=False)

    assert result.obstacle_csv is None
    assert fetch.DOF_CSV_URL not in fake_httpx.requested_urls
