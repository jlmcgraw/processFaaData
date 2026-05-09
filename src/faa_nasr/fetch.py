"""Download the FAA 28-day NASR subscription and the daily DOF obstacle file."""

from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path

import httpx

NASR_API_URL = "https://external-api.faa.gov/apra/nfdc/nasr/chart"
DOF_CSV_URL = "https://aeronav.faa.gov/Obst_Data/DAILY_DOF_CSV.ZIP"


@dataclass(frozen=True)
class FetchResult:
    nasr_dir: Path  # extracted top-level NASR directory
    csv_dir: Path  # extracted CSV bundle directory
    obstacle_csv: Path | None  # extracted DOF.CSV path, or None


def fetch(out_dir: Path, edition: str = "current", include_obstacles: bool = True) -> FetchResult:
    """Download the requested NASR edition and (optionally) the DOF, return paths."""
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    nasr_zip = _download_nasr(out_dir=out_dir, edition=edition)
    nasr_dir = _extract_zip(nasr_zip, out_dir / nasr_zip.stem)
    csv_dir = _extract_inner_csv_bundle(nasr_dir)

    obstacle_csv: Path | None = None
    if include_obstacles:
        obstacle_csv = _download_obstacles(out_dir=out_dir)

    return FetchResult(nasr_dir=nasr_dir, csv_dir=csv_dir, obstacle_csv=obstacle_csv)


def _download_nasr(out_dir: Path, edition: str) -> Path:
    """Resolve the current/next subscription URL via the FAA API and download it."""
    with httpx.Client(timeout=30.0, follow_redirects=True) as client:
        meta = client.get(
            NASR_API_URL,
            params={"edition": edition},
            headers={"accept": "application/json"},
        )
        meta.raise_for_status()
        payload = meta.json()
        url = payload["edition"][0]["product"]["url"]
        dest = out_dir / Path(url).name
        if dest.exists():
            return dest
        _stream_to_file(client, url, dest)
    return dest


def _download_obstacles(out_dir: Path) -> Path:
    """Download DAILY_DOF_CSV.ZIP and extract DOF.CSV; return the CSV path."""
    with httpx.Client(timeout=60.0, follow_redirects=True) as client:
        zip_path = out_dir / "DAILY_DOF_CSV.ZIP"
        _stream_to_file(client, DOF_CSV_URL, zip_path)
    extract_dir = out_dir / "dof"
    extract_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    csv_path = next(extract_dir.glob("DOF*.CSV"), None) or next(extract_dir.glob("DOF*.csv"))
    return csv_path


def _stream_to_file(client: httpx.Client, url: str, dest: Path) -> None:
    with client.stream("GET", url) as resp:
        resp.raise_for_status()
        with dest.open("wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)


def _extract_zip(zip_path: Path, dest: Path) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest)
    return dest


def _extract_inner_csv_bundle(nasr_dir: Path) -> Path:
    """Find and extract `CSV_Data/<date>_CSV.zip` from the unpacked NASR dir."""
    candidates = list((nasr_dir / "CSV_Data").glob("*_CSV.zip"))
    # Filter out delta/change-report bundles like '19_Mar_..._CSV-16_Apr_..._CSV.zip'.
    candidates = [c for c in candidates if not re.search(r"-\d", c.stem.split("_CSV")[0])]
    if not candidates:
        raise FileNotFoundError(f"no CSV bundle found under {nasr_dir / 'CSV_Data'}")
    bundle = candidates[0]
    csv_dir = nasr_dir / "CSV_Data" / "extracted"
    csv_dir.mkdir(exist_ok=True)
    with zipfile.ZipFile(bundle) as zf:
        zf.extractall(csv_dir)
    return csv_dir
