"""Download FAA EDAI shapefile datasets and build a SpatiaLite database.

EDAI (Enterprise Data) is the FAA's ArcGIS Hub open-data feed -- a parallel
publication of NASR-equivalent data as shapefiles, plus a few datasets that
aren't in the NASR subscription (TFRs, Stadiums, Pending changes, etc.).

The dataset list and naming convention mirror jlmcgraw/edai_data's
`freshen_edai_data.sh` so consumers see the same layer set. The download URL
format is the current one (https://hub.arcgis.com/api/v3/datasets/...) --
the script's old `ais.faa.opendata.arcgis.com` host is gone.
"""

from __future__ import annotations

import email.utils
import os
import zipfile
from dataclasses import dataclass
from pathlib import Path

import httpx
from tqdm import tqdm

from faa_nasr import _log
from faa_nasr.airspace import _copy_shapefile, _init_spatialite_db, _safe_name

EDAI_BASE_URL = "https://hub.arcgis.com/api/v3/datasets"
# NAD83 -- the FAA's published reference system for these datasets.
EDAI_SPATIAL_REF_ID = 4269
EDAI_OUTPUT_DB = "edai_spatialite.sqlite"

# {GUID: human-readable description}. GUIDs are stable item IDs in ArcGIS
# Hub; original list comes from jlmcgraw/edai_data's freshen_edai_data.sh.
EDAI_DATASETS: dict[str, str] = {
    "4d8fa46181aa470d809776c57a8ab1f6_0": "Runways",
    "0c6899de28af447c801231ed7ba7baa6_0": "MTR_Segment",
    "d5c81ec19e0d43748d5bb0a1e36b6341_0": "Changeover_Point",
    "f02750503edb4a69875cb1f744219370_0": "Route_Portion",
    "c6a62360338e408cb1512366ad61559e_0": "Class_Airspace",
    "8bf861bb9b414f4ea9f0ff2ca0f1a851_0": "Route_Airspace",
    "3f42ed70dba34ef09a3c03c68ea78d80_0": "Frequency",
    "c9254c171b6741d3a5e494860761443a_0": "NAVAID_Component",
    "3a379be9c3504403907ef6cabd20ea34_0": "ILS_Component",
    "990e238991b44dd08af27d7b43e70b92_0": "NAVAID_System",
    "9dcdee16e66b47d59c17f4dae53f6721_0": "ILS",
    "861043a88ff4486c97c3789e7dcdccc6_0": "Designated_Point",
    "ba57404f70184b858d2c929f99f7b40c_0": "Holding_Pattern",
    "6e89f7409c2f486894f5393859232cc9_0": "Services",
    "8458b1e305ff47ee9e4b840b63990da2_0": "Radial_Bearing",
    "e747ab91a11045e8b3f8a3efd093d3b5_0": "Airports",
    "67885972e4e940b2aa6d74024901c561_0": "Airspace_Boundary",
    "826bda9e0b324006a2da8f20ff334190_0": "EnRoute_Information",
    "5344a67700d543b582874b2da9c20559_0": "Notes",
    "dd0d1b726e504137ab3c41b21835d05b_0": "Special_Use_Airspace",
    "acf64966af5f48a1a40fdbcb31238ba7_0": "ATS_Route",
}


@dataclass(frozen=True)
class EdaiFetchResult:
    download_dir: Path  # holds the per-dataset .zip files
    extract_dir: Path  # extracted shapefiles, one subdir per dataset


def fetch(out_dir: Path) -> EdaiFetchResult:
    """Download every EDAI dataset zip with timestamp-based caching, then
    extract each into its own subdirectory of `extract_dir`.

    The HTTP cache uses `If-Modified-Since` against each file's mtime, so
    re-running this is cheap when nothing has changed upstream.
    """
    download_dir = (out_dir / "edai_downloads").resolve()
    extract_dir = (out_dir / "edai_extracted").resolve()
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    _log.step(f"fetch-edai -> {download_dir}")
    failures: list[tuple[str, str]] = []
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        bar = tqdm(
            EDAI_DATASETS.items(),
            desc="  EDAI",
            unit="file",
            disable=_log.is_quiet(),
            leave=True,
        )
        for guid, description in bar:
            bar.set_postfix_str(description, refresh=False)
            try:
                _fetch_one(client, guid, download_dir / f"{description}.zip")
            except httpx.HTTPStatusError as exc:
                # ArcGIS Hub occasionally 500s on individual datasets; log
                # and keep going so one bad dataset doesn't block the rest.
                failures.append((description, str(exc.response.status_code)))
    if failures:
        _log.info(
            f"  {len(failures)} dataset(s) failed: "
            + ", ".join(f"{name}({code})" for name, code in failures)
        )

    _extract_all(download_dir, extract_dir)
    return EdaiFetchResult(download_dir=download_dir, extract_dir=extract_dir)


def build(out_dir: Path, extract_dir: Path) -> None:
    """Build edai_spatialite.sqlite from every .shp under extract_dir.

    Each shapefile becomes a layer named after its file stem (sanitised by
    `_safe_name`). pyogrio handles the conversion + spatial-index creation
    on each layer write.
    """
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / EDAI_OUTPUT_DB
    _log.step(f"build-edai -> {dst}")
    if dst.exists():
        dst.unlink()
    _init_spatialite_db(dst)

    shapefiles = sorted(extract_dir.rglob("*.shp"))
    total = 0
    bar = tqdm(shapefiles, desc="  shapefiles", unit="file", disable=_log.is_quiet(), leave=True)
    for shp in bar:
        bar.set_postfix_str(shp.name, refresh=False)
        total += _copy_shapefile(src=shp, dst=dst, layer_name=_safe_name(shp.stem))
    _log.info(f"  wrote {len(shapefiles)} shapefiles / {total:,} features")


def _fetch_one(client: httpx.Client, guid: str, dest: Path) -> None:
    """Download `dest` only if remote is newer than the local mtime.

    Uses ArcGIS Hub V3's downloads endpoint. If `dest` exists, sends an
    `If-Modified-Since` header; on 304 we keep the local copy. On 200 we
    overwrite and reset the file's mtime to match the server's
    `Last-Modified` so subsequent runs can short-circuit again.
    """
    url = f"{EDAI_BASE_URL}/{guid}/downloads/data"
    params: dict[str, str | int] = {"format": "shp", "spatialRefId": EDAI_SPATIAL_REF_ID}
    headers: dict[str, str] = {}
    if dest.exists():
        headers["If-Modified-Since"] = email.utils.formatdate(dest.stat().st_mtime, usegmt=True)

    with client.stream("GET", url, params=params, headers=headers) as resp:
        if resp.status_code == 304:
            return
        resp.raise_for_status()
        with dest.open("wb") as f:
            for chunk in resp.iter_bytes():
                f.write(chunk)
        last_mod = resp.headers.get("Last-Modified")
        if last_mod:
            ts = email.utils.parsedate_to_datetime(last_mod).timestamp()
            os.utime(dest, (ts, ts))


def _extract_all(download_dir: Path, extract_dir: Path) -> None:
    """Extract every zip under download_dir into a per-zip subdirectory of
    extract_dir, so shapefiles from different datasets can't collide on
    same-named members (e.g. "metadata.xml")."""
    for zip_path in sorted(download_dir.glob("*.zip")):
        per_zip_dir = extract_dir / zip_path.stem
        per_zip_dir.mkdir(exist_ok=True)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(per_zip_dir)
