"""Download FAA EDAI shapefile datasets and build a SpatiaLite database.

EDAI (Enterprise Data) is the FAA's ArcGIS Hub open-data feed -- a parallel
publication of NASR-equivalent data as shapefiles, plus datasets that aren't
in the NASR subscription (TFRs, Stadiums, VFR/IFR chart layers, Airport
Mapping layers, etc.).

The dataset list is fetched dynamically from the FAA's DCAT catalog at
runtime so we always have the current set without hardcoded GUIDs going
stale. (The original jlmcgraw/edai_data script's host is gone, and its
21-dataset hardcoded list is now incomplete -- the current catalog has
~50 effective shapefile datasets plus draft "Pending" variants.)
"""

from __future__ import annotations

import contextlib
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
# DCAT-US 1.1 catalog of every dataset published on the FAA Hub.
EDAI_CATALOG_URL = "https://adds-faa.opendata.arcgis.com/api/feed/dcat-us/1.1.json"
# WGS84. The Hub requires spatialRefId; the Airport Mapping ("AM ...")
# datasets only publish in 4326, while every other EDAI dataset is happy
# to serve either 4269 or 4326 -- so 4326 is the one value that works
# across the whole catalog.
EDAI_SPATIAL_REF_ID = 4326
EDAI_OUTPUT_DB = "edai_spatialite.sqlite"


@dataclass(frozen=True)
class EdaiDatasetMeta:
    """One dataset entry from the FAA's DCAT catalog.

    `hub_id` (the `<guid>_<sublayer>` form used in download URLs) is the
    only identifier the Hub V3 API accepts.
    """

    guid: str
    sublayer: int
    title: str
    is_pending: bool

    @property
    def hub_id(self) -> str:
        return f"{self.guid}_{self.sublayer}"


@dataclass(frozen=True)
class EdaiFetchResult:
    download_dir: Path  # holds the per-dataset .zip files
    extract_dir: Path  # extracted shapefiles, one subdir per dataset


def fetch_catalog(client: httpx.Client | None = None) -> list[EdaiDatasetMeta]:
    """Fetch the FAA DCAT catalog and return one EdaiDatasetMeta per dataset
    that offers a shapefile (ZIP) download. Strips out raster/web-only
    datasets (VFR Sectional, ADDS-readme, etc.) and other non-spatial entries.
    """
    ctx = (
        httpx.Client(timeout=30.0, follow_redirects=True)
        if client is None
        else contextlib.nullcontext(client)
    )
    with ctx as c:
        resp = c.get(EDAI_CATALOG_URL)
        resp.raise_for_status()
        payload = resp.json()
    return [m for m in _parse_catalog(payload) if m is not None]


def fetch(out_dir: Path, include_pending: bool = False) -> EdaiFetchResult:
    """Download every EDAI dataset zip with timestamp-based caching, then
    extract each into its own subdirectory of `extract_dir`.

    Pulls the dataset list dynamically from the FAA DCAT catalog. Pass
    `include_pending=True` to also fetch the draft "Pending" variants
    (upcoming-cycle preview data).
    """
    download_dir = (out_dir / "edai_downloads").resolve()
    extract_dir = (out_dir / "edai_extracted").resolve()
    download_dir.mkdir(parents=True, exist_ok=True)
    extract_dir.mkdir(parents=True, exist_ok=True)

    _log.step(f"fetch-edai -> {download_dir}")
    failures: list[tuple[str, str]] = []
    with httpx.Client(timeout=120.0, follow_redirects=True) as client:
        catalog = fetch_catalog(client)
        if not include_pending:
            catalog = [d for d in catalog if not d.is_pending]
        _log.info(f"  {len(catalog)} dataset(s) from catalog")

        bar = tqdm(
            catalog,
            desc="  EDAI",
            unit="file",
            disable=_log.is_quiet(),
            leave=True,
        )
        for meta in bar:
            bar.set_postfix_str(meta.title, refresh=False)
            try:
                _fetch_one(client, meta.hub_id, download_dir / f"{_safe_name(meta.title)}.zip")
            except httpx.HTTPStatusError as exc:
                # ArcGIS Hub occasionally 500s on individual datasets; log
                # and keep going so one bad dataset doesn't block the rest.
                failures.append((meta.title, str(exc.response.status_code)))
    if failures:
        _log.info(
            f"  {len(failures)} dataset(s) failed: "
            + ", ".join(f"{name}({code})" for name, code in failures)
        )

    _extract_all(download_dir, extract_dir)
    return EdaiFetchResult(download_dir=download_dir, extract_dir=extract_dir)


def _parse_catalog(payload: dict) -> list[EdaiDatasetMeta]:
    """Turn the DCAT JSON into EdaiDatasetMeta entries.

    Each entry's `identifier` looks like
    `https://www.arcgis.com/home/item.html?id=<GUID>[&sublayer=N]`. We split
    out the GUID + sublayer, default sublayer to 0 when absent. Datasets
    without a ZIP distribution (rasters, web pages, the README) are dropped.
    """
    out: list[EdaiDatasetMeta] = []
    for d in payload.get("dataset", []):
        title = (d.get("title") or "").strip()
        ident = d.get("identifier", "")
        if not title or "id=" not in ident:
            continue
        # Skip datasets with no shapefile (ZIP) distribution.
        formats = {x.get("format", "") for x in d.get("distribution", [])}
        if "ZIP" not in formats:
            continue

        rest = ident.split("id=", 1)[1]
        parts = rest.split("&")
        guid = parts[0]
        sublayer = 0
        for p in parts[1:]:
            if p.startswith("sublayer="):
                try:
                    sublayer = int(p.split("=", 1)[1])
                except ValueError:
                    sublayer = 0
                break

        out.append(
            EdaiDatasetMeta(
                guid=guid,
                sublayer=sublayer,
                title=title,
                # FAA uses "Pending X" (prefix) for most draft datasets but
                # "XPending" (suffix) for a handful (RoutePortionPending,
                # FrequencyPending, EnrouteInformationPending).
                is_pending="pending" in title.lower(),
            )
        )
    return out


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
