"""Build a SpatiaLite database from FAA EDAI shapefile datasets.

EDAI (Enterprise Data) is the FAA's ArcGIS Hub open-data feed -- a parallel
publication of NASR-equivalent data as shapefiles, plus datasets that aren't
in the NASR subscription (TFRs, Stadiums, VFR/IFR chart layers, Airport
Mapping layers, etc.).
"""

from __future__ import annotations

from pathlib import Path

from tqdm import tqdm

from faa_nasr import _log
from faa_nasr.airspace import _copy_shapefile, _init_spatialite_db, _safe_name

EDAI_OUTPUT_DB = "edai_spatialite.sqlite"


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
