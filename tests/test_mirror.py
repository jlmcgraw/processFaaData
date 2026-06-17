"""Tests for aviation-data-mirror manifest input resolution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from faa_nasr import mirror


def _path_entry(path: Path, root: Path) -> dict[str, object]:
    return {"path": str(path.relative_to(root)), "exists": True}


def test_resolve_inputs_uses_manifest_paths(tmp_path):
    root = tmp_path / "aviation_data"
    nasr_dir = root / "data/products/nasr/2026-05-14/NASR/zip/extracted"
    csv_dir = nasr_dir / "CSV_Data/extracted"
    dof_dir = root / "data/products/daily_dof/uncycled/Daily_DOF/csv_zip/extracted"
    cifp_dir = root / "data/products/cifp/2026-05-14/CIFP_260514/zip/extracted"
    edai_a = root / "data/products/edai/2026-05-14/Airports/shp_zip/extracted"
    edai_b = root / "data/products/edai/2026-05-14/Runways/shp_zip/extracted"

    for path in (csv_dir, dof_dir, cifp_dir, edai_a, edai_b):
        path.mkdir(parents=True)
    (csv_dir / "APT_BASE.csv").write_text("ARPT_ID\n")
    (dof_dir / "DOF.CSV").write_text("OAS\n")
    (cifp_dir / "FAACIFP18").write_text("HDR\n")
    (edai_a / "Airports.shp").touch()
    (edai_b / "Runways.shp").touch()

    manifest = {
        "artifacts": [
            {
                "product": "nasr",
                "name": "NASR",
                "cycle": "2026-05-14",
                "publication_status": "current",
                "paths": {"extracted": _path_entry(nasr_dir, root)},
            },
            {
                "product": "daily_dof",
                "name": "Daily_DOF",
                "publication_status": "latest",
                "paths": {"extracted": _path_entry(dof_dir, root)},
            },
            {
                "product": "cifp",
                "name": "CIFP_260514",
                "cycle": "2026-05-14",
                "publication_status": "current",
                "paths": {"extracted": _path_entry(cifp_dir, root)},
            },
            {
                "product": "edai",
                "name": "Airports",
                "cycle": "2026-05-14",
                "publication_status": "current",
                "paths": {"extracted": _path_entry(edai_a, root)},
            },
            {
                "product": "edai",
                "name": "Runways",
                "cycle": "2026-05-14",
                "publication_status": "current",
                "paths": {"extracted": _path_entry(edai_b, root)},
            },
        ]
    }
    (root / "manifest.json").write_text(json.dumps(manifest))

    result = mirror.resolve_inputs(root)

    assert result.nasr_dir == nasr_dir.resolve()
    assert result.csv_dir == csv_dir.resolve()
    assert result.obstacle_csv == (dof_dir / "DOF.CSV").resolve()
    assert result.cifp_file == (cifp_dir / "FAACIFP18").resolve()
    assert result.edai_dir == (root / "data/products/edai/2026-05-14").resolve()


def test_resolve_inputs_prefers_relative_manifest_paths(tmp_path):
    root = tmp_path / "mounted_mirror"
    nasr_dir = root / "data/products/nasr/2026-05-14/NASR/zip/extracted"
    csv_dir = nasr_dir / "CSV_Data/extracted"
    csv_dir.mkdir(parents=True)
    (csv_dir / "APT_BASE.csv").write_text("ARPT_ID\n")
    manifest = {
        "artifacts": [
            {
                "product": "nasr",
                "name": "NASR",
                "publication_status": "current",
                "paths": {
                    "extracted": {
                        "path": "/host/path/that/does/not/exist",
                        "relative_path": "data/products/nasr/2026-05-14/NASR/zip/extracted",
                        "exists": True,
                    }
                },
            }
        ]
    }

    result = mirror.resolve_nasr_dirs(manifest=manifest, mirror_root=root)

    assert result == (nasr_dir.resolve(), csv_dir.resolve())


def test_resolve_cifp_file_raises_when_only_product_zip(tmp_path):
    root = tmp_path / "aviation_data"
    zip_path = root / "data/products/cifp/current/CIFP_260514/zip/CIFP_260514.zip"
    zip_path.parent.mkdir(parents=True)
    zip_path.write_text("not-a-real-zip")
    manifest = {
        "artifacts": [
            {
                "product": "cifp",
                "name": "CIFP_260514",
                "publication_status": "current",
                "paths": {"product": _path_entry(zip_path, root)},
            }
        ]
    }

    with pytest.raises(FileNotFoundError, match="extraction enabled"):
        mirror.resolve_cifp_file(manifest=manifest, mirror_root=root)


def test_resolve_obstacle_csv_raises_when_only_product_zip(tmp_path):
    root = tmp_path / "aviation_data"
    zip_path = root / "data/products/daily_dof/current/DDOF_CSV/csv_zip/DAILY_DOF_CSV.ZIP"
    zip_path.parent.mkdir(parents=True)
    zip_path.write_text("not-a-real-zip")
    manifest = {
        "artifacts": [
            {
                "product": "daily_dof",
                "name": "DDOF CSV File",
                "format": "csv_zip",
                "publication_status": "latest",
                "paths": {"product": _path_entry(zip_path, root)},
            },
        ]
    }

    with pytest.raises(FileNotFoundError, match="extraction enabled"):
        mirror.resolve_obstacle_csv(manifest=manifest, mirror_root=root)


def test_resolve_nasr_dirs_requires_extracted_artifact(tmp_path):
    root = tmp_path / "aviation_data"
    root.mkdir()
    manifest = {
        "artifacts": [
            {
                "product": "nasr",
                "publication_status": "current",
                "paths": {
                    "product": {"path": "data/products/nasr/current/NASR.zip", "exists": True}
                },
            }
        ]
    }

    with pytest.raises(FileNotFoundError, match="extracted directory"):
        mirror.resolve_nasr_dirs(manifest=manifest, mirror_root=root)


def test_resolve_nasr_dirs_falls_back_to_artifact_signature(tmp_path):
    root = tmp_path / "aviation_data"
    nasr_dir = root / "data/products/digital_products/current/NASR_Subscription/zip/extracted"
    csv_dir = nasr_dir / "CSV_Data/extracted"
    csv_dir.mkdir(parents=True)
    (nasr_dir / "Additional_Data").mkdir()
    (csv_dir / "APT_BASE.csv").write_text("ARPT_ID\n")
    manifest = {
        "artifacts": [
            {
                "product": "digital_products",
                "name": "NASR Subscription",
                "publication_status": "current",
                "paths": {"extracted": _path_entry(nasr_dir, root)},
            }
        ]
    }

    assert mirror.resolve_nasr_dirs(manifest=manifest, mirror_root=root) == (
        nasr_dir.resolve(),
        csv_dir.resolve(),
    )


def test_resolve_nasr_dirs_reports_available_products_when_missing(tmp_path):
    root = tmp_path / "aviation_data"
    root.mkdir()
    manifest = {
        "products": {"cifp": {}, "digital_products": {}},
        "artifacts": [
            {
                "product": "cifp",
                "name": "CIFP_260514",
                "publication_status": "current",
                "paths": {
                    "product": {"path": "data/products/cifp/current/CIFP.zip", "exists": True}
                },
            }
        ],
    }

    with pytest.raises(FileNotFoundError, match="artifact products: cifp"):
        mirror.resolve_nasr_dirs(manifest=manifest, mirror_root=root)
