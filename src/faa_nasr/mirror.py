"""Resolve aviation-data-mirror artifacts into local inputs for the NASR builders."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

NASR_PRODUCT_KEYS = (
    "nasr",
    "nasr_csv",
    "nasr_subscription",
    "28_day_nasr",
    "28_day_sub",
    "28_day_subscriber",
    "nfdc_nasr",
    "subscriber",
)
OBSTACLE_PRODUCT_KEYS = ("daily_dof", "dof")


@dataclass(frozen=True)
class MirrorInputs:
    nasr_dir: Path
    csv_dir: Path
    obstacle_csv: Path | None
    cifp_file: Path | None
    edai_dir: Path | None


def resolve_inputs(
    mirror_root: Path,
    manifest_path: Path | None = None,
    *,
    include_cifp: bool = True,
    include_edai: bool = True,
) -> MirrorInputs:
    """Resolve the downloaded aviation-data-mirror artifacts used by the full build."""
    manifest = load_manifest(mirror_root=mirror_root, manifest_path=manifest_path)
    nasr_dir, csv_dir = resolve_nasr_dirs(manifest=manifest, mirror_root=mirror_root)
    return MirrorInputs(
        nasr_dir=nasr_dir,
        csv_dir=csv_dir,
        obstacle_csv=resolve_obstacle_csv(manifest=manifest, mirror_root=mirror_root),
        cifp_file=resolve_cifp_file(manifest=manifest, mirror_root=mirror_root)
        if include_cifp
        else None,
        edai_dir=resolve_edai_dir(manifest=manifest, mirror_root=mirror_root)
        if include_edai
        else None,
    )


def load_manifest(mirror_root: Path, manifest_path: Path | None = None) -> dict[str, Any]:
    path = manifest_path or mirror_root / "manifest.json"
    if not path.is_file():
        raise FileNotFoundError(
            f"aviation-data-mirror manifest not found at {path}. "
            "Run aviation-data-mirror first and generate aviation_data/manifest.json."
        )
    return json.loads(path.read_text())


def resolve_nasr_dirs(
    *,
    manifest: dict[str, Any],
    mirror_root: Path,
) -> tuple[Path, Path]:
    artifact = _select_artifact(manifest, NASR_PRODUCT_KEYS)
    if artifact is None:
        artifact = _find_nasr_artifact_by_signature(manifest)
    if artifact is None:
        discovered = _find_nasr_dir_by_signature(mirror_root)
        if discovered is not None:
            return discovered, _find_nasr_csv_dir(discovered)
        raise FileNotFoundError(_missing_nasr_message(manifest))

    nasr_dir = _artifact_path(artifact, "extracted", mirror_root)
    if nasr_dir is None or not nasr_dir.is_dir():
        raise FileNotFoundError(
            "The NASR artifact does not have an extracted directory. "
            "Refresh aviation-data-mirror with archive extraction enabled."
        )

    csv_dir = _find_nasr_csv_dir(nasr_dir)
    return nasr_dir, csv_dir


def resolve_obstacle_csv(
    *,
    manifest: dict[str, Any],
    mirror_root: Path,
) -> Path | None:
    artifact = _select_obstacle_artifact(manifest)
    if artifact is None:
        return None

    extracted = _artifact_path(artifact, "extracted", mirror_root)
    if extracted is not None and extracted.is_dir():
        found = _find_file(extracted, ("DOF*.CSV", "DOF*.csv"))
        if found is not None:
            return found

    product = _artifact_path(artifact, "product", mirror_root)
    if product is not None and product.is_file() and product.suffix.lower() == ".csv":
        return product

    raise FileNotFoundError(
        "The DOF artifact is present in the aviation-data-mirror manifest, "
        "but no DOF CSV was found. "
        "Refresh aviation-data-mirror with archive extraction enabled."
    )


def resolve_cifp_file(
    *,
    manifest: dict[str, Any],
    mirror_root: Path,
) -> Path | None:
    artifact = _select_cifp_artifact(manifest)
    if artifact is None:
        return None

    extracted = _artifact_path(artifact, "extracted", mirror_root)
    if extracted is not None and extracted.is_dir():
        found = _find_file(extracted, ("FAACIFP18",))
        if found is not None:
            return found

    product = _artifact_path(artifact, "product", mirror_root)
    if product is not None and product.is_file() and product.name == "FAACIFP18":
        return product

    raise FileNotFoundError(
        "The CIFP artifact is present in the aviation-data-mirror manifest, "
        "but FAACIFP18 was not found. "
        "Refresh aviation-data-mirror with archive extraction enabled."
    )


def resolve_edai_dir(*, manifest: dict[str, Any], mirror_root: Path) -> Path | None:
    artifacts = _candidate_artifacts(manifest, ("edai",))
    if not artifacts:
        return None

    selected = _preferred_status_group(artifacts)
    extracted_dirs = [
        path
        for artifact in selected
        if (path := _artifact_path(artifact, "extracted", mirror_root)) is not None
        and path.is_dir()
    ]
    if not extracted_dirs:
        raise FileNotFoundError(
            "EDAI artifacts are present in the aviation-data-mirror manifest, but no extracted "
            "shapefile directories were found. "
            "Refresh aviation-data-mirror with archive extraction enabled."
        )

    if len(extracted_dirs) == 1:
        return extracted_dirs[0]
    return Path(os.path.commonpath([str(path) for path in extracted_dirs]))


def _select_artifact(
    manifest: dict[str, Any], product_keys: tuple[str, ...]
) -> dict[str, Any] | None:
    artifacts = _candidate_artifacts(manifest, product_keys)
    if not artifacts:
        return None
    selected = _preferred_status_group(artifacts)
    return max(
        selected,
        key=lambda artifact: (
            str(artifact.get("cycle") or ""),
            str(artifact.get("download", {}).get("downloaded_at") or ""),
            str(artifact.get("name") or ""),
        ),
    )


def _find_nasr_artifact_by_signature(manifest: dict[str, Any]) -> dict[str, Any] | None:
    artifacts = [
        artifact
        for artifact in manifest.get("artifacts", [])
        if _looks_downloaded(artifact) and _artifact_mentions_nasr(artifact)
    ]
    if not artifacts:
        return None
    selected = _preferred_status_group(artifacts)
    return max(
        selected,
        key=lambda artifact: (
            str(artifact.get("cycle") or ""),
            str(artifact.get("download", {}).get("downloaded_at") or ""),
            str(artifact.get("name") or ""),
        ),
    )


def _select_obstacle_artifact(manifest: dict[str, Any]) -> dict[str, Any] | None:
    artifacts = _candidate_artifacts(manifest, OBSTACLE_PRODUCT_KEYS)
    if not artifacts:
        return None
    csv_artifacts = [artifact for artifact in artifacts if _artifact_mentions_csv(artifact)]
    if csv_artifacts:
        artifacts = csv_artifacts
    selected = _preferred_status_group(artifacts)
    return max(
        selected,
        key=lambda artifact: (
            _artifact_mentions_csv(artifact),
            artifact.get("product") == "daily_dof",
            str(artifact.get("download", {}).get("downloaded_at") or ""),
            str(artifact.get("name") or ""),
        ),
    )


def _select_cifp_artifact(manifest: dict[str, Any]) -> dict[str, Any] | None:
    artifacts = _candidate_artifacts(manifest, ("cifp",))
    if not artifacts:
        return None
    selected = _preferred_status_group(artifacts)
    return max(
        selected,
        key=lambda artifact: (
            _artifact_mentions_cifp_zip(artifact),
            str(artifact.get("cycle") or ""),
            str(artifact.get("download", {}).get("downloaded_at") or ""),
        ),
    )


def _artifact_mentions_csv(artifact: dict[str, Any]) -> bool:
    fields = [
        artifact.get("name"),
        artifact.get("format"),
        artifact.get("faa_filename"),
        *_artifact_path_texts(artifact),
    ]
    text = " ".join(str(field or "") for field in fields).lower()
    return "csv" in text


def _artifact_mentions_cifp_zip(artifact: dict[str, Any]) -> bool:
    fields = [
        artifact.get("name"),
        artifact.get("format"),
        artifact.get("faa_filename"),
        *_artifact_path_texts(artifact),
    ]
    text = " ".join(str(field or "") for field in fields).lower()
    return "cifp" in text and "zip" in text


def _artifact_path_texts(artifact: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for entry in (artifact.get("paths") or {}).values():
        if isinstance(entry, str):
            out.append(entry)
        elif isinstance(entry, dict):
            for key in ("path", "relative_path", "resolved_path"):
                value = entry.get(key)
                if isinstance(value, str):
                    out.append(value)
    return out


def _artifact_mentions_nasr(artifact: dict[str, Any]) -> bool:
    fields = [
        artifact.get("name"),
        artifact.get("format"),
        artifact.get("faa_filename"),
        artifact.get("product"),
        artifact.get("source", {}).get("url"),
        artifact.get("source", {}).get("page_url"),
        artifact.get("metadata", {}).get("section"),
        artifact.get("metadata", {}).get("context"),
        artifact.get("metadata", {}).get("apra", {}).get("product", {}).get("productName"),
    ]
    text = " ".join(str(field or "") for field in fields).lower()
    return (
        "nasr" in text
        or "subscriber" in text
        or "28daysub" in text
        or "28daysubscription" in text
        or "csv_data" in text
    )


def _candidate_artifacts(
    manifest: dict[str, Any],
    product_keys: tuple[str, ...],
) -> list[dict[str, Any]]:
    product_set = set(product_keys)
    return [
        artifact
        for artifact in manifest.get("artifacts", [])
        if artifact.get("product") in product_set and _looks_downloaded(artifact)
    ]


def _preferred_status_group(artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rank = {
        "current": 0,
        "latest": 1,
        "unscheduled": 2,
        "unknown": 3,
        "next": 4,
    }
    best_rank = min(rank.get(str(a.get("publication_status") or ""), 99) for a in artifacts)
    return [
        a for a in artifacts if rank.get(str(a.get("publication_status") or ""), 99) == best_rank
    ]


def _looks_downloaded(artifact: dict[str, Any]) -> bool:
    paths = artifact.get("paths") or {}
    for key in ("product", "extracted", "blob"):
        entry = paths.get(key)
        if isinstance(entry, dict) and entry.get("exists") is False:
            continue
        if _entry_path(entry) is not None:
            return True
    return False


def _artifact_path(artifact: dict[str, Any], key: str, mirror_root: Path) -> Path | None:
    path_text = _entry_path((artifact.get("paths") or {}).get(key), prefer_relative=True)
    if path_text is None:
        return None
    path = Path(path_text)
    if not path.is_absolute():
        path = mirror_root / path
    return path.absolute()


def _entry_path(entry: Any, *, prefer_relative: bool = False) -> str | None:
    if isinstance(entry, str):
        return entry
    if isinstance(entry, dict):
        if prefer_relative:
            relative_path = entry.get("relative_path")
            if isinstance(relative_path, str) and relative_path:
                return relative_path
        path = entry.get("path")
        if isinstance(path, str) and path:
            return path
    return None



def _find_nasr_dir_by_signature(mirror_root: Path) -> Path | None:
    products_dir = mirror_root / "data" / "products"
    if not products_dir.is_dir():
        return None
    for candidate in products_dir.rglob("*"):
        if not candidate.is_dir():
            continue
        if (candidate / "CSV_Data").is_dir() and (candidate / "Additional_Data").is_dir():
            return candidate.resolve()
    return None


def _find_nasr_csv_dir(nasr_dir: Path) -> Path:
    preferred = nasr_dir / "CSV_Data" / "extracted"
    if preferred.is_dir() and _contains_csv(preferred):
        return preferred

    for candidate in [nasr_dir, *[p for p in nasr_dir.rglob("*") if p.is_dir()]]:
        if _contains_nasr_csv(candidate):
            return candidate

    csv_data = nasr_dir / "CSV_Data"
    if csv_data.is_dir() and any(csv_data.glob("*_CSV.zip")):
        raise FileNotFoundError(
            f"NASR CSV bundle under {csv_data} has not been extracted. "
            "Refresh aviation-data-mirror with archive extraction enabled."
        )
    raise FileNotFoundError(f"No extracted NASR CSV directory found under {nasr_dir}")


def _contains_nasr_csv(path: Path) -> bool:
    expected = {"APT_BASE.csv", "NAV_BASE.csv", "FIX_BASE.csv"}
    names = {child.name for child in path.iterdir() if child.is_file()}
    return bool(expected & names) or _contains_csv(path)


def _contains_csv(path: Path) -> bool:
    return any(child.is_file() and child.suffix.lower() == ".csv" for child in path.iterdir())


def _find_file(root: Path, patterns: tuple[str, ...]) -> Path | None:
    for pattern in patterns:
        found = next(root.rglob(pattern), None)
        if found is not None and found.is_file():
            return found
    return None


def _missing_nasr_message(manifest: dict[str, Any]) -> str:
    artifact_products = sorted(
        {
            str(artifact.get("product"))
            for artifact in manifest.get("artifacts", [])
            if artifact.get("product")
        }
    )
    configured_products = manifest.get("products", {})
    configured = sorted(configured_products) if isinstance(configured_products, dict) else []
    return (
        "No NASR subscription artifact was found in the aviation-data-mirror manifest or "
        "under data/products. The current mirror appears to contain artifact "
        f"products: {', '.join(artifact_products) or '(none)'}. "
        f"Configured products: {', '.join(configured) or '(unknown)'}. "
        "Run or configure aviation-data-mirror so it downloads the NASR subscriber ZIP "
        "with an extracted CSV_Data directory, or pass both --nasr-dir and "
        "--csv-dir to nasr build."
    )
