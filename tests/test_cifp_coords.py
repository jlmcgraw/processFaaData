"""Tests for CIFP coordinate parser."""

from __future__ import annotations

import pytest

from faa_nasr.cifp import parse_cifp_coord


@pytest.mark.parametrize(
    "raw, expected, places",
    [
        # 9-char lat (HDDMMSSFF)
        ("N51521587", 51.871, 3),
        ("N59565600", 59.949, 3),
        ("N61364939", 61.614, 3),
        # 10-char lon (HDDDMMSSFF)
        ("W176402739", -176.674, 3),
        ("W151413200", -151.692, 3),
        # 11-char high-precision lat (HDDMMSSFFFF): N,30,28,42,2400 → 30.478...
        ("N3028422400", 30.478, 3),
        # 3-char lat (deg only)
        ("N30", 30.0, 6),
        # 4-char lon (deg only)
        ("W081", -81.0, 6),
    ],
)
def test_parse_cifp_coord_valid(raw: str, expected: float, places: int) -> None:
    result = parse_cifp_coord(raw)
    assert result is not None
    assert round(result, places) == round(expected, places)


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        None,
        "X51521587",
        "Z30",
    ],
)
def test_parse_cifp_coord_invalid(raw: str | None) -> None:
    assert parse_cifp_coord(raw) is None  # type: ignore[arg-type]
