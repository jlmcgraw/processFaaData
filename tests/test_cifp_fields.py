"""Tests that all CIFP field specs sum to exactly 132 characters."""

from __future__ import annotations

import pytest

from faa_nasr.cifp_records import (
    CONTINUATION_APP_SPECS,
    CONTINUATION_BASE_SPECS,
    PRIMARY_SPECS,
    crn_pos,
    resolve_dups,
)


@pytest.mark.parametrize("key", list(PRIMARY_SPECS))
def test_primary_spec_width(key: tuple[str, str]) -> None:
    total = sum(w for _, w in PRIMARY_SPECS[key])
    assert total == 132, f"PRIMARY {key}: sum={total}"


@pytest.mark.parametrize("key", list(CONTINUATION_BASE_SPECS))
def test_continuation_base_spec_width(key: tuple[str, str]) -> None:
    total = sum(w for _, w in CONTINUATION_BASE_SPECS[key])
    assert total == 132, f"CONTINUATION_BASE {key}: sum={total}"


@pytest.mark.parametrize("key", list(CONTINUATION_APP_SPECS))
def test_continuation_app_spec_width(key: tuple[str, str, str]) -> None:
    total = sum(w for _, w in CONTINUATION_APP_SPECS[key])
    assert total == 132, f"CONTINUATION_APP {key}: sum={total}"


def test_resolve_dups_no_dups() -> None:
    fields = [("RecordType", 1), ("SectionCode", 1), ("FileRecordNumber", 5)]
    result = resolve_dups(fields)
    assert result == fields


def test_resolve_dups_with_dups() -> None:
    fields = [("MORA", 3), ("MORA", 3), ("MORA", 3)]
    result = resolve_dups(fields)
    assert result == [("MORA_1", 3), ("MORA_2", 3), ("MORA_3", 3)]


def test_resolve_dups_mixed() -> None:
    fields = [("A", 1), ("B", 2), ("A", 1), ("C", 3), ("B", 2)]
    result = resolve_dups(fields)
    assert result == [("A_1", 1), ("B_1", 2), ("A_2", 1), ("C", 3), ("B_2", 2)]


def test_primary_specs_have_section_names() -> None:
    from faa_nasr.cifp_records import SECTION_NAMES

    for key in PRIMARY_SPECS:
        assert key in SECTION_NAMES, f"No SECTION_NAMES entry for PRIMARY_SPECS key {key}"


@pytest.mark.parametrize(
    "sc,ssc,expected",
    [
        ("H", "F", 38),
        ("P", "F", 38),
        ("P", "P", 26),
        ("U", "R", 24),
    ],
)
def test_crn_pos(sc: str, ssc: str, expected: int) -> None:
    assert crn_pos(sc, ssc) == expected
