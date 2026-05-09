"""Tests for the pure helpers in faa_nasr.tables."""

from __future__ import annotations

from faa_nasr.tables import _safe_col


def test_passes_through_valid_column():
    assert _safe_col("LAT_DECIMAL") == "LAT_DECIMAL"


def test_strips_surrounding_whitespace():
    assert _safe_col("  ARPT_ID  ") == "ARPT_ID"


def test_replaces_internal_spaces_with_underscores():
    # FAA DOF.CSV has columns like "VERIFIED STATUS" and "FAA STUDY".
    assert _safe_col("VERIFIED STATUS") == "VERIFIED_STATUS"
    assert _safe_col("FAA STUDY") == "FAA_STUDY"


def test_empty_input_returns_placeholder():
    assert _safe_col("") == "_"
    assert _safe_col("   ") == "_"
