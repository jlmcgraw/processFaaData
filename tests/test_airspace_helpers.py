"""Tests for the pure helpers in faa_nasr.airspace."""

from __future__ import annotations

import pytest

from faa_nasr.airspace import _promote_geom_type, _safe_name


class TestSafeName:
    def test_passes_through_valid_identifier(self):
        assert _safe_name("Class_Airspace") == "Class_Airspace"

    def test_replaces_apostrophe(self):
        # Real-world case that broke SpatiaLite's geometry_columns constraint.
        assert _safe_name("O'NEILL MOA, NE_Airspace") == "O_NEILL_MOA_NE_Airspace"

    def test_replaces_spaces_and_commas(self):
        assert _safe_name("ADA EAST MOA, KS") == "ADA_EAST_MOA_KS"

    def test_collapses_runs_of_unsafe_chars(self):
        # Punctuation cluster shouldn't produce a run of underscores.
        assert _safe_name("R-3401A!!! ATTERBURY, IN") == "R_3401A_ATTERBURY_IN"

    def test_strips_leading_and_trailing_underscores(self):
        assert _safe_name("___foo___") == "foo"
        assert _safe_name("--bar--") == "bar"

    def test_empty_input_returns_placeholder(self):
        assert _safe_name("") == "layer"
        assert _safe_name("!!!") == "layer"


class TestPromoteGeomType:
    @pytest.mark.parametrize(
        "input_type,expected",
        [
            ("Polygon", "MultiPolygon"),
            ("LineString", "MultiLineString"),
            ("Point", "MultiPoint"),
            ("Polygon Z", "MultiPolygon Z"),
            ("LineString ZM", "MultiLineString ZM"),
        ],
    )
    def test_promotes_singletons_to_multi(self, input_type, expected):
        assert _promote_geom_type(input_type) == expected

    @pytest.mark.parametrize(
        "already_multi",
        ["MultiPolygon", "MultiLineString", "MultiPoint", "MultiPolygon Z"],
    )
    def test_does_not_double_promote(self, already_multi):
        assert _promote_geom_type(already_multi) == already_multi

    def test_passes_through_unknown_types(self):
        # Non-singleton, non-multi (e.g. GeometryCollection) is left untouched.
        assert _promote_geom_type("GeometryCollection") == "GeometryCollection"

    def test_preserves_none(self):
        assert _promote_geom_type(None) is None

    def test_preserves_empty_string(self):
        assert _promote_geom_type("") == ""
