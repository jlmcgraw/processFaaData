"""Tests for the pure source-chunk merging logic in faa_nasr.airspace."""

from __future__ import annotations

import numpy as np
import pytest

from faa_nasr.airspace import (
    _MergedLayer,
    _SourceChunk,
    _merge_chunks,
    _ordered_union,
    _stack_column,
)


def _chunk(
    *,
    xml_stem: str = "AIRSPACE_X",
    n_rows: int = 1,
    geometry: np.ndarray | None = None,
    fields: dict[str, list[object]] | None = None,
    fks: dict[str, list[object]] | None = None,
    geom_type: str | None = "Polygon",
    crs: object | None = None,
) -> _SourceChunk:
    """Concise factory for tests. Lists are converted to object-dtype arrays."""
    return _SourceChunk(
        xml_stem=xml_stem,
        n_rows=n_rows,
        geometry=(
            geometry
            if geometry is not None
            else np.array([b"GEOM"] * n_rows, dtype=object)
        ),
        fields={k: np.array(v, dtype=object) for k, v in (fields or {}).items()},
        fks={k: np.array(v, dtype=object) for k, v in (fks or {}).items()},
        geom_type=geom_type,
        crs=crs,
    )


# ---------------------------------------------------------------------------
# _ordered_union
# ---------------------------------------------------------------------------


def test_ordered_union_empty():
    assert _ordered_union() == []
    assert _ordered_union([], []) == []


def test_ordered_union_preserves_first_seen_order():
    assert _ordered_union(["a", "b", "c"]) == ["a", "b", "c"]


def test_ordered_union_dedupes_across_iterables():
    assert _ordered_union(["a", "b"], ["b", "c"], ["a", "d"]) == ["a", "b", "c", "d"]


def test_ordered_union_dedupes_within_one_iterable():
    assert _ordered_union(["a", "a", "b"]) == ["a", "b"]


# ---------------------------------------------------------------------------
# _stack_column
# ---------------------------------------------------------------------------


def test_stack_column_concatenates_present_data():
    chunks = [
        _chunk(n_rows=2, fields={"name": ["A", "B"]}),
        _chunk(n_rows=3, fields={"name": ["C", "D", "E"]}),
    ]
    result = _stack_column("name", chunks, "fields")
    assert list(result) == ["A", "B", "C", "D", "E"]


def test_stack_column_pads_missing_chunks_with_none():
    # First chunk has the column; second chunk doesn't.
    chunks = [
        _chunk(n_rows=2, fields={"name": ["A", "B"]}),
        _chunk(n_rows=3, fields={"other": [1, 2, 3]}),
    ]
    result = _stack_column("name", chunks, "fields")
    assert list(result) == ["A", "B", None, None, None]


def test_stack_column_works_for_fks_too():
    chunks = [
        _chunk(n_rows=1, fks={"clientAirspace": ["uuid-1"]}),
        _chunk(n_rows=2, fks={"clientAirspace": ["uuid-2", "uuid-3"]}),
    ]
    result = _stack_column("clientAirspace", chunks, "fks")
    assert list(result) == ["uuid-1", "uuid-2", "uuid-3"]


# ---------------------------------------------------------------------------
# _merge_chunks: empty / trivial cases
# ---------------------------------------------------------------------------


def test_merge_chunks_empty_returns_none():
    assert _merge_chunks([]) is None


def test_merge_chunks_single_chunk_passes_through():
    c = _chunk(
        xml_stem="ALPHA",
        n_rows=2,
        fields={"name": ["A1", "A2"]},
    )
    merged = _merge_chunks([c])
    assert merged is not None
    assert merged.fields == ["name", "_source_xml"]
    assert list(merged.field_data[0]) == ["A1", "A2"]
    assert list(merged.field_data[1]) == ["ALPHA", "ALPHA"]
    assert merged.has_geometry is True
    assert merged.geom_type == "Polygon"


# ---------------------------------------------------------------------------
# _merge_chunks: schema variation across sources
# ---------------------------------------------------------------------------


def test_merge_chunks_disjoint_field_sets_pad_with_none():
    c1 = _chunk(xml_stem="A", n_rows=1, fields={"alpha": ["x"]})
    c2 = _chunk(xml_stem="B", n_rows=1, fields={"beta": ["y"]})
    merged = _merge_chunks([c1, c2])
    assert merged is not None
    # Field order: alpha first (from chunk 1), then beta (from chunk 2),
    # then _source_xml (always last).
    assert merged.fields == ["alpha", "beta", "_source_xml"]
    assert list(merged.field_data[0]) == ["x", None]
    assert list(merged.field_data[1]) == [None, "y"]
    assert list(merged.field_data[2]) == ["A", "B"]


def test_merge_chunks_later_chunks_introducing_field_backfills_earlier():
    c1 = _chunk(xml_stem="A", n_rows=2, fields={"name": ["A1", "A2"]})
    # Second chunk has BOTH name AND a new "extra" field.
    c2 = _chunk(
        xml_stem="B",
        n_rows=1,
        fields={"name": ["B1"], "extra": ["B-extra"]},
    )
    merged = _merge_chunks([c1, c2])
    assert merged is not None
    assert merged.fields == ["name", "extra", "_source_xml"]
    assert list(merged.field_data[0]) == ["A1", "A2", "B1"]
    assert list(merged.field_data[1]) == [None, None, "B-extra"]


def test_merge_chunks_earlier_chunks_introducing_field_pads_later():
    c1 = _chunk(
        xml_stem="A",
        n_rows=1,
        fields={"name": ["A1"], "extra": ["A-extra"]},
    )
    c2 = _chunk(xml_stem="B", n_rows=2, fields={"name": ["B1", "B2"]})
    merged = _merge_chunks([c1, c2])
    assert merged is not None
    assert merged.fields == ["name", "extra", "_source_xml"]
    assert list(merged.field_data[0]) == ["A1", "B1", "B2"]
    assert list(merged.field_data[1]) == ["A-extra", None, None]


# ---------------------------------------------------------------------------
# _merge_chunks: FK columns
# ---------------------------------------------------------------------------


def test_merge_chunks_fk_columns_appear_after_fields():
    c = _chunk(
        xml_stem="A",
        n_rows=1,
        fields={"name": ["A1"]},
        fks={"clientAirspace": ["uuid-1"]},
    )
    merged = _merge_chunks([c])
    assert merged is not None
    assert merged.fields == ["name", "clientAirspace", "_source_xml"]


def test_merge_chunks_fk_union_across_chunks_with_pads():
    c1 = _chunk(xml_stem="A", n_rows=1, fks={"clientAirspace": ["a"]})
    c2 = _chunk(xml_stem="B", n_rows=1, fks={"serviceProvider": ["b"]})
    merged = _merge_chunks([c1, c2])
    assert merged is not None
    assert merged.fields == ["clientAirspace", "serviceProvider", "_source_xml"]
    assert list(merged.field_data[0]) == ["a", None]
    assert list(merged.field_data[1]) == [None, "b"]


# ---------------------------------------------------------------------------
# _merge_chunks: geometry handling
# ---------------------------------------------------------------------------


def test_merge_chunks_attribute_only_layer_has_geometry_false():
    # No chunk declares a geom_type and all geometries are None.
    c1 = _chunk(
        xml_stem="A",
        n_rows=1,
        geometry=np.array([None], dtype=object),
        geom_type=None,
    )
    c2 = _chunk(
        xml_stem="B",
        n_rows=1,
        geometry=np.array([None], dtype=object),
        geom_type=None,
    )
    merged = _merge_chunks([c1, c2])
    assert merged is not None
    assert merged.has_geometry is False
    assert merged.geom_type is None


def test_merge_chunks_first_non_null_geom_type_wins():
    c1 = _chunk(xml_stem="A", n_rows=1, geom_type=None)
    c2 = _chunk(xml_stem="B", n_rows=1, geom_type="Polygon")
    c3 = _chunk(xml_stem="C", n_rows=1, geom_type="LineString")
    merged = _merge_chunks([c1, c2, c3])
    assert merged is not None
    assert merged.geom_type == "Polygon"


def test_merge_chunks_first_non_null_crs_wins():
    c1 = _chunk(xml_stem="A", crs=None, n_rows=1)
    c2 = _chunk(xml_stem="B", crs="EPSG:4326", n_rows=1)
    merged = _merge_chunks([c1, c2])
    assert merged is not None
    assert merged.crs == "EPSG:4326"


# ---------------------------------------------------------------------------
# _merge_chunks: source_xml propagation
# ---------------------------------------------------------------------------


def test_merge_chunks_source_xml_repeats_per_row():
    c1 = _chunk(xml_stem="ALPHA", n_rows=3)
    c2 = _chunk(xml_stem="BETA", n_rows=2)
    merged = _merge_chunks([c1, c2])
    assert merged is not None
    src_idx = merged.fields.index("_source_xml")
    assert list(merged.field_data[src_idx]) == ["ALPHA", "ALPHA", "ALPHA", "BETA", "BETA"]


# ---------------------------------------------------------------------------
# _merge_chunks: returned shape invariants
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "chunks",
    [
        [_chunk(n_rows=1)],
        [_chunk(n_rows=2), _chunk(n_rows=3)],
        [
            _chunk(xml_stem="A", n_rows=1, fields={"x": [1]}),
            _chunk(xml_stem="B", n_rows=1, fields={"y": [2]}),
            _chunk(xml_stem="C", n_rows=1, fks={"rel": ["uuid"]}),
        ],
    ],
)
def test_merge_chunks_returns_parallel_arrays(chunks):
    """All field_data arrays must be the same length, equal to total rows
    across chunks, and equal to len(geometry)."""
    merged = _merge_chunks(chunks)
    assert merged is not None
    expected_n = sum(c.n_rows for c in chunks)
    assert len(merged.geometry) == expected_n
    for arr in merged.field_data:
        assert len(arr) == expected_n
    # field_data should be parallel to fields
    assert len(merged.fields) == len(merged.field_data)


def test_merge_chunks_result_is_a_merged_layer_dataclass():
    merged = _merge_chunks([_chunk(n_rows=1)])
    assert isinstance(merged, _MergedLayer)
