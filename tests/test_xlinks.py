"""Tests for the AIXM XLink extraction helpers in faa_nasr.airspace."""

from __future__ import annotations

import xml.etree.ElementTree as ET

import pytest

from faa_nasr.airspace import (
    _build_gml_to_uuid_map,
    _local_name,
    _resolve_xlinks,
)

# Namespace declarations matching the FAA's SAA AIXM bundle. The real files
# have many more; these are the only ones that affect the helpers' logic.
_NS = (
    'xmlns="http://www.aixm.aero/schema/5.0" '
    'xmlns:gml="http://www.opengis.net/gml/3.2" '
    'xmlns:xlink="http://www.w3.org/1999/xlink"'
)


def _parse(xml: str) -> ET.Element:
    return ET.fromstring(xml)


# ---------------------------------------------------------------------------
# _local_name
# ---------------------------------------------------------------------------


def test_local_name_strips_namespace():
    elem = _parse(f'<Airspace {_NS}/>')
    assert _local_name(elem) == "Airspace"


def test_local_name_handles_unprefixed_tag():
    elem = ET.fromstring("<bare/>")
    assert _local_name(elem) == "bare"


# ---------------------------------------------------------------------------
# _build_gml_to_uuid_map
# ---------------------------------------------------------------------------


def test_build_gml_to_uuid_map_extracts_features():
    root = _parse(f"""
        <hasMember {_NS}>
          <Airspace gml:id="Airspace1">
            <identifier>uuid-airspace-1</identifier>
          </Airspace>
          <Unit gml:id="Unit1">
            <identifier>uuid-unit-1</identifier>
          </Unit>
        </hasMember>
    """)
    assert _build_gml_to_uuid_map(root) == {
        "Airspace1": "uuid-airspace-1",
        "Unit1": "uuid-unit-1",
    }


def test_build_gml_to_uuid_map_ignores_non_feature_elements():
    # GML-internal elements like <Surface> have gml:id but aren't AIXM features.
    root = _parse(f"""
        <hasMember {_NS}>
          <Airspace gml:id="Airspace1">
            <identifier>uuid-airspace-1</identifier>
            <extent><Surface gml:id="Surface1"/></extent>
          </Airspace>
        </hasMember>
    """)
    result = _build_gml_to_uuid_map(root)
    assert "Surface1" not in result
    assert result == {"Airspace1": "uuid-airspace-1"}


def test_build_gml_to_uuid_map_skips_features_without_identifier():
    root = _parse(f"""
        <hasMember {_NS}>
          <Airspace gml:id="Airspace1"/>
        </hasMember>
    """)
    assert _build_gml_to_uuid_map(root) == {}


def test_build_gml_to_uuid_map_strips_whitespace_around_uuid():
    root = _parse(f"""
        <hasMember {_NS}>
          <Airspace gml:id="Airspace1">
            <identifier>
              uuid-with-whitespace
            </identifier>
          </Airspace>
        </hasMember>
    """)
    assert _build_gml_to_uuid_map(root) == {"Airspace1": "uuid-with-whitespace"}


# ---------------------------------------------------------------------------
# _resolve_xlinks
# ---------------------------------------------------------------------------


def test_resolve_xlinks_resolves_simple_reference():
    root = _parse(f"""
        <hasMember {_NS}>
          <Airspace gml:id="Airspace1">
            <identifier>uuid-airspace-1</identifier>
          </Airspace>
          <AirspaceUsage gml:id="AirspaceUsage1">
            <identifier>uuid-usage-1</identifier>
            <restrictedAirspace xlink:href="#Airspace1"/>
          </AirspaceUsage>
        </hasMember>
    """)
    gml_to_uuid = _build_gml_to_uuid_map(root)
    fks = _resolve_xlinks(root, gml_to_uuid)
    assert fks[("AirspaceUsage", "AirspaceUsage1")] == {
        "restrictedAirspace": "uuid-airspace-1",
    }


def test_resolve_xlinks_records_multiple_relationships_per_feature():
    # AirTrafficControlService points to both clientAirspace and serviceProvider.
    root = _parse(f"""
        <hasMember {_NS}>
          <Airspace gml:id="Airspace1"><identifier>uuid-airspace</identifier></Airspace>
          <Unit gml:id="Unit1"><identifier>uuid-unit</identifier></Unit>
          <AirTrafficControlService gml:id="ATC1">
            <identifier>uuid-atc</identifier>
            <clientAirspace xlink:href="#Airspace1"/>
            <serviceProvider xlink:href="#Unit1"/>
          </AirTrafficControlService>
        </hasMember>
    """)
    gml_to_uuid = _build_gml_to_uuid_map(root)
    fks = _resolve_xlinks(root, gml_to_uuid)
    assert fks[("AirTrafficControlService", "ATC1")] == {
        "clientAirspace": "uuid-airspace",
        "serviceProvider": "uuid-unit",
    }


def test_resolve_xlinks_attributes_to_topmost_feature_ancestor():
    # An xlink nested inside a TimeSlice should still be attributed to the
    # enclosing top-level feature (Unit), not to TimeSlice.
    root = _parse(f"""
        <hasMember {_NS}>
          <OrganisationAuthority gml:id="Org1"><identifier>uuid-org</identifier></OrganisationAuthority>
          <Unit gml:id="Unit1">
            <identifier>uuid-unit</identifier>
            <timeSlice>
              <UnitTimeSlice>
                <ownerOrganisation xlink:href="#Org1"/>
              </UnitTimeSlice>
            </timeSlice>
          </Unit>
        </hasMember>
    """)
    gml_to_uuid = _build_gml_to_uuid_map(root)
    fks = _resolve_xlinks(root, gml_to_uuid)
    assert fks == {
        ("Unit", "Unit1"): {"ownerOrganisation": "uuid-org"},
    }


def test_resolve_xlinks_drops_dangling_references():
    # If the href target doesn't appear in gml_to_uuid, the FK is silently
    # skipped -- we only record relationships we can actually resolve.
    root = _parse(f"""
        <hasMember {_NS}>
          <Unit gml:id="Unit1">
            <identifier>uuid-unit</identifier>
            <ownerOrganisation xlink:href="#OrgThatDoesntExist"/>
          </Unit>
        </hasMember>
    """)
    fks = _resolve_xlinks(root, {})
    assert fks == {}


def test_resolve_xlinks_ignores_xlinks_outside_features():
    # xlinks at the document level (not inside any AIXM feature) are skipped.
    root = _parse(f"""
        <root {_NS}>
          <Airspace gml:id="Airspace1"><identifier>uuid-airspace</identifier></Airspace>
          <somethingElse xlink:href="#Airspace1"/>
        </root>
    """)
    gml_to_uuid = _build_gml_to_uuid_map(root)
    fks = _resolve_xlinks(root, gml_to_uuid)
    assert fks == {}


@pytest.mark.parametrize(
    "href",
    [
        "#Airspace1",  # canonical form
        "Airspace1",  # missing leading hash; lstrip('#') is a no-op
    ],
)
def test_resolve_xlinks_strips_optional_leading_hash(href):
    root = _parse(f"""
        <hasMember {_NS}>
          <Airspace gml:id="Airspace1"><identifier>uuid-airspace</identifier></Airspace>
          <AirspaceUsage gml:id="AirspaceUsage1">
            <identifier>uuid-usage</identifier>
            <restrictedAirspace xlink:href="{href}"/>
          </AirspaceUsage>
        </hasMember>
    """)
    gml_to_uuid = _build_gml_to_uuid_map(root)
    fks = _resolve_xlinks(root, gml_to_uuid)
    assert fks[("AirspaceUsage", "AirspaceUsage1")] == {
        "restrictedAirspace": "uuid-airspace",
    }
