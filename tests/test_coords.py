import pytest

from faa_nasr.coords import dms_to_decimal, parse_dms


def test_dms_to_decimal_north():
    assert dms_to_decimal(36, 4, 0, "N") == pytest.approx(36.0666666666)


def test_dms_to_decimal_south_is_negative():
    assert dms_to_decimal(36, 4, 0, "S") == pytest.approx(-36.0666666666)


def test_dms_to_decimal_east():
    assert dms_to_decimal(120, 30, 30, "E") == pytest.approx(120.5083333333)


def test_dms_to_decimal_west_is_negative():
    assert dms_to_decimal(120, 30, 30, "W") == pytest.approx(-120.5083333333)


def test_dms_to_decimal_zero():
    assert dms_to_decimal(0, 0, 0, "N") == 0.0


def test_dms_to_decimal_invalid_hemisphere():
    with pytest.raises(ValueError, match="invalid hemisphere"):
        dms_to_decimal(36, 0, 0, "X")


def test_dms_to_decimal_latitude_out_of_range():
    with pytest.raises(ValueError, match="out of range"):
        dms_to_decimal(91, 0, 0, "N")


def test_dms_to_decimal_longitude_out_of_range():
    with pytest.raises(ValueError, match="out of range"):
        dms_to_decimal(181, 0, 0, "E")


def test_parse_dms_with_dashes():
    # Format used by legacy fixed-width NASR records.
    assert parse_dms("36-04-00.5N") == pytest.approx(36.06680555)


def test_parse_dms_with_decimal_seconds():
    assert parse_dms("47-37-23.123N") == pytest.approx(47.62308972)


def test_parse_dms_invalid_format():
    with pytest.raises(ValueError, match="cannot parse"):
        parse_dms("not a coordinate")
