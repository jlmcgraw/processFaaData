"""DMS↔decimal coordinate helpers.

The NASR CSV subscription pre-decimalizes coordinates as `LAT_DECIMAL` /
`LONG_DECIMAL`, so this module is rarely needed in the main pipeline. It exists
as a fallback when those columns are missing or for ad-hoc DMS conversion.
"""

from __future__ import annotations

import re

_DMS_RE = re.compile(
    r"""^\s*
        (?P<deg>\d+) [-\s] (?P<min>\d+) [-\s] (?P<sec>[\d.]+)
        \s*(?P<hemis>[NSEW])\s*$""",
    re.VERBOSE | re.IGNORECASE,
)


def dms_to_decimal(deg: float, minutes: float, seconds: float, hemis: str) -> float:
    """Convert degrees/minutes/seconds + hemisphere to signed decimal degrees.

    Raises ValueError on invalid hemisphere or out-of-range result.
    """
    h = hemis.upper()
    if h not in ("N", "S", "E", "W"):
        raise ValueError(f"invalid hemisphere {hemis!r}")
    value = float(deg) + float(minutes) / 60 + float(seconds) / 3600
    if h in ("S", "W"):
        value = -value
    if h in ("N", "S") and abs(value) > 90:
        raise ValueError(f"latitude {value} out of range")
    if h in ("E", "W") and abs(value) > 180:
        raise ValueError(f"longitude {value} out of range")
    return value


def parse_dms(coordinate: str) -> float:
    """Parse a NASR-style DMS string like '36-04-00.5N' to signed decimal degrees."""
    m = _DMS_RE.match(coordinate)
    if not m:
        raise ValueError(f"cannot parse coordinate {coordinate!r}")
    return dms_to_decimal(float(m["deg"]), float(m["min"]), float(m["sec"]), m["hemis"])
