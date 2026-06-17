"""Parse FAA CIFP (ARINC 424 format) files into SQLite."""

from __future__ import annotations

import math
import re
import sqlite3
from itertools import groupby
from pathlib import Path

from tqdm import tqdm

from faa_nasr import _log
from faa_nasr.cifp_records import (
    CONTINUATION_APP_SPECS,
    CONTINUATION_BASE_SPECS,
    PRIMARY_SPECS,
    SECTION_NAMES,
    crn_pos,
    resolve_dups,
)

_RECORD_LEN = 132

# ---------------------------------------------------------------------------
# Spatial: procedure path geometry
# ---------------------------------------------------------------------------

_SPATIALITE_CANDIDATES = [
    "/usr/lib/aarch64-linux-gnu/mod_spatialite.so",
    "/usr/lib/x86_64-linux-gnu/mod_spatialite.so",
    "/usr/local/lib/mod_spatialite.dylib",
    "/opt/homebrew/lib/mod_spatialite.dylib",
    "mod_spatialite",
]

# (sc, ssc, procedure_type_label, identifier_column_in_table)
# _PDEF uses SIDSTARApproachIdentifier; _HDEF uses SIDSTARAPPIdentifier
_PROC_SECTIONS: list[tuple[str, str, str, str]] = [
    ("P", "F", "IAP", "SIDSTARApproachIdentifier"),
    ("P", "D", "SID", "SIDSTARApproachIdentifier"),
    ("P", "E", "STAR", "SIDSTARApproachIdentifier"),
    ("H", "F", "IAP", "SIDSTARAPPIdentifier"),
    ("H", "D", "SID", "SIDSTARAPPIdentifier"),
    ("H", "E", "STAR", "SIDSTARAPPIdentifier"),
]

# Each row: (fix_sc, fix_ssc, lookup_table_name, alias,
#            extra_join_conditions, lon_expr, lat_expr)
# Legs whose fix section/subsection doesn't match any entry (CA, CI, VA …)
# simply produce a NULL coordinate and are skipped in the output.
_FIX_LOOKUPS: list[tuple[str, str, str, str, list[str], str, str]] = [
    (
        "P",
        "C",
        "primary_P_C_base_Airport - Terminal Waypoints",
        "tc",
        [
            "p.LandingFacilityIcaoIdentifier = tc.RegionCode",
            "p.FixIdentifier = tc.WaypointIdentifier",
            "p.FixIcaoRegionCode = tc.WaypointIcaoRegionCode",
        ],
        "tc.WaypointLongitude_WGS84",
        "tc.WaypointLatitude_WGS84",
    ),
    (
        "E",
        "A",
        "primary_E_A_base_Enroute - Grid Waypoints",
        "grid",
        [
            "p.FixIdentifier = grid.WaypointIdentifier",
            "p.FixIcaoRegionCode = grid.WaypointIcaoRegionCode",
        ],
        "grid.WaypointLongitude_WGS84",
        "grid.WaypointLatitude_WGS84",
    ),
    # VHF navaids: fall back to DME coords if VOR coords are blank
    (
        "D",
        "",
        "primary_D__base_Navaid - VHF Navaid",
        "vhf",
        ["p.FixIdentifier = vhf.VORIdentifier", "p.FixIcaoRegionCode = vhf.VorIcaoRegionCode"],
        "COALESCE(vhf.VORLongitude_WGS84, vhf.DMELongitude_WGS84)",
        "COALESCE(vhf.VORLatitude_WGS84,  vhf.DMELatitude_WGS84)",
    ),
    (
        "D",
        "B",
        "primary_D_B_base_Navaid - NDB Navaid",
        "ndb",
        ["p.FixIdentifier = ndb.NDBIdentifier", "p.FixIcaoRegionCode = ndb.NdbIcaoRegionCode"],
        "ndb.NDBLongitude_WGS84",
        "ndb.NDBLatitude_WGS84",
    ),
    (
        "P",
        "N",
        "primary_P_N_base_Airport - Terminal NDB",
        "tndb",
        [
            "p.LandingFacilityIcaoIdentifier = tndb.LandingFacilityIcaoIdentifier",
            "p.FixIdentifier = tndb.NDBIdentifier",
            "p.FixIcaoRegionCode = tndb.NdbIcaoRegionCode",
        ],
        "tndb.NDBLongitude_WGS84",
        "tndb.NDBLatitude_WGS84",
    ),
    (
        "P",
        "G",
        "primary_P_G_base_Airport - Runways",
        "rwy",
        [
            "p.LandingFacilityIcaoIdentifier = rwy.LandingFacilityIcaoIdentifier",
            "p.FixIdentifier = rwy.RunwayIdentifier",
        ],
        "rwy.RunwayLongitude_WGS84",
        "rwy.RunwayLatitude_WGS84",
    ),
    # Heliport terminal waypoints (H/C fixes in H/D, H/E, H/F procedures)
    (
        "H",
        "C",
        "primary_H_C_base_Heliport - Terminal Waypoints",
        "htc",
        [
            "p.LandingFacilityIcaoIdentifier = htc.LandingFacilityIcaoIdentifier",
            "p.FixIdentifier = htc.WaypointIdentifier",
            "p.FixIcaoRegionCode = htc.WaypointIcaoRegionCode",
        ],
        "htc.WaypointLongitude_WGS84",
        "htc.WaypointLatitude_WGS84",
    ),
]


def _load_spatialite(conn: sqlite3.Connection) -> None:
    conn.enable_load_extension(True)
    for candidate in _SPATIALITE_CANDIDATES:
        try:
            conn.load_extension(candidate)
            return
        except sqlite3.OperationalError:
            continue
    raise RuntimeError(
        "mod_spatialite not found — run inside the container or install libsqlite3-mod-spatialite"
    )


# ---------------------------------------------------------------------------
# Flat-earth geometry helpers (accurate to << 1 nm for legs under ~100 nm)
# ---------------------------------------------------------------------------


def _project(lon: float, lat: float, course_deg: float, dist_nm: float) -> tuple[float, float]:
    """Return a point projected from (lon, lat) along course_deg for dist_nm."""
    r = math.radians(course_deg)
    cos_lat = math.cos(math.radians(lat))
    return (
        lon + math.sin(r) * dist_nm / (60.0 * cos_lat),
        lat + math.cos(r) * dist_nm / 60.0,
    )


def _arc_pts(
    from_lon: float,
    from_lat: float,
    to_lon: float,
    to_lat: float,
    cx: float,
    cy: float,
    clockwise: bool,
    n: int = 16,
) -> list[tuple[float, float]]:
    """Interpolate n+1 points along the arc from (from) to (to) around center (cx, cy)."""
    ref_lat = (from_lat + to_lat + cy) / 3.0
    cos_lat = math.cos(math.radians(ref_lat))

    # Scaled so x-distances are comparable to y-distances in degrees
    ax = (from_lon - cx) * cos_lat
    ay = from_lat - cy
    bx = (to_lon - cx) * cos_lat
    by = to_lat - cy

    a0 = math.atan2(ay, ax)
    a1 = math.atan2(by, bx)
    r = math.sqrt(ax**2 + ay**2)

    if clockwise:
        if a1 > a0:
            a1 -= 2 * math.pi
    else:
        if a1 < a0:
            a1 += 2 * math.pi

    pts: list[tuple[float, float]] = []
    for i in range(n + 1):
        angle = a0 + (a1 - a0) * i / n
        pts.append(
            (
                cx + r * math.cos(angle) / cos_lat,
                cy + r * math.sin(angle),
            )
        )
    return pts


def _course_dme_isect(
    lon: float,
    lat: float,
    course_deg: float,
    nav_lon: float,
    nav_lat: float,
    dme_nm: float,
) -> tuple[float, float] | None:
    """Point where the course from (lon, lat) intersects the DME arc around (nav_lon, nav_lat)."""
    cos_lat = math.cos(math.radians(lat))
    cr = math.radians(course_deg)
    sc, cc = math.sin(cr), math.cos(cr)  # unit direction (east, north) per nm

    nx = (nav_lon - lon) * 60.0 * cos_lat  # nav position in nm, east
    ny = (nav_lat - lat) * 60.0  # nav position in nm, north

    # |t*(sc,cc) - (nx,ny)|² = dme_nm²  →  t² - 2t·(sc·nx+cc·ny) + (nx²+ny²-dme²) = 0
    b = sc * nx + cc * ny
    c = nx**2 + ny**2 - dme_nm**2
    disc = b**2 - c
    if disc < 0:
        return None
    sq = math.sqrt(disc)
    for t in sorted([b - sq, b + sq]):
        if t > 0.1:
            return (lon + sc * t / (60.0 * cos_lat), lat + cc * t / 60.0)
    return None


def _course_radial_isect(
    lon: float,
    lat: float,
    course_deg: float,
    nav_lon: float,
    nav_lat: float,
    radial_deg: float,
) -> tuple[float, float] | None:
    """Intersection of the course from (lon, lat) with the radial from (nav_lon, nav_lat)."""
    ref_lat = (lat + nav_lat) / 2.0
    cos_lat = math.cos(math.radians(ref_lat))

    cr = math.radians(course_deg)
    rr = math.radians(radial_deg)
    d1x, d1y = math.sin(cr), math.cos(cr)  # course direction (east, north) per nm
    d2x, d2y = math.sin(rr), math.cos(rr)  # radial direction

    # Nav offset in nm
    nx = (nav_lon - lon) * 60.0 * cos_lat
    ny = (nav_lat - lat) * 60.0

    # t·d1 = (nx,ny) + s·d2  →  [d1x -d2x][t] = [nx]
    #                             [d1y -d2y][s]   [ny]
    det = d1x * (-d2y) - (-d2x) * d1y
    if abs(det) < 1e-10:
        return None
    t = (nx * (-d2y) - (-d2x) * ny) / det
    if t < 0:
        return None
    return (lon + d1x * t / (60.0 * cos_lat), lat + d1y * t / 60.0)


def _turn_arc_pts(
    p0: tuple[float, float],
    p1: tuple[float, float],
    p2: tuple[float, float],
    radius_nm: float,
    n_arc: int = 8,
) -> list[tuple[float, float]]:
    """Circular turn-anticipation arc that replaces the sharp corner at p1.

    Computes the inscribed arc of the given radius tangent to both the
    incoming leg (p0→p1) and the outgoing leg (p1→p2).  Returns n_arc+1
    points spanning from the lead point before p1 to the roll-out point
    after p1.  Falls back to [p1] for negligible turns (< 2°) or legs
    too short to accommodate the lead distance.

    Turn radius for standard-rate (3°/s) turns:
        r_nm = V_kts / (60 · π)
    """
    ref_lat = p1[1]
    cos_lat = math.cos(math.radians(ref_lat))

    def _nm(pt: tuple[float, float]) -> tuple[float, float]:
        return ((pt[0] - p1[0]) * 60.0 * cos_lat, (pt[1] - p1[1]) * 60.0)

    def _deg(x: float, y: float) -> tuple[float, float]:
        return (p1[0] + x / (60.0 * cos_lat), p1[1] + y / 60.0)

    a = _nm(p0)
    b = _nm(p2)
    len_a = math.hypot(*a)
    len_b = math.hypot(*b)
    if len_a < 1e-6 or len_b < 1e-6:
        return [p1]

    d_in = (-a[0] / len_a, -a[1] / len_a)  # unit vector p0 → p1
    d_out = (b[0] / len_b, b[1] / len_b)  # unit vector p1 → p2

    dot = d_in[0] * d_out[0] + d_in[1] * d_out[1]
    if dot >= 0.9998:  # < ~2° track change — not worth arcing
        return [p1]

    # cross > 0 → d_out is CCW from d_in → left turn
    # cross < 0 → right turn
    cross = d_in[0] * d_out[1] - d_in[1] * d_out[0]

    if 1.0 + dot < 1e-9:  # ~180° reversal — geometry undefined
        return [p1]

    # Lead distance = r · tan(θ/2);  tan(θ/2) = √((1−cosθ)/(1+cosθ))
    tan_half = math.sqrt((1.0 - dot) / (1.0 + dot))
    lead = min(radius_nm * tan_half, 0.45 * len_a, 0.45 * len_b)
    if lead < 0.005:
        return [p1]

    # Turn-start S (back along incoming) and turn-end E (forward along outgoing)
    sx, sy = -lead * d_in[0], -lead * d_in[1]
    ex, ey = lead * d_out[0], lead * d_out[1]

    # Arc centre: perpendicular from S toward the inside of the turn
    if cross > 0:  # left turn → centre is left of d_in
        px, py = -d_in[1], d_in[0]
    else:  # right turn → centre is right of d_in
        px, py = d_in[1], -d_in[0]
    cx, cy = sx + radius_nm * px, sy + radius_nm * py

    a0 = math.atan2(sy - cy, sx - cx)
    a1 = math.atan2(ey - cy, ex - cx)
    if cross > 0:  # CCW
        if a1 < a0:
            a1 += 2.0 * math.pi
    else:  # CW
        if a1 > a0:
            a1 -= 2.0 * math.pi

    r = math.hypot(sx - cx, sy - cy)
    return [
        _deg(
            cx + r * math.cos(a0 + (a1 - a0) * i / n_arc),
            cy + r * math.sin(a0 + (a1 - a0) * i / n_arc),
        )
        for i in range(n_arc + 1)
    ]


def _smooth_turns(
    points: list[tuple[float, float]],
    radius_nm: float,
    n_arc: int = 8,
) -> list[tuple[float, float]]:
    """Replace each interior waypoint corner with a turn-anticipation arc."""
    if len(points) < 3:
        return points
    out: list[tuple[float, float]] = [points[0]]
    for i in range(1, len(points) - 1):
        out.extend(_turn_arc_pts(points[i - 1], points[i], points[i + 1], radius_nm, n_arc))
    out.append(points[-1])
    return out


def _flt(val: object) -> float | None:
    """Coerce a sqlite3.Row value to float, returning None for blank/NULL."""
    if val is None:
        return None
    s = str(val).strip()
    try:
        return float(s) if s else None
    except ValueError:
        return None


def _parse_tenths(s: str | None) -> float | None:
    """Parse a tenths-encoded ARINC 424 field (e.g. '2650' → 265.0)."""
    if not s:
        return None
    s = s.strip()
    return int(s) / 10.0 if s.isdigit() else None


def _parse_declination(s: str | None) -> float | None:
    """Parse ARINC 424 StationDeclination into signed degrees.

    Format: hemisphere char ('E'/'W') + 4-digit tenths, e.g. 'E0050' → +5.0,
    'W0200' → -20.0.  East variation is positive (true = magnetic + variation).
    """
    if not s:
        return None
    s = s.strip()
    if len(s) < 5:
        return None
    hemi = s[0].upper()
    try:
        val = int(s[1:5]) / 10.0
    except ValueError:
        return None
    if hemi == "E":
        return val
    if hemi == "W":
        return -val
    return None


# ---------------------------------------------------------------------------
# SQL builder
# ---------------------------------------------------------------------------


def _leg_sql(proc_table: str, ident_col: str, available: set[str]) -> str:
    """Return SQL yielding one row per procedure leg, in sequence order.

    fix_lon / fix_lat are non-NULL for legs whose fix is found in a lookup
    table (CF, TF, RF, IF, …).  They are NULL for no-fix step types
    (CA, VA, CI, VI, CD, VD, CR, VR) — the Python resolver handles those.

    arc_lon / arc_lat carry the CenterFixOrTAAProcedureTurnIndicator
    coordinates — always a terminal waypoint (P/C or H/C).  Used only
    for RF leg arc interpolation.

    nav_lon / nav_lat carry the RecommendedNavaid coordinates (VOR or NDB)
    used by CD/VD/CR/VR geometric intersections.
    """
    joins: list[str] = []
    lon_cases: list[str] = []
    lat_cases: list[str] = []

    for fix_sc, fix_ssc, tname, alias, extra_conds, lon_expr, lat_expr in _FIX_LOOKUPS:
        if tname not in available:
            continue
        sc_cond = f"p.FixSectionCode = '{fix_sc}' AND p.FixSubSectionCode = '{fix_ssc}'"
        all_conds = [f"({sc_cond})"] + extra_conds
        joins.append(f'LEFT JOIN "{tname}" AS {alias}\n    ON ' + "\n    AND ".join(all_conds))
        lon_cases.append(f"WHEN ({sc_cond}) THEN {lon_expr}")
        lat_cases.append(f"WHEN ({sc_cond}) THEN {lat_expr}")

    if not joins:
        return ""

    lon_sql = "CASE " + " ".join(lon_cases) + " ELSE NULL END"
    lat_sql = "CASE " + " ".join(lat_cases) + " ELSE NULL END"

    _T_VHF = "primary_D__base_Navaid - VHF Navaid"
    _T_NDB = "primary_D_B_base_Navaid - NDB Navaid"

    rnav_joins: list[str] = []
    nav_lon_expr = "NULL"
    nav_lat_expr = "NULL"

    if _T_VHF in available:
        rnav_joins.append(
            f'LEFT JOIN "{_T_VHF}" AS rnav_vhf\n'
            "    ON p.RecommendedNavaid != ''\n"
            "    AND p.RecommendedNavaid = rnav_vhf.VORIdentifier\n"
            "    AND p.RecommendedNavaidIcaoRegionCode = rnav_vhf.VorIcaoRegionCode"
        )
    if _T_NDB in available:
        rnav_joins.append(
            f'LEFT JOIN "{_T_NDB}" AS rnav_ndb\n'
            "    ON p.RecommendedNavaid != ''\n"
            "    AND p.RecommendedNavaid = rnav_ndb.NDBIdentifier\n"
            "    AND p.RecommendedNavaidIcaoRegionCode = rnav_ndb.NdbIcaoRegionCode"
        )

    parts: list[str] = []
    if _T_VHF in available:
        parts.append("COALESCE(rnav_vhf.VORLongitude_WGS84, rnav_vhf.DMELongitude_WGS84)")
    if _T_NDB in available:
        parts.append("rnav_ndb.NDBLongitude_WGS84")
    if parts:
        nav_lon_expr = "COALESCE(" + ", ".join(parts) + ")"

    parts = []
    if _T_VHF in available:
        parts.append("COALESCE(rnav_vhf.VORLatitude_WGS84, rnav_vhf.DMELatitude_WGS84)")
    if _T_NDB in available:
        parts.append("rnav_ndb.NDBLatitude_WGS84")
    if parts:
        nav_lat_expr = "COALESCE(" + ", ".join(parts) + ")"

    # StationDeclination is a VHF Navaid (VOR) field; NULL for NDB recommended navaids.
    decl_expr = "rnav_vhf.StationDeclination" if _T_VHF in available else "NULL"

    # RF arc centres: CenterFixOrTAAProcedureTurnIndicator is always a terminal
    # waypoint (P/C or H/C), never a navaid — so it needs its own lookup.
    _CF = "CenterFixOrTAAProcedureTurnIndicator"
    _CF_R = "CenterFixOrTAAProcedureTurnIndicatorIcaoRegionCode"
    _CF_SC = "CenterFixOrTAAProcedureTurnIndicatorSectionCode"
    _CF_SS = "CenterFixOrTAAProcedureTurnIndicatorSubSectionCode"
    _T_TC = "primary_P_C_base_Airport - Terminal Waypoints"
    _T_HTC = "primary_H_C_base_Heliport - Terminal Waypoints"

    arc_joins: list[str] = []
    arc_lon_parts: list[str] = []
    arc_lat_parts: list[str] = []

    if _T_TC in available:
        arc_joins.append(
            f'LEFT JOIN "{_T_TC}" AS arc_tc\n'
            f"    ON p.{_CF} != ''\n"
            f"    AND p.{_CF_SC} = 'P' AND p.{_CF_SS} = 'C'\n"
            f"    AND p.{_CF} = arc_tc.WaypointIdentifier\n"
            f"    AND p.{_CF_R} = arc_tc.WaypointIcaoRegionCode\n"
            "    AND p.LandingFacilityIcaoIdentifier = arc_tc.RegionCode"
        )
        arc_lon_parts.append("arc_tc.WaypointLongitude_WGS84")
        arc_lat_parts.append("arc_tc.WaypointLatitude_WGS84")

    if _T_HTC in available:
        arc_joins.append(
            f'LEFT JOIN "{_T_HTC}" AS arc_htc\n'
            f"    ON p.{_CF} != ''\n"
            f"    AND p.{_CF_SC} = 'H' AND p.{_CF_SS} = 'C'\n"
            f"    AND p.{_CF} = arc_htc.WaypointIdentifier\n"
            f"    AND p.{_CF_R} = arc_htc.WaypointIcaoRegionCode\n"
            "    AND p.LandingFacilityIcaoIdentifier = arc_htc.LandingFacilityIcaoIdentifier"
        )
        arc_lon_parts.append("arc_htc.WaypointLongitude_WGS84")
        arc_lat_parts.append("arc_htc.WaypointLatitude_WGS84")

    arc_lon_expr = "COALESCE(" + ", ".join(arc_lon_parts) + ")" if arc_lon_parts else "NULL"
    arc_lat_expr = "COALESCE(" + ", ".join(arc_lat_parts) + ")" if arc_lat_parts else "NULL"

    all_joins = "\n    ".join(joins + rnav_joins + arc_joins)

    return f"""\
SELECT airport, proc_id, trans_id, route_type, seq,
       path_term, fix_id,
       wpt_desc_1, wpt_desc_2, wpt_desc_3, wpt_desc_4,
       alt_desc, alt1, alt2, trans_alt,
       speed_lim, speed_lim_desc, vert_angle, rnp,
       mag_course, route_dist, theta, turn_dir,
       fix_lon, fix_lat, arc_lon, arc_lat, nav_lon, nav_lat, station_decl
FROM (
    SELECT
        p.LandingFacilityIcaoIdentifier      AS airport,
        p.{ident_col}                         AS proc_id,
        p.TransitionIdentifier                AS trans_id,
        p.RouteType                           AS route_type,
        CAST(p.SequenceNumber AS REAL)        AS seq,
        p.PathAndTermination                  AS path_term,
        p.FixIdentifier                       AS fix_id,
        p.WaypointDescriptionCode1            AS wpt_desc_1,
        p.WaypointDescriptionCode2            AS wpt_desc_2,
        p.WaypointDescriptionCode3            AS wpt_desc_3,
        p.WaypointDescriptionCode4            AS wpt_desc_4,
        p.AltitudeDescription                 AS alt_desc,
        p.Altitude_1                          AS alt1,
        p.Altitude_2                          AS alt2,
        p.TransitionAltitude                  AS trans_alt,
        p.SpeedLimit                          AS speed_lim,
        p.SpeedLimitDescription               AS speed_lim_desc,
        p.VerticalAngle                       AS vert_angle,
        p.RNP                                 AS rnp,
        p.MagneticCourse                      AS mag_course,
        p.RouteDistanceHoldingDistanceOrTime  AS route_dist,
        p.Theta                               AS theta,
        p.TurnDirection                       AS turn_dir,
        {lon_sql} AS fix_lon,
        {lat_sql} AS fix_lat,
        {arc_lon_expr} AS arc_lon,
        {arc_lat_expr} AS arc_lat,
        {nav_lon_expr} AS nav_lon,
        {nav_lat_expr} AS nav_lat,
        {decl_expr} AS station_decl
    FROM "{proc_table}" AS p
    {all_joins}
)
ORDER BY airport, proc_id, trans_id, seq
"""


# ---------------------------------------------------------------------------
# Position resolver: turn a list of raw leg rows into ordered (lon, lat) pts
# ---------------------------------------------------------------------------


def _resolve_legs(group: list[sqlite3.Row]) -> list[tuple[float, float]]:
    """Convert raw leg rows for one procedure+transition into a point sequence.

    Strategies by PathAndTermination:
      • Normal legs (fix_lon/fix_lat non-NULL): use directly.
      • RF  – curved arc: interpolate from prev fix to this fix around
              the RecommendedNavaid centre.
      • CI/VI – course/heading to intercept: look ahead to the next leg
              that has a resolved fix; that point IS the intercept.
      • CA/VA – course/heading to altitude: project 5 nm forward from
              the last known position along MagneticCourse (directional
              stub only — no altitude data to determine true distance).
      • CD/VD – course/heading to DME distance: intersect the outbound
              course line with a DME circle around RecommendedNavaid.
      • CR/VR – course/heading to radial: intersect the outbound course
              with the Theta radial from RecommendedNavaid.
    """
    resolved: list[tuple[float, float]] = []
    prev: tuple[float, float] | None = None

    for i, leg in enumerate(group):
        pt = leg["path_term"] or ""
        fix_lon = _flt(leg["fix_lon"])
        fix_lat = _flt(leg["fix_lat"])
        # VOR station declination converts magnetic course/radial to true.
        # East variation is positive; 0.0 when the recommended navaid is not a VOR.
        decl: float = _parse_declination(leg["station_decl"]) or 0.0

        if fix_lon is not None and fix_lat is not None:
            if pt.strip() == "RF" and prev is not None:
                # Curved arc: centre is CenterFixOrTAAProcedureTurnIndicator
                # (a terminal waypoint), not the RecommendedNavaid.
                nav_lon = _flt(leg["arc_lon"])
                nav_lat = _flt(leg["arc_lat"])
                turn = (leg["turn_dir"] or "").strip()
                if nav_lon is not None and nav_lat is not None:
                    arc = _arc_pts(
                        prev[0],
                        prev[1],
                        fix_lon,
                        fix_lat,
                        nav_lon,
                        nav_lat,
                        clockwise=(turn == "R"),
                    )
                    resolved.extend(arc[1:])  # skip duplicate of prev
                else:
                    resolved.append((fix_lon, fix_lat))
            else:
                resolved.append((fix_lon, fix_lat))
            prev = (fix_lon, fix_lat)
            continue

        # No direct fix — resolve by leg type.
        pt = pt.strip()

        if pt in ("CI", "VI"):
            # Intercept terminates at the next leg's first resolvable fix.
            for j in range(i + 1, len(group)):
                nlon = _flt(group[j]["fix_lon"])
                nlat = _flt(group[j]["fix_lat"])
                if nlon is not None and nlat is not None:
                    resolved.append((nlon, nlat))
                    prev = (nlon, nlat)
                    break

        elif pt in ("CA", "VA"):
            if prev is not None:
                course = _parse_tenths(leg["mag_course"])
                if course is not None:
                    p = _project(prev[0], prev[1], course + decl, 5.0)
                    resolved.append(p)
                    prev = p

        elif pt in ("CD", "VD"):
            if prev is not None:
                nav_lon = _flt(leg["nav_lon"])
                nav_lat = _flt(leg["nav_lat"])
                course = _parse_tenths(leg["mag_course"])
                dist = _parse_tenths(leg["route_dist"])
                if None not in (nav_lon, nav_lat, course, dist):
                    p = _course_dme_isect(
                        prev[0],
                        prev[1],
                        course + decl,  # type: ignore[arg-type]
                        nav_lon,
                        nav_lat,
                        dist,  # type: ignore[arg-type]
                    )
                    if p:
                        resolved.append(p)
                        prev = p

        elif pt in ("CR", "VR"):
            if prev is not None:
                nav_lon = _flt(leg["nav_lon"])
                nav_lat = _flt(leg["nav_lat"])
                course = _parse_tenths(leg["mag_course"])
                radial = _parse_tenths(leg["theta"])
                if None not in (nav_lon, nav_lat, course, radial):
                    # Both the outbound course and the TO-radial are magnetic;
                    # apply the same VOR declination to convert both to true.
                    p = _course_radial_isect(
                        prev[0],
                        prev[1],
                        course + decl,  # type: ignore[arg-type]
                        nav_lon,
                        nav_lat,
                        radial + decl,  # type: ignore[arg-type]
                    )
                    if p:
                        resolved.append(p)
                        prev = p
        # All other unresolvable legs are silently skipped.

    return resolved


def build_spatial(db_path: Path, design_speed_kts: float = 150.0) -> None:
    """Add LINESTRING procedure path geometries to an existing cifp.sqlite.

    Creates (or replaces) the ``procedure_paths`` table with one row per
    unique (airport, procedure, transition) combination.  Every procedure
    step is included where possible:

    * Steps with a named fix (CF, TF, DF, RF, IF, …) use the fix coordinates
      directly; RF arcs are interpolated rather than drawn as straight lines.
    * CI/VI steps use the next step's fix as the intercept point.
    * CA/VA steps project 5 nm forward along the published course (directional
      stub — altitude-dependent true distance is unknown).
    * CD/VD steps compute the course–DME-circle intersection.
    * CR/VR steps compute the course–radial intersection.

    If *design_speed_kts* is > 0 (default 150 kt), each waypoint corner is
    replaced with a circular turn-anticipation arc using a standard-rate
    (3°/s) turn radius of ``V / (60·π)`` nm.  Pass 0 to get straight
    fix-to-fix lines.

    Requires mod_spatialite — run inside the container on Linux.
    """
    db_path = db_path.resolve()
    _log.step(f"build-cifp-spatial {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA trusted_schema=ON")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA journal_mode=WAL")
        _load_spatialite(conn)

        has_meta = conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='geometry_columns'"
        ).fetchone()[0]
        if not has_meta:
            conn.execute("SELECT InitSpatialMetaData(1)")

        conn.execute("DROP TABLE IF EXISTS procedure_paths")
        conn.execute("""
            CREATE TABLE procedure_paths (
                _id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                procedure_type        TEXT,
                section_code          TEXT,
                subsection_code       TEXT,
                airport_icao          TEXT,
                procedure_identifier  TEXT,
                transition_identifier TEXT,
                route_type            TEXT,
                point_count           INTEGER
            )
        """)
        conn.execute(
            "SELECT AddGeometryColumn('procedure_paths', 'geometry', 4326, 'LINESTRING', 'XY')"
        )

        conn.execute("DROP TABLE IF EXISTS procedure_fixes")
        conn.execute("""
            CREATE TABLE procedure_fixes (
                _id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                procedure_type        TEXT,
                section_code          TEXT,
                subsection_code       TEXT,
                airport_icao          TEXT,
                procedure_identifier  TEXT,
                transition_identifier TEXT,
                route_type            TEXT,
                sequence_number       REAL,
                fix_identifier        TEXT,
                path_termination      TEXT,
                wpt_desc_1            TEXT,
                wpt_desc_2            TEXT,
                wpt_desc_3            TEXT,
                wpt_desc_4            TEXT,
                alt_description       TEXT,
                altitude_1            TEXT,
                altitude_2            TEXT,
                transition_altitude   TEXT,
                speed_limit           TEXT,
                speed_limit_desc      TEXT,
                vertical_angle        TEXT,
                rnp                   TEXT,
                magnetic_course       TEXT,
                leg_distance          TEXT,
                turn_direction        TEXT
            )
        """)
        conn.execute("SELECT AddGeometryColumn('procedure_fixes', 'geometry', 4326, 'POINT', 'XY')")

        available = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }

        total_paths = 0
        for sc, ssc, proc_type, ident_col in _PROC_SECTIONS:
            section_name = SECTION_NAMES.get((sc, ssc), f"{sc}_{ssc}")
            proc_table = f"primary_{sc}_{ssc}_base_{section_name}"
            if proc_table not in available:
                continue

            sql = _leg_sql(proc_table, ident_col, available)
            if not sql:
                _log.info(f"  {sc}/{ssc}: no fix lookup tables available, skipping")
                continue

            rows = conn.execute(sql).fetchall()
            _log.info(f"  {sc}/{ssc} ({proc_type}): {len(rows):,} legs fetched")

            paths_inserted = 0
            fixes_inserted = 0
            turn_radius_nm = design_speed_kts / (60.0 * math.pi) if design_speed_kts > 0 else 0.0

            for key, grp in groupby(
                rows, key=lambda r: (r["airport"], r["proc_id"], r["trans_id"])
            ):
                group = list(grp)
                airport, proc_id, trans_id = key
                route_type = group[0]["route_type"]

                # procedure_fixes: one POINT row per named fix
                for leg in group:
                    fix_lon = _flt(leg["fix_lon"])
                    fix_lat = _flt(leg["fix_lat"])
                    if fix_lon is None or fix_lat is None:
                        continue
                    conn.execute(
                        """INSERT INTO procedure_fixes
                           (procedure_type, section_code, subsection_code, airport_icao,
                            procedure_identifier, transition_identifier, route_type,
                            sequence_number, fix_identifier, path_termination,
                            wpt_desc_1, wpt_desc_2, wpt_desc_3, wpt_desc_4,
                            alt_description, altitude_1, altitude_2, transition_altitude,
                            speed_limit, speed_limit_desc, vertical_angle, rnp,
                            magnetic_course, leg_distance, turn_direction,
                            geometry)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                                   GeomFromText(?,4326))""",
                        (
                            proc_type,
                            sc,
                            ssc,
                            airport,
                            proc_id,
                            trans_id,
                            route_type,
                            leg["seq"],
                            leg["fix_id"],
                            leg["path_term"],
                            leg["wpt_desc_1"],
                            leg["wpt_desc_2"],
                            leg["wpt_desc_3"],
                            leg["wpt_desc_4"],
                            leg["alt_desc"],
                            leg["alt1"],
                            leg["alt2"],
                            leg["trans_alt"],
                            leg["speed_lim"],
                            leg["speed_lim_desc"],
                            leg["vert_angle"],
                            leg["rnp"],
                            leg["mag_course"],
                            leg["route_dist"],
                            leg["turn_dir"],
                            f"POINT({fix_lon} {fix_lat})",
                        ),
                    )
                    fixes_inserted += 1

                # procedure_paths: one LINESTRING per transition
                points = _resolve_legs(group)
                if turn_radius_nm > 0 and len(points) >= 3:
                    points = _smooth_turns(points, turn_radius_nm)
                if len(points) < 2:
                    continue
                wkt = "LINESTRING(" + ",".join(f"{lon} {lat}" for lon, lat in points) + ")"
                conn.execute(
                    """INSERT INTO procedure_paths
                       (procedure_type, section_code, subsection_code, airport_icao,
                        procedure_identifier, transition_identifier, route_type,
                        point_count, geometry)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, GeomFromText(?, 4326))""",
                    (proc_type, sc, ssc, airport, proc_id, trans_id, route_type, len(points), wkt),
                )
                paths_inserted += 1

            _log.info(f"    → {paths_inserted:,} paths, {fixes_inserted:,} fixes")
            total_paths += paths_inserted

        conn.execute("PRAGMA trusted_schema=ON")
        conn.execute("SELECT CreateSpatialIndex('procedure_paths', 'geometry')")
        conn.execute("SELECT CreateSpatialIndex('procedure_fixes', 'geometry')")
        conn.commit()
        _log.info(f"  done: {total_paths:,} procedure paths → procedure_paths + procedure_fixes")
    finally:
        conn.close()


def parse_cifp_coord(s: str) -> float | None:
    """Convert CIFP-format coordinate string to decimal degrees.

    Latitude formats (N/S prefix): 3-char (deg only), 9-char (HDDMMSSFF),
    11-char high-precision (HDDMMSSFFFF).
    Longitude formats (E/W prefix): 4-char (deg only), 10-char (HDDDMMSSFF),
    12-char high-precision (HDDDMMSSFFFF).

    Conversion: deg + min/60 + float(f"{sec}.{sec_frac}")/3600
    Sign: negative for S or W.
    Returns None for blank/invalid input.
    """
    if not s or not s.strip():
        return None
    s = s.strip()
    if not s:
        return None

    hemi = s[0].upper()
    n = len(s)

    try:
        if hemi in ("N", "S"):
            if n == 3:
                deg = int(s[1:3])
                mn = 0
                sec_str = "0.0"
            elif n == 9:
                deg = int(s[1:3])
                mn = int(s[3:5])
                sec_str = f"{s[5:7]}.{s[7:9]}"
            elif n == 11:
                deg = int(s[1:3])
                mn = int(s[3:5])
                sec_str = f"{s[5:7]}.{s[7:11]}"
            else:
                return None
        elif hemi in ("E", "W"):
            if n == 4:
                deg = int(s[1:4])
                mn = 0
                sec_str = "0.0"
            elif n == 10:
                deg = int(s[1:4])
                mn = int(s[4:6])
                sec_str = f"{s[6:8]}.{s[8:10]}"
            elif n == 12:
                deg = int(s[1:4])
                mn = int(s[4:6])
                sec_str = f"{s[6:8]}.{s[8:12]}"
            else:
                return None
        else:
            return None

        result = deg + mn / 60 + float(sec_str) / 3600
        if hemi in ("S", "W"):
            result = -result
        return result
    except (ValueError, IndexError):
        return None


def build(cifp_path: Path, db_path: Path) -> None:
    """Parse FAACIFP18 into SQLite tables matching the Perl pipeline's schema."""
    cifp_path = cifp_path.resolve()
    db_path = db_path.resolve()
    _log.step(f"build-cifp {cifp_path} -> {db_path}")

    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA journal_mode=WAL")

        # Pre-resolve field specs so we only call resolve_dups once per section.
        _primary_fields: dict[tuple[str, str], list[tuple[str, int]]] = {}
        for key, raw in PRIMARY_SPECS.items():
            _primary_fields[key] = resolve_dups(raw)

        _cb_fields: dict[tuple[str, str], list[tuple[str, int]]] = {}
        for key, raw in CONTINUATION_BASE_SPECS.items():
            _cb_fields[key] = resolve_dups(raw)

        _ca_fields: dict[tuple[str, str, str], list[tuple[str, int]]] = {}
        for key, raw in CONTINUATION_APP_SPECS.items():
            _ca_fields[key] = resolve_dups(raw)

        # Byte offset of ContinuationRecordNumber for each FK-relevant section.
        # Only sections in CONTINUATION_BASE_SPECS can have continuations.
        _crn_pos: dict[tuple[str, str], int] = {}
        for key in CONTINUATION_BASE_SPECS:
            p = crn_pos(key[0], key[1])
            if p is not None:
                _crn_pos[key] = p

        # Maps record_key (line[:crn_pos]) → primary _id for each FK section.
        primary_key_map: dict[tuple[str, str], dict[str, int]] = {k: {} for k in _crn_pos}

        # created[table_name] → (ordered column names list)
        created: dict[str, list[str]] = {}
        # pending[table_name] → list of value dicts
        pending: dict[str, list[dict[str, str]]] = {}

        def _flush(table: str) -> None:
            if not pending.get(table):
                return
            cols = created[table]
            placeholders = ",".join("?" * len(cols))
            sql = f'INSERT INTO "{table}" ({",".join(cols)}) VALUES ({placeholders})'
            rows = [[row.get(c, "") for c in cols] for row in pending[table]]
            conn.executemany(sql, rows)
            pending[table] = []

        def _ensure_table(
            table: str,
            columns: list[str],
            int_cols: set[str] | None = None,
        ) -> None:
            if table in created:
                return
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')

            def _col_type(c: str) -> str:
                return "INTEGER" if int_cols and c in int_cols else "TEXT"

            col_defs = "_id INTEGER PRIMARY KEY AUTOINCREMENT," + ",".join(
                f'"{c}" {_col_type(c)}' for c in columns
            )
            conn.execute(f'CREATE TABLE "{table}" ({col_defs})')
            created[table] = columns
            pending[table] = []

        # Binary line count is fast and gives tqdm a meaningful total.
        total_lines = sum(1 for _ in cifp_path.open("rb"))

        records_parsed = 0
        with cifp_path.open("r", encoding="latin-1", errors="replace") as fh:
            bar = tqdm(
                fh,
                total=total_lines,
                desc="  CIFP records",
                unit="rec",
                disable=_log.is_quiet(),
                dynamic_ncols=True,
                leave=True,
            )
            for line_number, raw_line in enumerate(bar, 1):
                line = raw_line.rstrip("\n").rstrip("\r")

                if len(line) != _RECORD_LEN:
                    continue

                rec_type = line[0]
                if rec_type == "H":
                    continue
                if rec_type != "S":
                    continue

                # Positions are 0-based: RecordType[0], CustomerAreaCode[1:4],
                # SectionCode[4], SubSectionCode[5]
                sc = line[4]
                ssc = line[5]

                # For P/H sections the SubSectionCode is at position 12 when pos 5 is blank.
                if sc in ("P", "H") and ssc == " ":
                    ssc = line[12]

                # The spec dict uses "" for blank subsection codes (e.g. VHF navaids D-'').
                ssc_key = ssc.strip()

                if (sc, ssc_key) not in PRIMARY_SPECS:
                    continue

                section_name = SECTION_NAMES.get((sc, ssc_key), f"{sc}_{ssc_key}")
                fields = _primary_fields[(sc, ssc_key)]

                # Parse record using field widths.
                parsed = _slice_record(line, fields)

                crn = parsed.get("ContinuationRecordNumber", "0")
                is_continuation = crn not in ("", "0", "1")

                p_or_c = "primary"
                application = "base"

                if is_continuation:
                    p_or_c = "continuation"
                    cb_key = (sc, ssc_key)
                    if cb_key not in CONTINUATION_BASE_SPECS:
                        # No continuation parser — skip.
                        _log.info(
                            f"  no continuation parser for {sc}/{ssc_key} (line {line_number})"
                        )
                        continue
                    cb_fields = _cb_fields[cb_key]
                    parsed = _slice_record(line, cb_fields)
                    application = parsed.get("ApplicationType", "").strip() or "base"

                    ca_key = (sc, ssc_key, application)
                    if ca_key not in CONTINUATION_APP_SPECS:
                        _log.info(
                            f"  no app continuation parser for {sc}/{ssc_key}/{application}"
                            f" (line {line_number})"
                        )
                        continue
                    ca_fields = _ca_fields[ca_key]
                    parsed = _slice_record(line, ca_fields)
                    fields = ca_fields

                # Strip whitespace from all values.
                for k in list(parsed):
                    parsed[k] = parsed[k].strip()

                # Add WGS84 columns for lat/lon fields (in field-definition order).
                ordered_cols = [
                    name for name, _ in fields if not re.search(r"BlankSpacing", name, re.I)
                ]
                wgs84_cols: list[str] = []
                coord_re = re.compile(r"(?:latitude|longitude)$", re.I)
                for col in ordered_cols:
                    if coord_re.search(col):
                        wgs84_col = col + "_WGS84"
                        wgs84_cols.append(wgs84_col)
                        val = parsed.get(col, "")
                        converted = parse_cifp_coord(val) if val else None
                        parsed[wgs84_col] = str(converted) if converted is not None else ""

                # FK: for continuation rows, look up the matching primary _id.
                fk_key = (sc, ssc_key)
                if is_continuation and fk_key in _crn_pos:
                    record_key = line[: _crn_pos[fk_key]]
                    parent_id = primary_key_map[fk_key].get(record_key)
                    if parent_id is None:
                        _log.info(
                            f"  no primary for continuation {sc}/{ssc_key} at line {line_number}"
                        )
                    # Store as string; INTEGER affinity in SQLite coerces "42" → 42.
                    parsed["_primary_id"] = str(parent_id) if parent_id is not None else ""
                    final_cols = ["_primary_id"] + ordered_cols + wgs84_cols
                else:
                    final_cols = ordered_cols + wgs84_cols

                table = f"{p_or_c}_{sc}_{ssc_key}_{application}_{section_name}"
                int_cols = {"_primary_id"} if "_primary_id" in parsed else None
                _ensure_table(table, final_cols, int_cols=int_cols)

                # FK: for primary rows in FK-relevant sections, insert per-row so we can
                # capture lastrowid immediately (primary always precedes its continuations).
                if not is_continuation and fk_key in _crn_pos:
                    cols = created[table]
                    cur = conn.execute(
                        f'INSERT INTO "{table}" ({",".join(cols)})'
                        f" VALUES ({','.join('?' * len(cols))})",
                        [parsed.get(c, "") for c in cols],
                    )
                    assert cur.lastrowid is not None
                    primary_key_map[fk_key][line[: _crn_pos[fk_key]]] = cur.lastrowid
                    records_parsed += 1
                    continue

                pending[table].append(parsed)
                records_parsed += 1

                if len(pending.get(table, [])) >= 1000:
                    _flush(table)

        # Flush all remaining.
        for t in list(pending):
            _flush(t)

        conn.commit()
        _log.info(f"  done: {records_parsed:,} records, {len(created)} tables → {db_path}")
    finally:
        conn.close()


def _slice_record(line: str, fields: list[tuple[str, int]]) -> dict[str, str]:
    """Slice a 132-char record into a dict using the given field widths."""
    result: dict[str, str] = {}
    pos = 0
    for name, width in fields:
        result[name] = line[pos : pos + width]
        pos += width
    return result
