-- Sample queries against the databases produced by `nasr build`.
--
-- Tables and column names follow the FAA NASR CSV subscription convention
-- (see the *_DATA_LAYOUT.pdf files inside the CSV bundle for the full schema).
-- Lat/long columns named LAT_DECIMAL / LONG_DECIMAL are signed decimal degrees.

--------------------------------------------------------------------------------
-- Find which Class B/C/D/E airspace contains a given point.
-- $ spatialite class_airspace_spatialite.sqlite
--------------------------------------------------------------------------------
SELECT
    IDENT, NAME, CLASS, LOCAL_TYPE, UPPER_DESC, LOWER_DESC
FROM
    Class_Airspace
WHERE
    Within(GeomFromText('POINT(-80.79 34.04)', 4326), GEOMETRY);

--------------------------------------------------------------------------------
-- Find which special-use airspace (MOA / restricted / prohibited / etc.)
-- contains a given point. The Airspace table merges every per-airspace AIXM
-- file into one row per airspace, with `_source_xml` carrying the human-
-- readable airspace name (e.g. "R-6001A FORT JACKSON, SC").
-- $ spatialite special_use_airspace_spatialite.sqlite
--------------------------------------------------------------------------------
SELECT
    designator,
    name,
    saaType,
    administrativeArea,
    _source_xml
FROM
    Airspace
WHERE
    Within(GeomFromText('POINT(-80.79 34.04)', 4326), GEOMETRY);

--------------------------------------------------------------------------------
-- Find the controlling unit and owning organisation for each airspace in a
-- state, joining via the AIXM XLink-derived foreign keys (UUIDs):
--
--   Airspace <- AirTrafficControlService.clientAirspace
--   AirTrafficControlService.serviceProvider -> Unit
--   Unit.ownerOrganisation -> OrganisationAuthority
--
-- Every metadata table also carries _source_xml for human-readable lookup
-- (e.g. "R-6001A FORT JACKSON, SC"); the UUID joins below are the precise
-- relationships from the source AIXM, not file-name string matches.
--------------------------------------------------------------------------------
-- DISTINCT collapses the per-airspace duplication: shared metadata entities
-- (Unit, OrganisationAuthority, ...) are re-declared in every XML that
-- references them, so the joined product is much wider than you'd expect.
-- Dedup-on-write would lose information (per-XML timing fields differ), so
-- the build keeps every row and we collapse at query time.
SELECT DISTINCT
    a.designator,
    a.name             AS airspace,
    u.name             AS controlling_unit,
    o.name             AS owning_organisation
FROM
    Airspace                  AS a
    LEFT JOIN AirTrafficControlService AS atc ON atc.clientAirspace   = a.identifier
    LEFT JOIN Unit                     AS u   ON u.identifier         = atc.serviceProvider
    LEFT JOIN OrganisationAuthority    AS o   ON o.identifier         = u.ownerOrganisation
WHERE
    a.administrativeArea = 'ALABAMA'
ORDER BY
    a.designator;

--------------------------------------------------------------------------------
-- All obstacles within 5 NM of a point, tallest first.
-- Note: for SRID 4326, PtDistWithin / Distance return meters (not degrees).
-- $ spatialite nasr.sqlite
--------------------------------------------------------------------------------
SELECT
    OAS, CITY, STATE, TYPE, AGL, AMSL,
    LATDEC, LONDEC
FROM
    OBSTACLE
WHERE
    PtDistWithin(MakePoint(-80.79, 34.04, 4326), geometry, 5.0 * 1852, 1) = 1
ORDER BY
    CAST(AGL AS INTEGER) DESC;

--------------------------------------------------------------------------------
-- All AWOS / ASOS stations within 5 NM of a point, sorted by distance.
-- $ spatialite nasr.sqlite
--------------------------------------------------------------------------------
SELECT
    ASOS_AWOS_ID, ASOS_AWOS_TYPE, CITY, STATE_CODE, PHONE_NO,
    Distance(geometry, MakePoint(-80.79, 34.04, 4326), 1) / 1852.0 AS distance_nm
FROM
    AWOS
WHERE
    PtDistWithin(MakePoint(-80.79, 34.04, 4326), geometry, 5.0 * 1852, 1) = 1
ORDER BY
    distance_nm;

--------------------------------------------------------------------------------
-- Per-airport summary: ATC tower call/hours plus on-airport AWOS/ASOS.
-- $ sqlite3 nasr.sqlite     (no spatial functions needed)
--------------------------------------------------------------------------------
WITH
    awos_at_airport AS (
        SELECT
            SITE_NO,
            GROUP_CONCAT(ASOS_AWOS_TYPE || ':' || ASOS_AWOS_ID, '; ') AS awos
        FROM AWOS
        WHERE SITE_TYPE_CODE = 'A'        -- A = on a landing facility
        GROUP BY SITE_NO
    )
SELECT
    apt.ARPT_ID,
    apt.ARPT_NAME,
    apt.STATE_CODE,
    atc.TWR_CALL,
    atc.TWR_HRS,
    atc.PRIMARY_APCH_RADIO_CALL,
    atc.PRIMARY_DEP_RADIO_CALL,
    awos.awos
FROM
    APT_BASE AS apt
    LEFT JOIN ATC_BASE AS atc       ON atc.SITE_NO = apt.SITE_NO
    LEFT JOIN awos_at_airport AS awos ON awos.SITE_NO = apt.SITE_NO
WHERE
    apt.ARPT_ID IN ('OFP', 'JYO', 'RIC', 'IAD', 'ADW')
ORDER BY
    apt.ARPT_ID;

--------------------------------------------------------------------------------
-- All VHF (118-137 MHz) frequencies serving a given airport.
-- The FRQ table holds the post-2023 unified frequency listing (tower/approach/
-- departure/center/etc.) keyed by SERVICED_FACILITY = airport identifier.
--------------------------------------------------------------------------------
SELECT
    SERVICED_FACILITY,
    FACILITY_TYPE,
    TOWER_OR_COMM_CALL,
    FREQ,
    FREQ_USE,
    SECTORIZATION,
    REMARK
FROM
    FRQ
WHERE
    SERVICED_FACILITY = 'IAD'
    AND CAST(FREQ AS REAL) BETWEEN 118 AND 137
ORDER BY
    FACILITY_TYPE, FREQ;

--------------------------------------------------------------------------------
-- ARTCC boundary segments as ordered point lists (build a polygon per boundary).
-- $ sqlite3 nasr.sqlite
--------------------------------------------------------------------------------
.headers on
.mode csv
.output arb-polygons.csv
SELECT
    LOCATION_ID || '-' || COALESCE(ALTITUDE, '') AS unique_id,
    LOCATION_NAME,
    ALTITUDE,
    'POLYGON((' ||
        GROUP_CONCAT(LONG_DECIMAL || ' ' || LAT_DECIMAL,
                     ',' ORDER BY CAST(POINT_SEQ AS INTEGER)) ||
    '))' AS wkt
FROM
    ARB_SEG
GROUP BY
    LOCATION_ID, ALTITUDE
ORDER BY
    LOCATION_ID, ALTITUDE;
.output stdout

--------------------------------------------------------------------------------
-- Airway segments as WKT linestrings between consecutive POINT_SEQ values.
-- $ sqlite3 nasr.sqlite
--------------------------------------------------------------------------------
-- Note: AWY_SEG_ALT.csv does not include lon/lat for FROM/TO_POINT; we join to
-- FIX_BASE / NAV_BASE to find them.
.headers on
.mode csv
.output airways.csv
WITH
    pt(name, lon, lat) AS (
        SELECT FIX_ID, LONG_DECIMAL, LAT_DECIMAL FROM FIX_BASE
        UNION ALL
        SELECT NAV_ID, LONG_DECIMAL, LAT_DECIMAL FROM NAV_BASE
    )
SELECT
    seg.AWY_ID,
    seg.POINT_SEQ,
    seg.FROM_POINT,
    seg.TO_POINT,
    seg.MIN_ENROUTE_ALT,
    seg.MIN_ENROUTE_ALT_OPPOSITE,
    seg.MAX_AUTH_ALT,
    'LINESTRING(' || a.lon || ' ' || a.lat || ',' || b.lon || ' ' || b.lat || ')' AS wkt
FROM
    AWY_SEG_ALT AS seg
    JOIN pt AS a ON a.name = seg.FROM_POINT
    JOIN pt AS b ON b.name = seg.TO_POINT
WHERE
    seg.AWY_ID = 'V38'
ORDER BY
    seg.AWY_ID, CAST(seg.POINT_SEQ AS INTEGER);
.output stdout

--------------------------------------------------------------------------------
-- All remarks for airports in a given state.
-- $ sqlite3 nasr.sqlite
--------------------------------------------------------------------------------
SELECT
    apt.ARPT_ID,
    rmk.TAB_NAME,
    rmk.REF_COL_NAME,
    rmk.ELEMENT,
    rmk.REMARK
FROM
    APT_BASE AS apt
    JOIN APT_RMK AS rmk ON rmk.SITE_NO = apt.SITE_NO
WHERE
    apt.STATE_NAME = 'VIRGINIA'
ORDER BY
    apt.ARPT_ID, rmk.TAB_NAME, rmk.REF_COL_NAME;

--------------------------------------------------------------------------------
-- Runway shapes as LINESTRINGs (use this to render runway centerlines).
-- APT_RWY.runway_geometry is built at build-spatial time by joining APT_RWY
-- to its two APT_RWY_END rows on (SITE_NO, RWY_ID).
-- $ spatialite nasr.sqlite
--------------------------------------------------------------------------------
SELECT
    apt.ARPT_ID,
    rwy.RWY_ID,
    rwy.RWY_LEN,
    ST_AsText(rwy.runway_geometry) AS shape
FROM
    APT_BASE AS apt
    JOIN APT_RWY AS rwy ON rwy.SITE_NO = apt.SITE_NO
WHERE
    apt.ARPT_ID = 'RIC';

--------------------------------------------------------------------------------
-- ATC tower locations -- ATC_BASE.geometry is populated at build-spatial time
-- by looking up the airport via SITE_NO (ATC_BASE itself has no lat/long).
--------------------------------------------------------------------------------
SELECT
    FACILITY_ID,
    FACILITY_NAME,
    TWR_CALL,
    ST_AsText(geometry) AS location
FROM
    ATC_BASE
WHERE
    FACILITY_ID IN ('IAD', 'DCA', 'BWI');

--------------------------------------------------------------------------------
-- Holding pattern -- both fix_geometry (from FIX_BASE.FIX_ID) and
-- navaid_geometry (from NAV_BASE.NAV_ID) are populated by build-spatial.
--------------------------------------------------------------------------------
SELECT
    HP_NAME,
    FIX_ID,
    NAV_ID,
    HOLD_DIRECTION,
    LEG_LENGTH_DIST,
    ST_AsText(fix_geometry)    AS fix_loc,
    ST_AsText(navaid_geometry) AS nav_loc
FROM
    HPF_BASE
WHERE
    FIX_ID = 'AABEE';

--------------------------------------------------------------------------------
-- Runway lengths/widths for a given airport.
--------------------------------------------------------------------------------
SELECT
    apt.ARPT_ID,
    rwy.RWY_ID,
    rwy.RWY_LEN,
    rwy.RWY_WIDTH,
    rwy.SURFACE_TYPE_CODE,
    rwy.RWY_LGT_CODE
FROM
    APT_BASE AS apt
    JOIN APT_RWY AS rwy ON rwy.SITE_NO = apt.SITE_NO
WHERE
    apt.ARPT_ID = 'OFP'
ORDER BY
    rwy.RWY_ID;

--------------------------------------------------------------------------------
-- Runway endpoints (per-end true alignment, lat/long, displaced threshold).
-- APT_RWY_END has one row per end, joined back to APT_RWY by (SITE_NO, RWY_ID).
--------------------------------------------------------------------------------
SELECT
    apt.ARPT_ID,
    rwy.RWY_ID,
    end.RWY_END_ID,
    end.TRUE_ALIGNMENT,
    end.LAT_DECIMAL  AS end_lat,
    end.LONG_DECIMAL AS end_lon,
    end.RWY_END_ELEV,
    end.DISPLACED_THR_LEN
FROM
    APT_BASE AS apt
    JOIN APT_RWY     AS rwy ON rwy.SITE_NO = apt.SITE_NO
    JOIN APT_RWY_END AS end ON end.SITE_NO = apt.SITE_NO AND end.RWY_ID = rwy.RWY_ID
WHERE
    apt.ARPT_ID = 'RIC'
ORDER BY
    rwy.RWY_ID, end.RWY_END_ID;
