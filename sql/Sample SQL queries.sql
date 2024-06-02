-- An example spatialite query to find airspaces that a given point is inside:
-- $ spatialite special_use_airspace_spatialite.sqlite 
SELECT 
    designator, name, upperLimit, lowerLimit  
FROM 
    Airspace 
WHERE 
    within(GeomFromText('POINT(-80.79 34.04)'),Airspace.Geometry);

-- R6001A|R-6001A FORT JACKSON, SC|3200|GND
-- R6001B|R-6001B FORT JACKSON, SC|230|3200

-- All obstacles a certain distance from a point
-- $ spatialite spatialite_nasr.sqlite
SELECT
    *
FROM
    OBSTACLE_OBSTACLE as obstacle
WHERE
    PtDistWithin( GeomFromText('POINT(-80.79 34.04)') , obstacle.obstacleGeom, .1 )
    ;

SELECT * FROM MyPoints
WHERE PtDistWithin(Geometry,
MakePoint(11.87691, 43.46139, 4326),
2500.0, 1) = 1;

--------------------------------------------------------------------------------
--All VHF frequencies, including remotes and SID/STARs, for an airport
-- Uses Common Table Expressions (CTEs)
--------------------------------------------------------------------------------
WITH 
    names(landing_facility_site_number, tower,approach, departure, approach_backup, departure_backup, military_call) AS (
        SELECT
            twr1.landing_facility_site_number
            , twr1.radio_call_used_by_pilot_to_contact_tower
            , twr1.radio_call_of_facility_that_furnishes_primary_approach_control
            , twr1.radio_call_of_facility_that_furnishes_primary_departure_control
            , twr1.radio_call_of_facility_that_takes_over_approach_control_when_pr
            , twr1.radio_call_of_facility_that_takes_over_departure_control_when_p
            , twr1.radio_call_name_for_military_operations_at_this_airport
        FROM 
            twr_twr1 AS twr1
        ),
        
    awos (landing_facility_site_number, awos) AS (
        SELECT 
            awos.landing_facility_site_number_when_station_located_at_airport
            , GROUP_CONCAT( awos.wx_sensor_type || '*' || awos.station_frequency )

        FROM
            awos_awos1 as awos
            
        GROUP BY
            awos.landing_facility_site_number_when_station_located_at_airport
        ),
        
    local_freqs (terminal_communications_facility_identifier, local_freqs) AS (
        SELECT
            twr3a.terminal_communications_facility_identifier
            , group_concat( twr3a.frequency_use || '*' || twr3a.frequency   )
        FROM 
            twr_twr3a AS twr3a
        WHERE
            (twr3a.frequency IS NULL OR CAST(twr3a.frequency AS REAL) BETWEEN 118 and 137)
        GROUP BY
            twr3a.terminal_communications_facility_identifier
        ),
        
    remote_freqs (landing_facility_site_number, remote_freqs) AS (
        SELECT 
            twr7.satellite_airport_site_number
            , GROUP_CONCAT( twr7.satellite_frequency_use || '*' || twr7.satellite_frequency)
        FROM 
            twr_twr7 AS twr7
        WHERE
            (twr7.satellite_frequency IS NULL OR CAST(twr7.satellite_frequency AS REAL) BETWEEN 118 and 137)
        GROUP BY
            twr7.satellite_airport_site_number
        )
SELECT
    apt.location_identifier
    , apt.landing_facility_site_number
    , apt.common_traffic_advisory_frequency_ctaf
    , apt.unicom_frequency_available_at_the_airport
    , tower
    , approach
    , departure
    , approach_backup
    , departure_backup
    , military_call
    , awos
    , local_freqs
    , remote_freqs
    
FROM
    apt_apt AS apt

LEFT OUTER JOIN
    names
        ON
            apt.landing_facility_site_number 
            = names.landing_facility_site_number

LEFT OUTER JOIN
    awos
        ON
            apt.landing_facility_site_number 
            = awos.landing_facility_site_number

LEFT OUTER JOIN
    local_freqs
        ON
            apt.location_identifier
            = local_freqs.terminal_communications_facility_identifier

LEFT OUTER JOIN
    remote_freqs
        ON
            apt.landing_facility_site_number 
            = remote_freqs.landing_facility_site_number

WHERE
    -- An example selection of various sized military and civilian airports
    apt.location_identifier IN ('OFP' , 'JYO' , 'RIC' , 'IAD' , 'ADW')

ORDER BY
    apt.location_identifier
;
--------------------------------------------------------------------------------
--ARB polygons
-- Note that these polygons aren't closed as they should be per-standard
-- Still trying out how to get just the first point via SQL and add to end of 
-- point list
--------------------------------------------------------------------------------
.headers on
.mode csv
.output "arb-polygons.csv"
WITH first_point as (
    SELECT
        center_name || '-' || altitude_structure_decode_name AS unique_id
        , longitude
        , latitude
    FROM
        arb_arb
    GROUP BY
        unique_id
    ORDER BY 
--      CAST(six_digit_number_used_to_maintain_proper_sequence_of_boundary_s AS REAL)
    )
SELECT
    center_name || '-' || altitude_structure_decode_name AS unique_id
    , center_name 
    , altitude_structure_decode_name
    , 'polygon (( '
        || GROUP_CONCAT(  longitude 
                        || ' ' 
                        || latitude )
        || ' ))'
        AS
            geometry
FROM
    arb_arb
GROUP BY
    unique_id
;
--------------------------------------------------------------------------------
--Individual segments in an airway
--  Won't work if sequence numbers aren't incrementing by 10
--------------------------------------------------------------------------------
.headers on
.mode csv
.output airways.csv

SELECT
    awy1.airway_designation
    , awy1.airway_type
    , awy1.airway_point_sequence_number
    , awy2.navaid_facility_fix_name
    , awy1.point_to_point_minimum_enroute_altitude_mea
    , awy1.point_to_point_minimum_enroute_altitude_mea_opposite_direction
    , awy2.fix_minimum_reception_altitude_mra
    , awy2.longitude AS Longitude1
    , awy2.latitude AS Latitude1
    , awy2a.longitude AS Longitude2
    , awy2a.latitude AS Latitude2
    , 'linestring( ' 
        || awy2.longitude 
        || ' ' 
        || awy2.latitude  
        || ' , ' 
        || awy2a.longitude 
        || ' ' 
        || awy2a.latitude 
        || ' )' 
            AS geometry
    
FROM 
    awy_awy1 AS awy1
JOIN 
    awy_awy2 AS awy2
        ON
            awy1.airway_designation = awy2.airway_designation
        AND
            awy1.airway_point_sequence_number = awy2.airway_point_sequence_number 
JOIN 
    awy_awy2 AS awy2a
        ON
            awy1.airway_designation = awy2a.airway_designation            
        AND
            CAST (awy1.airway_point_sequence_number AS REAL) + 10 = CAST(awy2a.airway_point_sequence_number AS REAL)
-- WHERE
--     awy1.airway_designation = 'V38'
ORDER BY
    awy1.airway_point_sequence_number
    ;
--------------------------------------
--All remarks for a given airport
SELECT 
    apt.location_identifier
    , rem.remark_element_name
    , rem.remark_element_name_expanded
    , rem.remark_text
FROM 
    apt_apt AS apt
JOIN 
    apt_rmk AS rem
ON 
    apt.landing_facility_site_number = rem.landing_facility_site_number
WHERE
    -- apt.location_identifier = 'JYO'
    apt.associated_state_name LIKE "%VIR%"
ORDER BY
    apt.location_identifier
    , rem.remark_element_name
;
--------------------------------------
--All runways and their lengths for a given airport
SELECT 
    apt.location_identifier
    , rwy.runway_identification
    , rwy.runway_physical_runway_length_nearest_foot
FROM 
    apt_apt AS apt
JOIN 
    apt_rwy AS rwy
ON 
    apt.landing_facility_site_number=rwy.landing_facility_site_number
WHERE
    apt.location_identifier = 'OFP'
    --apt.associated_state_name LIKE "%VA%"
--      GROUP BY 
    --	apt.location_identifier
ORDER BY
    apt.location_identifier,rwy.runway_identification
;
--------------------------------------
--All runways and their lengths for a given airport
SELECT 
    rwy. base_end_identifier
    , rwy.base_latitude
    , rwy.base_longitude
    , rwy.base_runway_end_true_alignment
    , rwy.reciprocal_end_identifier
    , rwy.reciprocal_latitude
    , rwy.reciprocal_longitude
    , rwy.reciprocal_runway_end_true_alignment
    , rwy.runway_identification
FROM 
    apt_apt AS apt
JOIN 
    apt_rwy AS rwy
ON 
    apt.landing_facility_site_number=rwy.landing_facility_site_number
WHERE
    apt.location_identifier = 'RIC'
    --apt.associated_state_name LIKE "%VA%"
--      GROUP BY 
--	apt.location_identifier
ORDER BY
    apt.location_identifier,rwy.runway_identification
;

--------------------------------------
--All runways and their lengths for a given airport
SELECT 
    apt.location_identifier
    , rwy.runway_identification
    , rwy.base_runway_end_true_alignment
    , rwy.reciprocal_runway_end_true_alignment
    , base_latitude
    , base_longitude
    , reciprocal_latitude
    , reciprocal_longitude

FROM 
    apt_apt AS apt
JOIN 
    apt_rwy AS rwy
ON 
    apt.landing_facility_site_number=rwy.landing_facility_site_number
WHERE
    --apt.location_identifier = 'RIC'
    apt.associated_state_name LIKE "%VIR%"
--      GROUP BY 
--	apt.location_identifier
ORDER BY
    apt.location_identifier,rwy.runway_identification
;
--------------------------------------

SELECT 
    A.LOCATION_IDENTIFIER
	, R.BASE_END_IDENTIFIER
	, R.TYPE_OF_AIRCRAFT_ARRESTING_DEVICE
FROM 
    APT_APT AS A
JOIN 
    APT_RWY AS R
ON 
    A.LANDING_FACILITY_SITE_NUMBER=R.LANDING_FACILITY_SITE_NUMBER
WHERE
    A.LOCATION_IDENTIFIER = 'RIC';
GROUP BY 
    A.LOCATION_IDENTIFIER;
;
