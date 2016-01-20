--------------------------------------------------------------------------------
--All frequencies, including remotes, for an airport
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
    apt.location_identifier IN ('SAC', 'VCB','OFP','RIC', 'JYO')

ORDER BY
    apt.location_identifier
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
WHERE
    awy1.airway_designation = 'V38'
ORDER BY
    awy1.airway_point_sequence_number
    ;

-- --------------------------------------------------------------------------------
-- --All frequencies for a satellite airport
-- --------------------------------------------------------------------------------
-- SELECT 
--     apt.location_identifier
--     , apt.landing_facility_site_number
--     , apt.common_traffic_advisory_frequency_ctaf
--     , awos.wx_sensor_type
--     , awos.station_frequency
--     , apt.unicom_frequency_available_at_the_airport
--     , twr7.satellite_frequency_use
--     , twr7.satellite_frequency
--     , twr1.radio_call_of_facility_that_furnishes_primary_approach_control
-- 
-- FROM 
--     apt_apt AS apt
-- 
-- JOIN 
--     twr_twr7 AS twr7
--         ON
--         apt.landing_facility_site_number 
--         = twr7.satellite_airport_site_number
-- 
-- JOIN
--     twr_twr1 AS twr1
--         ON
--         apt.landing_facility_site_number 
--         = twr1.landing_facility_site_number
-- 
-- JOIN
--     awos_awos1 as awos
--         ON
--         apt.landing_facility_site_number
--         = awos.landing_facility_site_number_when_station_located_at_airport
-- 
-- WHERE
--     apt.location_identifier = 'OFP'
--         AND
--     CAST( twr7.satellite_frequency AS REAL) BETWEEN 118 and 137
-- 
-- ORDER BY
--     apt.location_identifier
--     , twr7.satellite_frequency_use
-- ;



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

      SELECT A.LOCATION_IDENTIFIER,R.RUNWAY_END_IDENTIFIER,R.RUNWAY_IDENTIFICATION,R.TYPE_OF_AIRCRAFT_ARRESTING_DEVICE
      FROM APT_APT as A
      JOIN APT_RWY as R

      ON A.LANDING_FACILITY_SITE_NUMBER=R.LANDING_FACILITY_SITE_NUMBER
           WHERE
               A.LOCATION_IDENTIFIER = 'RIC';

GROUP BY A.LOCATION_IDENTIFIER;
             WHERE  
                 D.CHART_CODE = 'IAP'
                AND
                DG.PDF_NAME NOT LIKE '%DELETED%'
                 AND
                D.FAA_CODE LIKE '%CEF%'
                AND
                CAST (DG.yScaleAvgSize AS FLOAT) > 1
                AND
                CAST (DG.xScaleAvgSize as FLOAT) > 1
                AND
                D.MILITARY_USE LIKE 'M'
                AND
                (CAST (DG.targetLonLatRatio AS FLOAT) - CAST(DG.lonLatRatio AS FLOAT)  BETWEEN -.09 AND .09 )
                ORDER BY 
                (CAST (DG.targetLonLatRatio AS FLOAT) - CAST(DG.lonLatRatio AS FLOAT) ) ASC;
                
      SELECT D.PDF_NAME,DG.gcpCount,DG.yScaleAvgSize,DG.xScaleAvgSize
      FROM dtpp as D 
      JOIN dtppGeo as DG 
      ON D.PDF_NAME=DG.PDF_NAME
             WHERE  
                 D.CHART_CODE = 'IAP'            
                 AND
                D.FAA_CODE LIKE '%BIG%';
            
      SELECT D.PDF_NAME,DG.gcpCount,DG.yScaleAvgSize,DG.xScaleAvgSize
      FROM dtpp as D 
      JOIN dtppGeo as DG 
      ON D.PDF_NAME=DG.PDF_NAME
             WHERE  
                 D.CHART_CODE = 'IAP'
                AND
                DG.PDF_NAME NOT LIKE '%DELETED%'
                AND
                D.MILITARY_USE LIKE 'M'
                ;
----------------------

      SELECT D.FAA_CODE,D.PDF_NAME,DG.gcpCount,DG.yMedian,DG.xMedian,DG.upperLeftLon,DG.upperLeftLat,DG.lowerRightLon,DG.lowerRightLat,DG.lonLatRatio
      FROM dtpp as D 
      JOIN dtppGeo as DG 
      ON D.PDF_NAME=DG.PDF_NAME
             WHERE  
                 D.CHART_CODE = 'APD'            
                 AND
                D.FAA_CODE LIKE '%DHN%'
	ORDER BY DG.lonLatRatio;

------------------
SELECT count(*)
      FROM dtpp as D 
      JOIN dtppGeo as DG 
      ON D.PDF_NAME=DG.PDF_NAME
             WHERE  
                 D.CHART_CODE = 'APD'            
                 AND
                 DG.upperLeftLat IS NOT "";
------------------

.headers on
.output ./apd.csv

      SELECT D.STATE_ID,D.FAA_CODE,D.PDF_NAME,DG.airportLatitude,DG.gcpCount,DG.yMedian,DG.xMedian,DG.upperLeftLon,DG.upperLeftLat,DG.lowerRightLon,DG.lowerRightLat,DG.lonLatRatio,DG.notToScaleIndicatorCount
      FROM dtpp as D 
      JOIN dtppGeo as DG 
      ON D.PDF_NAME=DG.PDF_NAME
             WHERE  
                 D.CHART_CODE = 'APD'            
	ORDER BY DG.notToScaleIndicatorCount,DG.lonLatRatio,D.STATE_ID,D.FAA_CODE;

------------------

.headers on
.output ./diagrams.csv

      SELECT *
      FROM dtpp as D 
      JOIN dtppGeo as DG 
      ON D.PDF_NAME=DG.PDF_NAME
             WHERE  
                D.CHART_CODE = 'APD'
		OR
		D.CHART_CODE = 'IAP'
	ORDER BY D.FAA_CODE,D.PDF_NAME;
	
