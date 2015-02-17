--------------------------------------
-- --Points in an airway
-- --------------------------------------
SELECT 
  awy1.airway_designation
  , awy1.airway_type
  , awy1.airway_point_sequence_number
  , awy2.navaid_facility_fix_name
  , awy1.point_to_point_minimum_enroute_altitude_mea
  , awy2.navaid_facility_fix_latitude
  , awy2.navaid_facility_fix_longitude
  , awy2a.navaid_facility_fix_latitude
  , awy2a.navaid_facility_fix_longitude
FROM 
  awy_awy1 AS awy1
JOIN 
 awy_awy2 AS awy2
    ON
      awy1.airway_point_sequence_number = awy2.airway_point_sequence_number 
      AND
      awy1.airway_designation = awy2.airway_designation
JOIN 
 awy_awy2 AS awy2a
    ON
      CAST (awy1.airway_point_sequence_number AS REAL) + 10 = CAST(awy2a.airway_point_sequence_number  AS REAL)
      AND
      awy1.airway_designation = awy2a.airway_designation      
      
WHERE
        awy1.airway_designation = 'V38'
ORDER BY
  awy1.airway_point_sequence_number
;


--Points in airway with latitude defined
SELECT 
  awy1.airway_designation
  , awy1.airway_type
  , awy1.airway_point_sequence_number
  , awy2.navaid_facility_fix_name
  , awy1.point_to_point_minimum_enroute_altitude_mea
  , awy2.navaid_facility_fix_latitude
  , awy2.navaid_facility_fix_longitude
FROM 
  awy_awy1 AS awy1
JOIN 
 awy_awy2 AS awy2
    ON
      awy1.airway_point_sequence_number = awy2.airway_point_sequence_number 
      AND
      awy1.airway_designation = awy2.airway_designation
WHERE
        awy1.airway_designation = 'J1'
          AND
        awy2.navaid_facility_fix_latitude != ''
        AND
        awy2.navaid_facility_fix_latitude != '0'
ORDER BY
  awy1.airway_point_sequence_number
  ;

--------------------------------------
--All frequencies for a satellite airport
--------------------------------------
SELECT 
  apt.location_identifier
  , apt.landing_facility_site_number
  , apt.common_traffic_advisory_frequency_ctaf
  , awos.wx_sensor_type
  , awos.station_frequency
  , twr7.satellite_frequency_use
  , twr7.satellite_frequency
  , twr1.radio_call_of_facility_that_furnishes_primary_approach_control
  -- , twr3a.frequency
  -- , twr3a.frequency_use
FROM 
  apt_apt AS apt
JOIN 
  twr_twr7 AS twr7
    ON
      apt.landing_facility_site_number=twr7.satellite_airport_site_number
JOIN
  twr_twr1 AS twr1
    ON
      apt.landing_facility_site_number=twr1.landing_facility_site_number
-- LEFT OUTER JOIN
  -- twr_twr3a AS twr3a
    -- ON
      -- apt.location_identifier=twr3a.terminal_communications_facility_identifier
JOIN
  awos_awos1 as awos
    ON
      apt.landing_facility_site_number=awos.landing_facility_site_number_when_station_located_at_airport
WHERE
        apt.location_identifier = 'EDU'
        AND
        twr7.satellite_frequency != ''
        AND
        CAST(twr7.satellite_frequency AS REAL) < 137
        --apt.associated_state_name LIKE "%VIR%"
ORDER BY
  apt.location_identifier
,twr7.satellite_frequency_use
--,rem.remark_element_name
;

--------------------------------------
--All frequencies for a towered airport
--------------------------------------
SELECT 
  apt.location_identifier
  , apt.landing_facility_site_number
  -- , apt.common_traffic_advisory_frequency_ctaf
  -- , awos.wx_sensor_type
  -- , awos.station_frequency
  , twr1.radio_call_used_by_pilot_to_contact_tower
  , twr1.radio_call_of_facility_that_furnishes_primary_approach_control
  , twr3a.frequency
  , twr3a.frequency_use
FROM 
  apt_apt AS apt
JOIN
  twr_twr1 AS twr1
    ON
      apt.landing_facility_site_number=twr1.landing_facility_site_number
JOIN
  twr_twr3a AS twr3a
    ON
      apt.location_identifier=twr3a.terminal_communications_facility_identifier
JOIN
  awos_awos1 as awos
    ON
      apt.landing_facility_site_number=awos.landing_facility_site_number_when_station_located_at_airport
WHERE
        apt.location_identifier = 'SFO'
        AND
        twr3a.frequency != ''
        AND
        CAST(twr3a.frequency AS REAL) < 137
        --apt.associated_state_name LIKE "%VIR%"
ORDER BY
  apt.location_identifier
,twr3a.frequency_use
--,rem.remark_element_name
;

--------------------------------------
--All remarks for a given airport
      SELECT 
	apt.location_identifier,
	rem.remark_element_name,
	rem.remark_element_name_expanded,
	rem.remark_text
      FROM 
	apt_apt AS apt
      JOIN 
	apt_rmk AS rem
      ON 
	apt.landing_facility_site_number=rem.landing_facility_site_number
      WHERE
        -- apt.location_identifier = 'JYO'
        apt.associated_state_name LIKE "%VIR%"
      ORDER BY
	apt.location_identifier,
	rem.remark_element_name
	;
--------------------------------------
--------------------------------------
--All runways and their lengths for a given airport
      SELECT 
	apt.location_identifier,
	rwy.runway_identification,
	rwy.runway_physical_runway_length_nearest_foot
      FROM 
	apt_apt AS apt
      JOIN 
	apt_rwy AS rwy
      ON 
	apt.landing_facility_site_number=rwy.landing_facility_site_number
      WHERE
        apt.location_identifier = 'EGI'
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
,rwy.base_latitude
,rwy.base_longitude
,rwy.base_runway_end_true_alignment
,rwy.reciprocal_end_identifier
,rwy.reciprocal_latitude
,rwy.reciprocal_longitude
,rwy.reciprocal_runway_end_true_alignment
,rwy.runway_identification
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
	,rwy.runway_identification
	,rwy.base_runway_end_true_alignment
	,rwy.reciprocal_runway_end_true_alignment
	,base_latitude
	,base_longitude
	,reciprocal_latitude
	,reciprocal_longitude

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
	
