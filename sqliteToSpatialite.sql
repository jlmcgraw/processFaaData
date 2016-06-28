-- PRAGMA foreign_keys=ON;
PRAGMA synchronous=OFF;
-- PRAGMA journal_mode=MEMORY;
-- PRAGMA default_cache_size=10000;
-- PRAGMA locking_mode=EXCLUSIVE;

-- The old way of loading spatialite
-- SELECT load_extension('libspatialite.so');
-- The new way
-- See https://www.gaia-gis.it/fossil/libspatialite/wiki?name=mod_spatialite
SELECT load_extension('mod_spatialite');
SELECT InitSpatialMetadata(1);

/*
--#UPDATE OBSTACLE_OBSTACLE SET geometry = SetSrid(geometry, 4326);
--#SELECT RecoverGeometryColumn('OBSTACLE_OBSTACLE','geometry',4326,'POINT','XY');
--#
--# UPDATE APT_RWY SET geometry = SetSrid(geometry, 4326);
--# SELECT RecoverGeometryColumn('APT_RWY','geometry',4326,'LINESTRING');
--# SELECT ASTEXT(geometry) from APT_RWY;
*/

--#Obstacles
        SELECT AddGeometryColumn( 'OBSTACLE_OBSTACLE' , 'obstacleGeom', 4326, 'POINT', 'XY');
        SELECT CreateSpatialIndex( 'OBSTACLE_OBSTACLE' , 'obstacleGeom' );
        UPDATE OBSTACLE_OBSTACLE
                SET obstacleGeom = MakePoint(
                                CAST (obstacle_longitude AS DOUBLE),
                                CAST (obstacle_latitude AS DOUBLE),
                                4326);

--AIR ROUTE TRAFFIC CONTROL CENTER FACILITIES AND COMMUNICATIONS (AFF)
        SELECT AddGeometryColumn( 'AFF_AFF1' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'AFF_AFF1' , 'geometry'  );
        UPDATE AFF_AFF1
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

        SELECT AddGeometryColumn( 'AFF_AFF3' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'AFF_AFF3' , 'geometry'  );
        UPDATE AFF_AFF3
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--Airports (APT)
        --#The airport reference points
        SELECT AddGeometryColumn( 'APT_APT' , 'referenceGeom' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'APT_APT' , 'referenceGeom' );
        UPDATE APT_APT
                SET referenceGeom = MakePoint(
                                        CAST (apt_longitude AS DOUBLE),
                                        CAST (apt_latitude AS DOUBLE),
                                        4326);

        --#Runway lines
        SELECT AddGeometryColumn( 'APT_RWY' , 'runwayGeom', 4326, 'LINESTRING');
        SELECT CreateSpatialIndex( 'APT_RWY' , 'runwayGeom' );
        UPDATE APT_RWY
                SET runwayGeom = LineFromText('LINESTRING('||base_longitude||' '||base_latitude||','||reciprocal_longitude||' '||reciprocal_latitude||')', 4326)
                        WHERE
                                base_longitude IS NOT '0'
                                AND
                                base_latitude IS NOT '0'
                                AND
                                reciprocal_longitude IS NOT '0'
                                AND
                                reciprocal_latitude IS NOT '0';


        --#Base end points
        SELECT AddGeometryColumn( 'APT_RWY' , 'baseGeom', 4326, 'POINT');
        SELECT CreateSpatialIndex( 'APT_RWY' , 'baseGeom' );
        UPDATE APT_RWY
                SET baseGeom = MakePoint(
                                CAST (base_longitude AS DOUBLE),
                                CAST (base_latitude AS DOUBLE),
                                4326);

        --#Base displaced threshold
        SELECT AddGeometryColumn( 'APT_RWY' , 'baseDisplacedThresholdGeom' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'APT_RWY' , 'baseDisplacedThresholdGeom' );
        UPDATE APT_RWY
                set baseDisplacedThresholdGeom = MakePoint(
                                CAST (base_displaced_threshold_longitude AS DOUBLE),
                                CAST (base_displaced_threshold_latitude AS DOUBLE),
                                4326);


        --#Reciprocal end points
        SELECT AddGeometryColumn( 'APT_RWY' , 'reciprocalGeom' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'APT_RWY' , 'reciprocalGeom' );
        UPDATE APT_RWY
                SET reciprocalGeom = MakePoint(
                                        CAST (reciprocal_longitude AS DOUBLE),
                                        CAST (reciprocal_latitude AS DOUBLE),
                                        4326);

        --#Reciprocal displaced threshold
        SELECT AddGeometryColumn( 'APT_RWY' , 'reciprocalDisplacedThresholdGeom' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'APT_RWY' , 'reciprocalDisplacedThresholdGeom' );
        UPDATE APT_RWY
                SET reciprocalDisplacedThresholdGeom = MakePoint(
                                CAST (reciprocal_displaced_threshold_longitude AS DOUBLE),
                                CAST (reciprocal_displaced_threshold_latitude AS DOUBLE),
                                4326);

--ARB
        SELECT AddGeometryColumn( 'ARB_ARB' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'ARB_ARB' , 'geometry'  );
        UPDATE ARB_ARB
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--ATS
        --ATS2
        SELECT AddGeometryColumn( 'ATS_ATS2' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'ATS_ATS2' , 'geometry'  );
        UPDATE ATS_ATS2
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);
--         --ATS3
--          There aren't any of these yet so I'm commenting them out
--         SELECT AddGeometryColumn( 'ATS_ATS3' , 'geometry' , 4326, 'POINT');
--         SELECT CreateSpatialIndex( 'ATS_ATS3' , 'geometry'  );
--         UPDATE ATS_ATS3
--                 SET geometry = MakePoint(
--                                 CAST (longitude AS DOUBLE),
--                                 CAST (latitude AS DOUBLE),
--                                 4326);
--AWOS Stations
        SELECT AddGeometryColumn( 'AWOS_AWOS1' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'AWOS_AWOS1' , 'geometry'  );
        UPDATE AWOS_AWOS1
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);
--AWY
        --AWY2
        SELECT AddGeometryColumn( 'AWY_AWY2' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'AWY_AWY2' , 'geometry'  );
        UPDATE AWY_AWY2
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);
        --AWY3
        SELECT AddGeometryColumn( 'AWY_AWY3' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'AWY_AWY3' , 'geometry'  );
        UPDATE AWY_AWY3
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--COM
        SELECT AddGeometryColumn( 'COM_COM' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'COM_COM' , 'geometry'  );
        UPDATE COM_COM
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--Fixes (FIX)
        SELECT AddGeometryColumn( 'FIX_FIX1' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'FIX_FIX1' , 'geometry'  );
        UPDATE FIX_FIX1
                set geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--Flight Service Stations (FSS)
        SELECT AddGeometryColumn( 'FSS_FSS' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'FSS_FSS' , 'geometry'  );
        UPDATE FSS_FSS
                set geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--Holding Patterns (HPF)
        SELECT AddGeometryColumn( 'HPF_HP1' , 'fixGeometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'HPF_HP1' , 'fixGeometry'  );
        UPDATE HPF_HP1
                set fixGeometry = MakePoint(
                                CAST (longitude_of_the_associated_fix AS DOUBLE),
                                CAST (latitude_of_the_associated_fix AS DOUBLE),
                                4326);

        SELECT AddGeometryColumn( 'HPF_HP1' , 'navaidGeometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'HPF_HP1' , 'navaidGeometry'  );
        UPDATE HPF_HP1
                set navaidGeometry = MakePoint(
                                CAST (longitude_of_the_associated_navaid AS DOUBLE),
                                CAST (latitude_of_the_associated_navaid AS DOUBLE),
                                4326);

--ILS (ILS)
        SELECT AddGeometryColumn( 'ILS_ILS2' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'ILS_ILS2' , 'geometry'  );
        UPDATE ILS_ILS2
                set geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

        SELECT AddGeometryColumn( 'ILS_ILS3' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'ILS_ILS3' , 'geometry'  );
        UPDATE ILS_ILS3
                set geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

        SELECT AddGeometryColumn( 'ILS_ILS4' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'ILS_ILS4' , 'geometry'  );
        UPDATE ILS_ILS4
                set geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

        SELECT AddGeometryColumn( 'ILS_ILS5' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'ILS_ILS5' , 'geometry'  );
        UPDATE ILS_ILS5
                set geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);
--Military Training Routes (MTR)
        SELECT AddGeometryColumn( 'MTR_MTR5' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'MTR_MTR5' , 'geometry'  );
        UPDATE MTR_MTR5
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--Navaids (NAV)
        SELECT AddGeometryColumn( 'NAV_NAV1' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'NAV_NAV1' , 'geometry'  );
        UPDATE NAV_NAV1
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);
--Parachute Jump Areas (PJA)
        SELECT AddGeometryColumn( 'PJA_PJA1' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'PJA_PJA1' , 'geometry'  );
        UPDATE PJA_PJA1
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--STARs and SIDs (SSD)
        SELECT AddGeometryColumn( 'SSD_SSD' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'SSD_SSD' , 'geometry'  );
        UPDATE SSD_SSD
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--STARs and SIDs (STARDP)
        SELECT AddGeometryColumn( 'STARDP_STARDP' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'STARDP_STARDP' , 'geometry'  );
        UPDATE STARDP_STARDP
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--Towers
        SELECT AddGeometryColumn( 'TWR_TWR1' , 'airport_reference_pointGeometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'TWR_TWR1' , 'airport_reference_pointGeometry'  );
        UPDATE TWR_TWR1
                SET airport_reference_pointGeometry = MakePoint(
                                CAST (airport_reference_point_longitude AS DOUBLE),
                                CAST (airport_reference_point_latitude AS DOUBLE),
                                4326);

        SELECT AddGeometryColumn( 'TWR_TWR1' , 'airport_surveillance_radarGeometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'TWR_TWR1' , 'airport_surveillance_radarGeometry'  );
        UPDATE TWR_TWR1
                SET airport_surveillance_radarGeometry = MakePoint(
                                CAST (airport_surveillance_radar_longitude AS DOUBLE),
                                CAST (airport_surveillance_radar_latitude AS DOUBLE),
                                4326);

        SELECT AddGeometryColumn( 'TWR_TWR1' , 'direction_finding_antennaGeometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'TWR_TWR1' , 'direction_finding_antennaGeometry'  );
        UPDATE TWR_TWR1
                SET direction_finding_antennaGeometry = MakePoint(
                                CAST (longitude_of_direction_finding_antenna AS DOUBLE),
                                CAST (latitude_of_direction_finding_antenna AS DOUBLE),
                                4326);

        SELECT AddGeometryColumn( 'TWR_TWR7' , 'airportGeometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'TWR_TWR7' , 'airportGeometry'  );
        UPDATE TWR_TWR7
                SET airportGeometry = MakePoint(
                                CAST ( airport_longitude AS DOUBLE),
                                CAST ( airport_latitude AS DOUBLE),
                                4326);

--Weather Locations (WXL)
        SELECT AddGeometryColumn( 'WXL_WXL' , 'geometry' , 4326, 'POINT');
        SELECT CreateSpatialIndex( 'WXL_WXL' , 'geometry'  );
        UPDATE WXL_WXL
                SET geometry = MakePoint(
                                CAST (longitude AS DOUBLE),
                                CAST (latitude AS DOUBLE),
                                4326);

--#Create airway lines
--Military training routes
    CREATE TABLE
            MTR_MTRLINES (unique_id
                , route_identifier
                , route_type
                , geometry
                );

    INSERT INTO MTR_MTRLINES
        SELECT
            mtr_mtr5.route_type || mtr_mtr5.route_identifier   AS unique_id
            , mtr_mtr5.route_identifier
            , mtr_mtr5.route_type
            , 'linestring( ' || GROUP_CONCAT(mtr_mtr5.longitude 
                                            || ' ' 
                                            || mtr_mtr5.latitude) 
                    || ' )' AS geometry
        FROM
            mtr_mtr5
        GROUP BY
            unique_id
        ORDER BY
            CAST(mtr_mtr5.record_sort_sequence_number_segment_sequence_number_for_this_po AS REAL)
        ;


    SELECT
        AddGeometryColumn( 'MTR_MTRLINES' , 'airwayGeom' , 4326, 'LINESTRING');

    SELECT
        CreateSpatialIndex( 'MTR_MTRLINES' , 'airwayGeom' );

    UPDATE MTR_MTRLINES
            SET
                airwayGeom = LineFromText(geometry, 4326)
            ;

--Airway segments
    --First create a table
    CREATE TABLE
        AWY_AWYSEGMENTS (airway_designation
            , airway_type
            , airway_point_sequence_number
            , navaid_facility_fix_name
            , point_to_point_minimum_enroute_altitude_mea
            , navaid_facility_fix_latitude
            , navaid_facility_fix_longitude
            , navaid_facility_fix_latitude2
            , navaid_facility_fix_longitude2
            );


    --use a transaction to ensure it all gets done (or nothing is changed)
        -- begin transaction

    --Insert data into that new table with rows consisting of data from two separate tables (AWY1 and AWY2)
    INSERT INTO AWY_AWYSEGMENTS
        SELECT
        awy1.airway_designation
        , awy1.airway_type
        , awy1.airway_point_sequence_number
        , awy2.navaid_facility_fix_name
        , awy1.point_to_point_minimum_enroute_altitude_mea
        , awy2.latitude
        , awy2.longitude
        , awy2a.latitude
        , awy2a.longitude
        FROM
            awy_awy1 AS awy1
        JOIN
            awy_awy2 AS awy2
                ON
                    awy1.airway_point_sequence_number = awy2.airway_point_sequence_number
                    AND
                    awy1.airway_designation = awy2.airway_designation
                    AND
                    awy1.airway_type = awy2.airway_type
        JOIN
            awy_awy2 AS awy2a
                ON
                    CAST (awy1.airway_point_sequence_number AS REAL) + 10 = CAST(awy2a.airway_point_sequence_number  AS REAL)
                    AND
                    awy1.airway_designation = awy2a.airway_designation
                    AND
                    awy1.airway_type = awy2a.airway_type

        WHERE
            -- awy1.airway_designation = 'J1'
            -- AND
            awy2.longitude != ''
                AND
            awy2.latitude != ''
                AND
            awy2.longitude != '0'
                AND
            awy2.latitude != '0'
                AND
            awy2a.longitude != ''
                AND
            awy2a.latitude != ''
                AND
            awy2a.longitude != '0'
                AND
            awy2a.latitude != '0'
        ORDER BY
        awy1.airway_point_sequence_number
            ;
            
    -- Make some lines from the segments
    SELECT
        AddGeometryColumn( 'AWY_AWYSEGMENTS' , 'airwayGeom' , 4326, 'LINESTRING');

    SELECT
        CreateSpatialIndex( 'AWY_AWYSEGMENTS' , 'airwayGeom' );

    UPDATE AWY_AWYSEGMENTS
            SET
                airwayGeom = LineFromText('LINESTRING( '
                                        || navaid_facility_fix_longitude 
                                        ||' '
                                        || navaid_facility_fix_latitude 
                                        ||','
                                        || navaid_facility_fix_longitude2 
                                        ||' '
                                        || navaid_facility_fix_latitude2 
                                        ||' )'
                                        , 4326)
            ;

VACUUM;
