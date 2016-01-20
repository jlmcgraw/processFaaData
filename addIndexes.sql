--Airports
DROP INDEX IF EXISTS airports_location_index;
CREATE INDEX airports_location_index
	on APT_APT (apt_latitude, apt_longitude); 

DROP INDEX IF EXISTS airports_identifier_index;
CREATE INDEX airports_identifier_index
	on APT_APT (location_identifier); 

DROP INDEX IF EXISTS landing_facility_site_number_index;
CREATE INDEX landing_facility_site_number_index
	on APT_APT (landing_facility_site_number); 

--Navaids
DROP INDEX IF EXISTS  navaids_location_index;
CREATE INDEX navaids_location_index
	on nav_nav1 (latitude, longitude); 

--Fixes	
DROP INDEX IF EXISTS  fixes_location_index;
CREATE INDEX fixes_location_index
	on fix_fix1 (latitude, longitude); 

--Obstacles	
DROP INDEX IF EXISTS  obstacles_location_index;
CREATE INDEX obstacles_location_index
	on obstacle_obstacle (obstacle_latitude, obstacle_longitude); 

DROP INDEX IF EXISTS  obstacles_height_index;
CREATE INDEX obstacles_height_index
	on obstacle_obstacle (amsl_ht);
	
--Towers
	
DROP INDEX IF EXISTS TWR_TWR1_landing_facility_site_number_index;
CREATE INDEX TWR_TWR1_landing_facility_site_number_index
	on TWR_TWR1 (landing_facility_site_number); 

DROP INDEX IF EXISTS TWR_TWR3a_terminal_communications_facility_identifier_index;
CREATE INDEX TWR_TWR3a_terminal_communications_facility_identifier_index
	on TWR_TWR3a (terminal_communications_facility_identifier); 
	
DROP INDEX IF EXISTS TWR_TWR7_satellite_airport_site_number_index;
CREATE INDEX TWR_TWR7_satellite_airport_site_number_index
	on TWR_TWR7 (satellite_airport_site_number); 	

--AWOS/ASOS	
-- 	awos.landing_facility_site_number_when_station_located_at_airport

