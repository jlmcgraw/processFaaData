DROP INDEX IF EXISTS airports_location_index;
CREATE INDEX airports_location_index
	on APT_APT (apt_latitude, apt_longitude); 

DROP INDEX IF EXISTS airports_identifier_index;
CREATE INDEX airports_identifier_index
	on APT_APT (location_identifier); 
	
DROP INDEX IF EXISTS  navaids_location_index;
CREATE INDEX navaids_location_index
	on nav_nav1 (latitude, longitude); 

DROP INDEX IF EXISTS  fixes_location_index;
CREATE INDEX fixes_location_index
	on fix_fix1 (latitude, longitude); 

DROP INDEX IF EXISTS  obstacles_location_index;
CREATE INDEX obstacles_location_index
	on obstacle_obstacle (obstacle_latitude, obstacle_longitude); 

DROP INDEX IF EXISTS  obstacles_height_index;
CREATE INDEX obstacles_height_index
	on obstacle_obstacle (amsl_ht);