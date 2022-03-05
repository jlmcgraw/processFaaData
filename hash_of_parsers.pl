AFF => {
    'AFF1' => '
    record_type_indicator:4
    air_route_traffic_control_center_identifier:4
    air_route_traffic_control_center_name:40
    site_location_location_of_the_facility_controlled_by_the_air_ro:30
    cross_reference_alternate_name_for_remote_communications_air_gr:50
    facility_type:5
    information_effective_date_mm_dd_yyyy:10
    site_state_name:30
    site_state_post_office_code:2
    site_latitude_formatted:14
    site_latitude_seconds:11
    site_longitude_formatted:14
    site_longitude_seconds:11
    icao_artcc_id:4
    blank:25
    ',
    'AFF2' => '
    record_type_indicator:4
    air_route_traffic_control_center_identifier:4
    site_location_location_of_the_facility_controlled_by_the_air_ro:30
    facility_type:5
    site_remarks_element_number:4
    site_remarks_text:200
    blank:7
    ',
    'AFF3' => '
    record_type_indicator:4
    air_route_traffic_control_center_identifier:4
    site_location_location_of_the_facility_controlled_by_the_air_ro:30
    facility_type:5
    frequency_associated_with_the_facility:8
    altitude_indication_of_whether_low_high_low_high_and_or_ultra_h:10
    special_usage_name_special_usage_name_for_each_frequency:16
    rcag_frequency_charted_flag:1
    location_identifier_assigned_to_the_landing_facility_airport:4
    associated_state_name:30
    associated_state_post_office_code:2
    associated_city_name:40
    official_airport_name:50
    latitude_of_the_airport_formatted:14
    latitude_of_the_airport_seconds:11
    longitude_of_the_airport_formatted:14
    longitude_of_the_airport_seconds:11
    ',
    'AFF4' => '
    record_type_indicator:4
    air_route_traffic_control_center_identifier:4
    site_location_location_of_the_facility_controlled_by_the_air_ro:30
    facility_type:5
    frequency_associated_with_the_remark:8
    frequency_remark_sequence_number:2
    frequency_remarks_text:200
    blank:1
    '
  },

  #     ANR => {
  #         'ANR1' => '
  #         record_type_indicator:4
  #         origin_facility_location_identifier:5
  #         destination_facility_location_identifier:5
  #         type_of_route_code_anr:3
  #         route_identifier_sequence_number_1_99:2
  #         type_of_route_description_advanced_nav_route:30
  #         advanced_nav_route_area_description:75
  #         advanced_nav_route_altitude_description:40
  #         aircraft_allowed_limitations_description:35
  #         effective_hours_gmt_description_1:15
  #         effective_hours_gmt_description_2:15
  #         effective_hours_gmt_description_3:15
  #         route_direction_limitations_description:20
  #         ',
  #         'ANR2' => '
  #         record_type_indicator:4
  #         origin_facility_location_identifier:5
  #         destination_facility_location_identifier:5
  #         type_of_route_code_anr:3
  #         route_identifier_sequence_number_1_99:2
  #         segment_sequence_number_within_the_route:3
  #         segment_identifier_navaid_ident_awy_number_fix_name_sid_name_st:15
  #         segment_type_described:7
  #         fix_state_code_post_office_alpha_code:2
  #         icao_region_code_for_fix:2
  #         navaid_facility_type_code:2
  #         navaid_facility_type_described:20
  #         radial_and_distance_from_navaid:7
  #         blank:187
  #     ',
  #     },
  APT => {
    'ATT' => '
    record_type_indicator:3
    landing_facility_site_number:11
    landing_facility_state_post_office_code:2
    attendance_schedule_sequence_number:2
    airport_attendance_schedule:108
    attendance_schedule_record_filler:1405
    ',

    'APT' => '
    record_type_indicator:3
    landing_facility_site_number:11
    landing_facility_type:13
    location_identifier:4
    information_effective_date_mm_dd_yyyy:10
    faa_region_code:3
    faa_district_or_field_office_code:4
    associated_state_post_office_code:2
    associated_state_name:20
    associated_county_or_parish_name:21
    associated_countys_state_post_office_code:2
    associated_city_name:40
    official_facility_name:50
    airport_ownership_type:2
    facility_use:2
    facility_owners_name:35
    owners_address:72
    owners_city_state_and_zip_code:45
    owners_phone_number:16
    facility_managers_name:35
    managers_address:72
    managers_city_state_and_zip_code:45
    managers_phone_number:16
    airport_reference_point_latitude_formatted:15
    airport_reference_point_latitude_seconds:12
    airport_reference_point_longitude_formatted:15
    airport_reference_point_longitude_seconds:12
    airport_reference_point_determination_method:1
    airport_elevation_nearest_tenth_of_a_foot_msl:7
    airport_elevation_determination_method:1
    magnetic_variation_and_direction:3
    magnetic_variation_epoch_year:4
    traffic_pattern_altitude_whole_feet_agl:4
    aeronautical_sectional_chart_on_which_facility_appears:30
    distance_from_central_business_district_of_the_associated_city_to_the_airport:2
    direction_of_airport_from_central_business_district_of_associated_city:3
    land_area_covered_by_airport_acres:5
    boundary_artcc_identifier:4
    boundary_artcc_faa_computer_identifier:3
    boundary_artcc_name:30
    responsible_artcc_identifier:4
    responsible_artcc_faa_computer_identifier:3
    responsible_artcc_name:30
    tie_in_fss_physically_located_on_facility:1
    tie_in_flight_service_station_fss_identifier:4
    tie_in_fss_name:30
    local_phone_number_from_airport_to_fss_for_administrative_services:16
    toll_free_phone_number_from_airport_to_fss_for_pilot_briefing_services:16
    alternate_fss_identifier:4
    alternate_fss_name:30
    toll_free_phone_number_from_airport_to_alternate_fss_for_pilot_briefing_services:16
    identifier_of_the_facility_responsible_for_issuing_notices_to_airmen_notams_and_weather_information_for_the_airport:4
    availability_of_notam_d_service_at_airport:1
    airport_activation_date_mm_yyyy:7
    airport_status_code:2
    airport_arff_certification_type_and_date:15
    npias_federal_agreements_code:7
    airport_airspace_analysis_determination:13
    facility_has_been_designated_by_the_us_treasury_as_an_international_airport_of_entry_for_customs:1
    facility_has_been_designated_by_the_us_treasury_as_a_customs_landing_rights_airport:1
    facility_has_military_civil_joint_use_agreement:1
    airport_has_entered_into_an_agreement_that_grants_landing_rights_to_the_military:1
    airport_inspection_method:2
    agency_group_performing_physical_inspection:1
    last_physical_inspection_date_mmddyyyy:8
    last_date_information_request_was_completed_by_facility_owner_or_manager_mmddyyyy:8
    fuel_types_available_for_public_use_at_the_airport:40
    airframe_repair_service_availability_type:5
    power_plant_engine_repair_availability_type:5
    type_of_bottled_oxygen_available:8
    type_of_bulk_oxygen_available:8
    airport_lighting_schedule:7
    beacon_lighting_schedule:7
    air_traffic_control_tower_located_on_airport:1
    unicom_frequency_available_at_the_airport:7
    common_traffic_advisory_frequency_ctaf:7
    segmented_circle_airport_marker_system_on_the_airport:4
    lens_color_of_operable_beacon_located_on_the_airport:3
    landing_fee_charged_to_non_commercial_users_of_airport:1
    landing_facility_is_used_for_medical_purposes:1
    single_engine_general_aviation_aircraft:3
    multi_engine_general_aviation_aircraft:3
    jet_engine_general_aviation_aircraft:3
    general_aviation_helicopter:3
    operational_gliders:3
    operational_military_aircraft_including_helicopters:3
    ultralight_aircraft:3
    commercial_services:6
    commuter_services:6
    air_taxi:6
    general_aviation_local_operations:6
    general_aviation_itinerant_operations:6
    military_aircraft_operations:6
    ending_date_on_which_annual_operations_data_is_based_mm_dd_yyyy:10
    airport_position_source:16
    airport_position_source_date_mm_dd_yyyy:10
    airport_elevation_source:16
    airport_elevation_source_date_mm_dd_yyyy:10
    contract_fuel_available:1
    transient_storage_facilities:12
    other_airport_services_available:71
    wind_indicator:3
    icao_identifier:7
    minimum_operational_network:1
    airport_record_filler_blank:313
    ',
    'RWY' => '
    record_type_indicator:3
    landing_facility_site_number:11
    runway_state_post_office_code:2
    runway_identification:7
    runway_physical_runway_length_nearest_foot:5
    runway_physical_runway_width_nearest_foot:4
    runway_surface_type_and_condition:12
    runway_surface_treatment:5
    runway_pavement_classification_number_pcn:11
    runway_lights_edge_intensity:5
    base_end_identifier:3
    base_runway_end_true_alignment:3
    base_instrument_landing_system_ils_type:10
    base_right_hand_traffic_pattern_for_landing_aircraft:1
    base_runway_markings_type:5
    base_runway_markings_condition:1
    base_latitude_of_physical_runway_end_formatted:15
    base_latitude_of_physical_runway_end_seconds:12
    base_longitude_of_physical_runway_end_formatted:15
    base_longitude_of_physical_runway_end_seconds:12
    base_elevation_feet_msl_at_physical_runway_end:7
    base_threshold_crossing_height_feet_agl:3
    base_visual_glide_path_angle_hundredths_of_degrees:4
    base_latitude_at_displaced_threshold_formatted:15
    base_latitude_at_displaced_threshold_seconds:12
    base_longitude_at_displaced_threshold_formatted:15
    base_longitude_at_displaced_threshold_seconds:12
    base_elevation_at_displaced_threshold_feet_msl:7
    base_displaced_threshold_length_in_feet_from_runway_end:4
    base_elevation_at_touchdown_zone_feet_msl:7
    base_visual_glide_slope_indicators:5
    base_runway_visual_range_equipment_rvr:3
    base_runway_visibility_value_equipment_rvv:1
    base_approach_light_system:8
    base_runway_end_identifier_lights_reil_availability:1
    base_runway_centerline_lights_availability:1
    base_runway_end_touchdown_lights_availability:1
    base_controlling_object_description:11
    base_controlling_object_marked_lighted:4
    base_faa_cfr_part_77_objects_affecting_navigable_airspace_runway_category:5
    base_controlling_object_clearance_slope:2
    base_controlling_object_height_above_runway:5
    base_controlling_object_distance_from_runway_end:5
    base_controlling_object_centerline_offset:7
    reciprocal_end_identifier:3
    reciprocal_runway_end_true_alignment:3
    reciprocal_instrument_landing_system_ils_type:10
    reciprocal_right_hand_traffic_pattern_for_landing_aircraft:1
    reciprocal_runway_markings_type:5
    reciprocal_runway_markings_condition:1
    reciprocal_latitude_of_physical_runway_end_formatted:15
    reciprocal_latitude_of_physical_runway_end_seconds:12
    reciprocal_longitude_of_physical_runway_end_formatted:15
    reciprocal_longitude_of_physical_runway_end_seconds:12
    reciprocal_elevation_feet_msl_at_physical_runway_end:7
    reciprocal_threshold_crossing_height_feet_agl:3
    reciprocal_visual_glide_path_angle_hundredths_of_degrees:4
    reciprocal_latitude_at_displaced_threshold_formatted:15
    reciprocal_latitude_at_displaced_threshold_seconds:12
    reciprocal_longitude_at_displaced_threshold_formatted:15
    reciprocal_longitude_at_displaced_threshold_seconds:12
    reciprocal_elevation_at_displaced_threshold_feet_msl:7
    reciprocal_displaced_threshold_length_in_feet_from_runway_end:4
    reciprocal_elevation_at_touchdown_zone_feet_msl:7
    reciprocal_approach_slope_indicator_equipment:5
    reciprocal_runway_visual_range_equipment_rvr:3
    reciprocal_runway_visibility_value_equipment_rvv:1
    reciprocal_approach_light_system:8
    reciprocal_runway_end_identifier_lights_reil_availability:1
    reciprocal_runway_centerline_lights_availability:1
    reciprocal_runway_end_touchdown_lights_availability:1
    reciprocal_controlling_object_description:11
    reciprocal_controlling_object_marked_lighted:4
    reciprocal_faa_cfr_part_77_objects_affecting_navigable_airspace_runway_category:5
    reciprocal_controlling_object_clearance_slope:2
    reciprocal_controlling_object_height_above_runway:5
    reciprocal_controlling_object_distance_from_runway_end:5
    reciprocal_controlling_object_centerline_offset:7
    runway_length_source:16
    runway_length_source_date_mm_dd_yyyy:10
    runway_weight_bearing_capacity_for_single_wheel:6
    runway_weight_bearing_capacity_for_dual_wheel:6
    runway_weight_bearing_capacity_for_two_dual_wheels_in_tandem:6
    runway_weight_bearing_capacity_for_two_dual_wheels_in_double_tandem:6
    base_runway_end_gradient:5
    base_runway_end_gradient_direction_up_or_down:4
    base_runway_end_position_source:16
    base_runway_end_position_source_date_mm_dd_yyyy:10
    base_runway_end_elevation_source:16
    base_runway_end_elevation_source_date_mm_dd_yyyy:10
    base_displaced_theshold_position_source:16
    base_displaced_theshold_position_source_date_mm_dd_yyyy:10
    base_displaced_theshold_elevation_source:16
    base_displaced_theshold_elevation_source_date_mm_dd_yyyy:10
    base_touchdown_zone_elevation_source:16
    base_touchdown_zone_elevation_source_date_mm_dd_yyyy:10
    base_takeoff_run_available_tora_in_feet:5
    base_takeoff_distance_available_toda_in_feet:5
    base_aclt_stop_distance_available_asda_in_feet:5
    base_landing_distance_available_lda_in_feet:5
    base_available_landing_distance_for_land_and_hold_short_operations_lahso:5
    base_id_of_intersecting_runway_defining_hold_short_point:7
    base_description_of_entity_defining_hold_short_point:40
    base_latitude_of_lahso_hold_short_point_formatted:15
    base_latitude_of_lahso_hold_short_point_seconds:12
    base_longitude_of_lahso_hold_short_point_formatted:15
    base_longitude_of_lahso_hold_short_point_seconds:12
    base_lahso_hold_short_point_lat_long_source:16
    base_hold_short_point_lat_long_source_date_mm_dd_yyyy:10
    reciprocal_runway_end_gradient:5
    reciprocal_runway_end_gradient_direction_up_or_down:4
    reciprocal_runway_end_position_source:16
    reciprocal_runway_end_position_source_date_mm_dd_yyyy:10
    reciprocal_runway_end_elevation_source:16
    reciprocal_runway_end_elevation_source_date_mm_dd_yyyy:10
    reciprocal_displaced_theshold_position_source:16
    reciprocal_displaced_theshold_position_source_date_mm_dd_yyyy:10
    reciprocal_displaced_theshold_elevation_source:16
    reciprocal_displaced_theshold_elevation_source_date_mm_dd_yyyy:10
    reciprocal_touchdown_zone_elevation_source:16
    reciprocal_touchdown_zone_elevation_source_date_mm_dd_yyyy:10
    reciprocal_takeoff_run_available_tora_in_feet:5
    reciprocal_takeoff_distance_available_toda_in_feet:5
    reciprocal_aclt_stop_distance_available_asda_in_feet:5
    reciprocal_landing_distance_available_lda_in_feet:5
    reciprocal_available_landing_distance_for_land_and_hold_short_operations_lahso:5
    reciprocal_id_of_intersecting_runway_defining_hold_short_point:7
    reciprocal_description_of_entity_defining_hold_short_point:40
    reciprocal_latitude_of_lahso_hold_short_point_formatted:15
    reciprocal_latitude_of_lahso_hold_short_point_seconds:12
    reciprocal_longitude_of_lahso_hold_short_point_formatted:15
    reciprocal_longitude_of_lahso_hold_short_point_seconds:12
    reciprocal_lahso_hold_short_point_lat_long_source:16
    reciprocal_hold_short_point_lat_long_source_date_mm_dd_yyyy:10
    runway_record_filler_blank:390
    ',

    'ARS' => '
    record_type_indicator:3
    landing_facility_site_number:11
    landing_facility_state_post_office_code:2
    runway_identification:7
    runway_end_identifier:3
    type_of_aircraft_arresting_device:9
    arresting_system_record_filler_blank:1496
    ',
    'RMK' => '
    record_type_indicator:3
    landing_facility_site_number:11
    landing_facility_state_post_office_code:2
    remark_element_name:15
    remark_text:1500
    '
  },
  ARB => {
    'ARB' => '
    record_identifier_artcc_identifier_altitude_structure_code_five:12
    center_name:40
    altitude_structure_decode_name:10
    latitude_of_the_boundary_point:14
    longitude_of_the_boundary_point:14
    description_of_boundary_line_connecting_points_on_the_boundary:300
    six_digit_number_used_to_maintain_proper_sequence_of_boundary_s:6
    an_x_in_this_field_indicates_this_point_is_used_only_in_the_nas:1
    '
  },
  ATS => {
    'ATS1' => '
    record_type_indicator:4
    ats_airway_designation:2
    ats_airway_id:12
    rnav_indicator:1
    airway_type:1
    airway_point_sequence_number:5
    chart_publication_effective_date_mm_dd_yyyy:10
    track_angle_outbound_rnav_format_nnn_nnn:7
    distance_to_changeover_point_rnav_format_nnnnn:5
    track_angle_inbound_rnav_format_nnn_nnn:7
    distance_to_next_point_in_nautical_miles:6
    bearing_reserved_presently_000_00_entered:6
    segment_magnetic_course_format:6
    segment_magnetic_course_opposite_direction:6
    distance_to_next_point_in_segment_in_nautical_miles:6
    point_to_point_minimum_enroute_altitude_mea:5
    point_to_point_minimum_enroute_direction_mea:7
    point_to_point_minimum_enroute_altitude_mea_opposite_direction:5
    point_to_point_minimum_enroute_direction_mea_opposite_direction:7
    point_to_point_maximum_authorized_altitude:5
    point_to_point_minimum_obstruction_clearance_altitude_moca:5
    airway_gap_flag_indicator_x_entered_when_airway_discontinued:1
    distance_from_this_point_to_the_changeover_point_for_the_next_n:3
    minimum_crossing_altitude_format_nnnnn:5
    direction_of_crossing_format_aaaaaaa:7
    minimum_crossing_altitude_opposite_direction:5
    direction_of_crossing_opposite_direction:7
    gap_in_signal_coverage_indicator:1
    us_airspace_only_indicator:1
    navaid_magnetic_variation:5
    navaid_fix_artcc:3
    reserved_to_point_part95:40
    reserved_next_mea_point_part95:50
    point_to_point_minimum_enroute_altitude:5
    point_to_point_minimum_enroute_direction:7
    point_to_point_minimum_enroute_altitude_opposite_direction:5
    point_to_point_minimum_enroute_direction_opposite_direction:7
    minimum_crossing_altitude_mca_point:50
    point_to_point_dme_dme_iru_minimum_enroute_altitude:5
    point_to_point_dme_dme_iru_minimum_enroute_direction:6
    point_to_point_dme_dme_iru_minimum_enroute_altitude_opposite_direction:5
    point_to_point_dme_dme_iru_minimum_enroute_direction_opposite_direction:6
    dogleg:1
    rnp_format:5
    record_sort_sequence_number:7
    ',
    'ATS2' => '
    record_type_indicator:4
    ats_airway_designation:2
    ats_airway_id:12
    rnav_indicator:1
    ats_airway_type:1
    airway_point_sequence_number:5
    navaid_facility_fix_name:40
    navaid_facility_fix_type:25
    fix_type_publication_category:15
    navaid_facility_fix_state_po_code:2
    icao_region_code_for_fix:2
    navaid_facility_fix_latitude:14
    navaid_facility_fix_longitude:14
    fix_minimum_reception_altitude_mra:5
    navaid_identifier:4
    reserved_from_point_part95:57
    blanks:145
    record_sort_sequence_number:7
    ',
    'ATS3' => '
    record_type_indicator:4
    ats_airway_designation:2
    ats_airway_id:12
    rnav_indicator:1
    ats_airway_type:1
    airway_point_sequence_number:5
    navaid_facility_name:30
    navaid_facility_type:25
    navaid_facility_state_p_o_code:2
    navaid_facility_latitude:14
    navaid_facility_longitude:14
    blanks:238
    record_sort_sequence_number:7
    ',
    'ATS4' => '
    record_type_indicator:4
    ats_airway_designation:2
    ats_airway_id:12
    rnav_indicator:1
    ats_airway_type:1
    airway_point_sequence_number:5
    remarks_text:200
    blanks:123
    record_sort_sequence_number:7
    ',
    'ATS5' => '
    record_type_indicator:4
    ats_airway_designation:2
    ats_airway_id:12
    rnav_indicator:1
    airway_type:1
    airway_point_sequence_number:5
    remarks_text:200
    blanks:123
    record_sort_sequence_number:7
    ',
    'RMK' => '
    record_type_indicator:4
    ats_airway_designation:2
    ats_airway_id:12
    rnav_indicator:1
    airway_type:1
    remark_sequence_number:3
    remark_reference:5
    remarks_text:200
    blanks:120
    record_sort_sequence_number:7
    ',
  },
  AWOS => {
    'AWOS1' => '
    record_type_indicator:5
    wx_sensor_ident:4
    wx_sensor_type:10
    commissioning_status:1
    commissioning_decommissioning_date_mm_dd_yyyy:10
    navaid_flag_wx_sensor_associated_with_navaid:1
    station_latitude_dd_mm_ss_ssssh:14
    station_longitude_ddd_mm_ss_ssssh:15
    elevation:7
    survey_method_code:1
    station_frequency:7
    second_station_frequency:7
    station_telephone_number:14
    second_station_telephone_number:14
    landing_facility_site_number_when_station_located_at_airport:11
    station_city:40
    station_state_post_office_code:2
    information_effective_date_mm_dd_yyyy:10
    blanks_filler:82
    ',
    'AWOS2' => '
    record_type_indicator:5
    wx_sensor_ident:4
    wx_sensor_type:10
    asos_awos_remarks_free_form_text:236
    ',
  },
  AWY => {
    'AWY1' => '
    record_type_indicator:4
    airway_designation:5
    airway_type:1
    airway_point_sequence_number:5
    chart_publication_effective_date_mm_dd_yyyy:10
    track_angle_outbound_rnav_format_nnn_nnn:7
    distance_to_changeover_point_rnav_format_nnnnn:5
    track_angle_inbound_rnav_format_nnn_nnn:7
    distance_to_next_point_in_nautical_miles:6
    bearing_reserved_presently_000_00_entered:6
    segment_magnetic_course:6
    segment_magnetic_course_opposite_direction:6
    distance_to_next_point_in_segment_in_nautical_miles:6
    point_to_point_minimum_enroute_altitude_mea:5
    point_to_point_minimum_enroute_direction_mea:6
    point_to_point_minimum_enroute_altitude_mea_opposite_direction:5
    point_to_point_minimum_enroute_direction_mea_opposite_direction:6
    point_to_point_maximum_authorized_altitude:5
    point_to_point_minimum_obstruction_clearance_altitude_moca:5
    airway_gap_flag_indicator:1
    distance_from_this_point_to_the_changeover_point_for_the_next_n:3
    minimum_crossing_altitude_format_nnnnn:5
    direction_of_crossing_format_aaaaaaa:7
    minimum_crossing_altitude_opposite_direction:5
    direction_of_crossing_opposite_direction:7
    gap_in_signal_coverage_indicator:1
    us_airspace_only_indicator:1
    navaid_magnetic_variation:5
    navaid_fix_artcc:3
    reserved_to_point_part95:33
    reserved_next_mea_point_part95:40
    point_to_point_minimum_enroute_altitude:5
    point_to_point_minimum_enroute_direction:6
    point_to_point_minimum_enroute_altitude_opposite_direction:5
    point_to_point_minimum_enroute_direction_opposite_direction:6
    minimum_crossing_altitude_mca_point:40
    point_to_point_dme_dme_iru_minimum_enroute_altitude:5
    point_to_point_dme_dme_iru_minimum_enroute_direction:6
    point_to_point_dme_dme_iru_minimum_enroute_altitude_opposite_direction:5
    point_to_point_dme_dme_iru_minimum_enroute_direction_opposite_direction:6
    dogleg:1
    rnp_format:5
    record_sort_sequence_number:7
    ',
    'AWY2' => '
        record_type_indicator:4
    airway_designation:5
    airway_type:1
    airway_point_sequence_number:5
    navaid_facility_fix_name:30
    navaid_facility_fix_type:19
    fix_type_publication_category:15
    navaid_facility_fix_state_p_o_code:2
    icao_region_code_for_fix:2
    navaid_facility_fix_latitude:14
    navaid_facility_fix_longitude:14
    fix_minimum_reception_altitude_mra:5
    navaid_identifier:4
    reserved_from_point_part95:40
    blanks:147
    record_sort_sequence_number:7
    ',
    'AWY3' => '
        record_type_indicator:4
    airway_designation:5
    airway_type:1
    airway_point_sequence_number:5
    navaid_facility_name:30
    navaid_facility_type:19
    navaid_facility_state_p_o_code:2
    navaid_facility_latitude:14
    navaid_facility_longitude:14
    blanks:213
    record_sort_sequence_number:7
    ',
    'AWY4' => '
        record_type_indicator:4
    airway_designation:5
    airway_type:1
    airway_point_sequence_number:5
    remarks_text:202
    blanks:90
    record_sort_sequence_number:7
    ',
    'AWY5' => '
        record_type_indicator:4
    airway_designation:5
    airway_type:1
    airway_point_sequence_number:5
    remarks_text:202
    blanks:90
    record_sort_sequence_number:7
    ',
    'RMK' => '
        record_type_indicator:4
    airway_designation:5
    airway_type:1
    remark_sequence_number:3
    remark_reference:6
    remarks_text:220
    blanks:68
    record_sort_sequence_number:7
    ',
  },

  # cdr    => {},

  COM => {

    #     communications_outlet_frequencies_in_16_fields_of_9_characters:144
    #     operational_hours_in_3_fields_of_20_characters_each:60
    #     charts_in_4_fields_of_2_characters_each:8
    'COM' => '
    communications_outlet_ident:4
    communications_outlet_type:7
    associated_navaid_ident:4
    associated_navaid_type:2
    associated_navaid_city:26
    associated_navaid_state:20
    associated_navaid_name:26
    associated_navaid_latitude:14
    associated_navaid_longitude:14
    communications_outlet_city:26
    communications_outlet_state:20
    communications_outlet_region_name:20
    communications_outlet_region_code:3
    communications_outlet_latitude:14
    communications_outlet_longitude:14
    communications_outlet_call:26
    communications_outlet_frequencies_1:9
    communications_outlet_frequencies_2:9
    communications_outlet_frequencies_3:9
    communications_outlet_frequencies_4:9
    communications_outlet_frequencies_5:9
    communications_outlet_frequencies_6:9
    communications_outlet_frequencies_7:9
    communications_outlet_frequencies_8:9
    communications_outlet_frequencies_9:9
    communications_outlet_frequencies_10:9
    communications_outlet_frequencies_11:9
    communications_outlet_frequencies_12:9
    communications_outlet_frequencies_13:9
    communications_outlet_frequencies_14:9
    communications_outlet_frequencies_15:9
    communications_outlet_frequencies_16:9
    fss_ident:4
    fss_ident_fss_name:30
    alternate_fss_ident:4
    alternate_fss_ident_alternate_fss_name:30
    operational_hours_1:20
    operational_hours_2:20
    operational_hours_3:20
    owner_code:1
    owner_name:69
    operator_code:1
    operator_name:69
    charts_1:2
    charts_2:2
    charts_3:2
    charts_4:2
    standard_time_zone:2
    status:20
    status_date:11
    '
  },
  FIX => {
    'FIX1' => '
    record_type_indicator:4
    record_identifier_fix_id:30
    record_identifier_fix_state_name:30
    icao_region_code:2
    geographical_latitude_of_the_fix:14
    geographical_longitude_of_the_fix:14
    categorizes_the_fix_as_a_military_mil_or_civil_fix_fix:3
    ident_facility_type_direction_or_course_of_mls_co:22
    airport_id_approach_end_rwy_distance_of_radar_component_used_in:22
    previous_name_of_the_fix_before_it_was_renamed:33
    charting_information:38
    fix_to_be_published:1
    fix_use:15
    national_airspace_system_nas_identifier_for_the_fix_usually_5_c:5
    denotes_high_artcc_area_of_jurisdiction:4
    denotes_low_artcc_area_of_jurisdiction:4
    fix_country_name_outside_conus:30
    pitch:1
    catch:1
    sua_atcaa:1
    blanks:192
    ',
    'FIX2' => '
    record_type_indicator:4
    record_identifier_fix_name:30
    record_identifier_fix_state_name:30
    icao_region_code:2
    location_identifier_facility_type_and_radial_or_bearing_dme_dis:23
    blanks:377
    ',
    'FIX3' => '
    record_type_indicator:4
    record_identifier_fix_name:30
    record_identifier_fix_state_name:30
    icao_region_code:2
    ident_facility_type_direction_or_course_of_ils_co:23
    blanks:377
    ',
    'FIX4' => '
    record_type_indicator:4
    record_identifier_fix_name:30
    record_identifier_fix_state_name:30
    icao_region_code:2
    field_label:100
    remark_text:300
    ',
    'FIX5' => '
    record_type_indicator:4
    record_identifier_fix_name:30
    record_identifier_fix_state_name:30
    icao_region_code:2
    chart_on_which_fix_is_to_be_depicted:22
    blanks:378
    ',
  },
  FSS => {
    'FSS' => '
        record_identifier_the_flight_service_stations_location_ident:4
    name_of_fss:26
    fss_update_date_last_date_on_which_the_record_was_updated:11
    site_number_of_associated_airport:11
    airport_name_fss_on_arpt:50
    airport_associated_city_fss_on_arpt:26
    airport_associated_state_fss_on_arpt:20
    airport_latitude_fss_on_arpt:14
    airport_longitude_fss_on_arpt:14
    facility_types:8
    facility_identifier_fss_name_and_fss_voice_call:26
    fss_owner_code:1
    fss_owner_name:69
    fss_operator_code:1
    fss_operator_name:69
    primary_fss_frequencies_used_60_occurences_of_40_characters_eac:2400
    fss_hours_of_operation:100
    status_of_facility:20
    name_of_fss_with_circuit_b_teletype_capable_of_transmitting_rec:3
    communications_outlet_identification_40_occurences_of_14_charac:560
    navaid_identifier_and_navaid_facility_type_of_associated_navaid:525
    reserved_was_f23_9_27_83:20
    availability_of_weather_radar:1
    enroute_flight_advisory_service_efas_available_also_called_flig:10
    flight_watch_hours_of_operation:50
    city_when_fss_is_not_on_airport_see_f6:26
    state_when_fss_is_not_on_airport_see_f6:20
    latitude_when_fss_is_not_on_airport_see_f6:14
    longitude_when_fss_is_not_on_airport_see_f6:14
    region_when_fss_is_not_on_airport_see_f6:3
    reserved1:3
    airport_advisory_frequencies_20_occurences_of_6_characters_each:120
    frequency_on_which_volmet_meteorological_broadcasts_are_transmi:120
    volmet_schedule_of_operation_20_occurences_of_12_characters_eac:240
    type_of_direction_finding_df_equipment:30
    latitude_of_direction_finding_df_equipment:14
    longitude_of_direction_finding_df_equipment:14
    low_altitude_enroute_chart_number_that_the_fss_is_located_on_20:40
    telephone_number_used_to_reach_fss:12
    reserved2:50
    flight_service_station_remarks:1050
    city_facility_located_in_not_used_when_colocated_with_navaid:780
    state_facility_located_in_not_used_when_colocated_with_navaid:600
    geographical_latitude_of_communication_facility_not_used_when_c:420
    geographical_longitude_of_communication_facility_not_used_when:420
    facility_owner_code:40
    facility_owner_name:2760
    facility_operator_code:40
    facility_operator_name:2760
    fss_associated_with_the_communication_facility:160
    frequencies_used_by_the_communication_facility:540
    operational_hours_of_the_communication_facility_local_time:800
    status_of_communication_facility:800
    communication_facility_status_date:330
    navaid_identifier_fact_type_when_colocated:140
    low_alitude_enroute_chart_that_the_communication_facility_is_lo:60
    standard_time_zone_that_the_communication_is_located_in:40
    communication_remarks:1050
    date_information_extracted:11
    ',

    #bug todo: continuation records not implemented
    '*' => '                     ',
  },

  #HARFIX => {
  #  'HARFIX' => '
  #      fix_navaid_id:77
  #  blank_character_separating_fields1:1
  #  fix_navaid_latitude_format_ddmmss_ssssx:12
  #  blank_character_separating_fields2:1
  #  fix_navaid_longitude_format_dddmmss_ssssx:13
  #  blank_character_separating_fields3:1
  #  type:1
  #  blank_character_separating_fields4:1
  #  class_of_navaid:11
  #  blank_character_separating_fields5:1
  #  pitch_point:1
  #  blank_character_separating_fields6:1
  #  catch_point:1
  #  blank_character_separating_fields7:1
  #  sua_atcaa_waypoint:1
  #  ',
  #
  #  },
  HPF => {
    'HP1' => '
        record_type_indicator:4
    holding_pattern_name:80
    pattern_number_to_uniquely_identify_holding_pattern:3
    holding_pattern_effective_date:11
    direction_of_holding_on_the_navaid_or_fix:3
    magnetic_bearing_or_radial_degrees_of_holding:3
    azimuth_degrees_shown_above_is_a_radial_course_bearing_or_rnav:5
    identifier_of_ils_facility_used_to_provide_course_for_holding:7
    identifier_of_navaid_facility_used_to_provide_radial_or_bearing:7
    additional_facility_used_in_holding_pattern_make_up:12
    inbound_course:3
    turning_direction:3
    holding_altitudes_for_all_aircraft:7
    holding_alt_170_175_kt:7
    holding_alt_200_230_kt:7
    holding_alt_265_kt:7
    holding_alt_280_kt:7
    holding_alt_310_kt:7
    fix_with_which_holding_is_associated:36
    artcc_associated_with_fix:3
    latitude_of_the_associated_fix_dd_mm_ss_sssn:14
    longitude_of_the_associated_fix_ddd_mm_ss_sssw:14
    high_route_artcc_associated_with_navaid:3
    low_route_artcc_associated_with_navaid:3
    latitude_of_the_associated_navaid_dd_mm_ss_sssn:14
    longitude_of_the_associated_navaid_ddd_mm_ss_sssw:14
    leg_length_outbound_two_subfields_separated_by_a_slash_time_min:8
    blanks:195
    ',
    'HP2' => '
        record_type_indicator:4
    holding_pattern_name:80
    pattern_number_to_uniquely_identify_holding_pattern:3
    charting_description:22
    blanks:378
    ',
    'HP3' => '
        record_type_indicator:4
    holding_pattern_name:80
    pattern_number_to_uniquely_identify_holding_pattern:3
    holding_altitudes_speeds_other_than_ones_shown_in_hp1_record:15
    blanks:385
    ',
    'HP4' => '
        record_type_indicator:4
    holding_pattern_name:80
    pattern_number_to_uniquely_identify_holding_pattern:3
    field_label:100
    descriptive_remarks:300
    ',
  },
  ILS => {
    'ILS1' => '
        record_type_indicator:4
    airport_site_number_identifier:11
    ils_runway_end_identifier:3
    ils_system_type:10
    identification_code_of_ils:6
    information_effective_date_mm_dd_yyyy:10
    airport_name:50
    associated_city:40
    two_letter_post_office_code_for_the_state:2
    state_name:20
    faa_region_code:3
    airport_identifier:4
    ils_runway_length_in_whole_feet:5
    ils_runway_width_in_whole_feet:4
    category_of_the_ils_i_ii_iiia:9
    name_of_owner_of_the_facility:50
    name_of_the_ils_facility_operator:50
    ils_approach_bearing_in_degrees_magnetic:6
    the_magnetic_variation_at_the_ils_facility:3
    blank:88
    ',
    'ILS2' => '
        record_type_indicator:4
    airport_site_number_identifier:11
    ils_runway_end_identifier:3
    ils_system_type:10
    operational_status_of_localizer:22
    effective_date_of_localizer_operational_status:10
    latitude_of_localizer_antenna_formatted:14
    latitude_of_localizer_antenna_all_seconds:11
    longitude_of_localizer_antenna_formatted:14
    longitude_of_localizer_antenna_all_seconds:11
    code_indicating_source_of_latitude_longitude_information:2
    distance_of_localizer_antenna_from_approach_end_of_runway_feet:7
    distance_of_localizer_antenna_from_runway_centerline_feet:4
    direction_of_localizer_antenna_from_runway_centerline:1
    code_indicating_source_of_distance:2
    site_elevation_of_localizer_antenna_in_tenth_of_a_foot_msl:7
    localizer_frequency_mhz:7
    localizer_back_course_status:15
    localizer_course_width_degrees_and_hundredths:5
    localizer_course_width_at_threshold:7
    localizer_distance_from_stop_end_of_rwy_feet:7
    localizer_direction_from_stop_end_of_rwy:1
    localizer_services_code:2
    blank:201
    ',
    'ILS3' => '
        record_type_indicator:4
    airport_site_number_identifier:11
    ils_runway_end_identifier:3
    ils_system_type:10
    operational_status_of_glide_slope:22
    effective_date_of_glide_slope_operational_status:10
    latitude_of_glide_slope_transmitter_antenna_formatted:14
    latitude_of_glide_slope_transmitter_antenna_all_seconds:11
    longitude_of_glide_slope_transmitter_antenna_formatted:14
    longitude_of_glide_slope_transmitter_antenna_all_seconds:11
    code_indicating_source_of_latitude_longitude_information:2
    distance_of_glide_slope_transmitter_antenna_from_approach_end_o:7
    distance_of_glide_slope_transmitter_antenna_from_runway_centerl:4
    direction_of_glide_slope_transmitter_antenna_from_runway_center:1
    code_indicating_source_of_distance_information:2
    site_elevation_of_glide_slope_transmitter_antenna_in_tenth_of_a:7
    glide_slope_class_type:15
    glide_slope_angle_in_degrees_and_hundredths_of_degree:5
    glide_slope_transmission_frequency:7
    elevation_of_runway_at_point_adjacent_to_the_glide_slope_antenn:8
    blank:210
    ',
    'ILS4' => '
        record_type_indicator:4
    airport_site_number_identifier:11
    ils_runway_end_identifier:3
    ils_system_type:10
    operational_status_of_dme:22
    effective_date_of_dme_operational_status:10
    latitude_of_dme_transponder_antenna_formatted:14
    latitude_of_dme_transponder_antenna_all_seconds:11
    longitude_of_dme_transponder_antenna_formatted:14
    longitude_of_dme_transponder_antenna_all_seconds:11
    code_indicating_source_of_latitude_longitude_information:2
    distance_of_dme_transmitter_antenna_from_approach_end_of_runway:7
    distance_of_dme_transponder_antenna_from_runway_centerline_feet:4
    direction_of_dme_transponder_antenna_from_runway_centerline:1
    code_indicating_source_of_distance_information:2
    site_elevation_of_dme_transponder_antenna_in_tenth_of_a_foot_ms:7
    channel_on_which_distance_data_is_transmitted:4
    distance_of_dme_antenna_from_stop_end_of_runway:7
    blank:234
    ',
    'ILS5' => '
        record_type_indicator:4
    airport_site_number_identifier:11
    ils_runway_end_identifier:3
    ils_system_type:10
    marker_type:2
    operational_status_of_marker_beacon:22
    effective_date_of_marker_beacon_operational_status:10
    latitude_of_marker_beacon_formatted:14
    latitude_of_marker_beacon_all_seconds:11
    longitude_of_marker_beacon_formatted:14
    longitude_of_marker_beacon_all_seconds:11
    code_indicating_source_of_latitude_longitude_information:2
    distance_of_marker_beacon_from_approach_end_of_runway_feet:7
    distance_of_marker_beacon_from_runway_centerline_feet:4
    direction_of_marker_beacon_from_runway_centerline:1
    code_indicating_source_of_distance_information:2
    site_elevation_of_marker_beacon_in_tenth_of_a_foot_msl:7
    facility_type_of_marker_locator:15
    location_identifier_of_beacon_at_marker:2
    name_of_the_marker_locator_beacon:30
    frequency_of_locator_beacon_at_middle_marker_in_khz:3
    location_identifier_navaid_type_of_navigation_aid_colocated_wit:25
    low_powered_ndb_status_of_marker_beacon:22
    service_provided_by_marker:30
    blank:116
    ',
    'ILS6' => '
        record_type_indicator:4
    airport_site_number_identifier:11
    ils_runway_end_identifier:3
    ils_system_type:10
    ils_remarks_free_form_text:350
    ',
  },
  LID => {
    '1' => '
        identifier_group_sort_code:1
    identifier_group_code:3
    location_identifier:5
    faa_region_associated_with_the_location_identifier:3
    state_associated_with_the_location_identifier_alphabetic_post_o:2
    city_associated_with_the_location_identifier:40
    controlling_artcc_for_this_location:4
    controlling_artcc_for_this_location_computer_id:3
    landing_facility_name:50
    landing_facility_type:13
    tie_in_flight_service_station_fss_identifier1:4
    navaid_facility_name_1:30
    navaid_facility_type_1:20
    navaid_facility_name_2:30
    navaid_facility_type_2:20
    navaid_facility_name_3:30
    navaid_facility_type_3:20
    navaid_facility_name_4:30
    navaid_facility_type_4:20
    tie_in_flight_service_station_fss_identifier2:4
    ils_runway_end:3
    ils_facility_type:20
    location_identifier_of_ils_airport:5
    ils_airport_name:50
    tie_in_flight_service_station_fss_identifier3:4
    fss_name:30
    artcc_name:30
    artcc_facility_type:17
    flight_watch_station_fltwo_indicator:1
    other_facility_facility_name_description:75
    other_facility_facility_type:15
    effective_date_of_this_information_mm_dd_yyyy:10
    blanks:447
    ',
    '2' => '
        identifier_group_sort_code:1
    identifier_group_code:3
    location_identifier:5
    country_associated_with_the_location_identifier:30
    second_level_qualifier_of_location_description:20
    city_location_associated_with_the_identifier:50
    landing_facility_name:50
    landing_facility_type:20
    navaid_facility_name_1:50
    navaid_facility_type_1:20
    navaid_facility_name_2:50
    navaid_facility_type_2:20
    other_facility_name_1:50
    other_facility_type_1:20
    other_facility_name_2:50
    other_facility_type_2:20
    effective_date_of_this_information_mm_dd_yyyy:10
    blanks:570
    ',
    '3' => '
        identifier_group_sort_code:1
    identifier_group_code:3
    location_identifier:5
    canadian_province_associated_with_the_location_identifier:10
    city_location_associated_with_the_identifier:50
    landing_facility_name_1:50
    landing_facility_additional_descriptive_text_1:50
    landing_facility_type_1:20
    landing_facility_name_2:50
    landing_facility_additional_descriptive_text_2:50
    landing_facility_type_2:20
    landing_facility_name_3:50
    landing_facility_additional_descriptive_text_3:50
    landing_facility_type_3:20
    navaid_facility_name_1:30
    navaid_facility_additional_descriptive_text_1:50
    navaid_facility_type_1:20
    navaid_facility_name_2:30
    navaid_facility_additional_descriptive_text_2:50
    navaid_facility_type_2:20
    ils_facility_name_1:30
    ils_facility_additional_descriptive_text_1:50
    ils_facility_type_1:20
    ils_facility_name_2:30
    ils_facility_additional_descriptive_text_2:50
    ils_facility_type_2:20
    other_facility_name_1:30
    other_facility_additional_descriptive_text_1:50
    other_facility_type_1:20
    other_facility_name_2:30
    other_facility_additional_descriptive_text_2:50
    other_facility_type_2:20
    effective_date_of_this_information_mm_dd_yyyy:10
    ',
  },
  MTR => {

    #   artcc_ident_20_occurences_of_4_character_ident:80
    'MTR1' => '
        record_type_indicator:4
    route_type:3
    route_identifier:5
    publication_effective_date_yyyymmdd:8
    faa_region_code:3
    artcc_ident_1:4
    artcc_ident_2:4
    artcc_ident_3:4
    artcc_ident_4:4
    artcc_ident_5:4
    artcc_ident_6:4
    artcc_ident_7:4
    artcc_ident_8:4
    artcc_ident_9:4
    artcc_ident_10:4
    artcc_ident_11:4
    artcc_ident_12:4
    artcc_ident_13:4
    artcc_ident_14:4
    artcc_ident_15:4
    artcc_ident_16:4
    artcc_ident_17:4
    artcc_ident_18:4
    artcc_ident_19:4
    artcc_ident_20:4
    all_flight_service_station_fss_idents_within_150_nautical_miles:160
    times_of_use_text_information:175
    blanks:76
    sort_sequence_number_for_record:5
    ',
    'MTR2' => '
        record_type_indicator:4
    route_type:3
    route_identifier:5
    standard_operating_procedure_text:100
    blanks:402
    sort_sequence_number_for_record:5
    ',
    'MTR3' => '
        record_type_indicator:4
    route_type:3
    route_identifier:5
    route_width_description_text:100
    blanks:402
    record_sort_sequence_number:5
    ',
    'MTR4' => '
        record_type_indicator:4
    route_type:3
    route_identifier:5
    terrain_following_operations_text:100
    blanks:402
    record_sort_sequence_number:5
    ',
    'MTR5' => '
        record_type_indicator:4
    route_type:3
    route_identifier:5
    route_point_id:5
    segment_description_text_leading_up_to_the_point_maximum_of_4_o:228
    segment_description_text_leaving_the_point_maximum_of_4_occuren:228
    ident_of_related_navaid:4
    bearing_of_navaid_from_point:5
    distance_of_navaid_from_point:4
    latitude_location_of_point:14
    longitude_location_of_point:14
    record_sort_sequence_number_segment_sequence_number_for_this_po:5
    ',
    'MTR6' => '
        record_type_indicator:4
    route_type:3
    route_identifier:5
    agency_type_code:2
    agency_organization_name:61
    agency_station:30
    agency_address:35
    agency_city:30
    agency_state_alpha_post_office_code:2
    agency_zip_code:10
    agency_commercial_phone_number:40
    agency_dsn_phone_number:40
    agency_hours:175
    blanks:77
    record_sort_sequence_number:5
    ',
  },

  #NATFIX => {
  #  'NATFIX' => '
  #      character_i_that_indicates_beginning_of_record:1
  #  blank_character_separating_fields1:1
  #  fix_navaid_airport_id:5
  #  blank_character_separating_fields2:1
  #  fix_navaid_airport_latitude_format_ddmmssx:7
  #  blank_character_separating_fields3:1
  #  fix_navaid_airport_longitude_format_dddmmssx:8
  #  blank_character_separating_fields4:1
  #  single_quote_character_that_precedes_artcc_id:1
  #  artcc_id:4
  #  blank_character_separating_fields5:1
  #  state_post_office_code:2
  #  blank_character_separating_fields6:1
  #  icao_region_code:2
  #  blank_character_separating_fields7:1
  #  fix_navaid_type_or_string_arpt:7
  #  ',
  # },
  NAV => {
    'NAV1' => '
    record_type_indicator:4
    navaid_facility_identifier:4
    navaid_facility_type_see_description:20
    official_navaid_facility_identifier:4
    effective_date:10
    name_of_navaid:30
    city_associated_with_the_navaid:40
    state_name_where_associated_city_is_located:30
    state_post_office_code_where_associated:2
    faa_region_responsible_for_navaid_code:3
    country_navaid_located_if_other_than_u_s:30
    country_post_office_code_navaid:2
    navaid_owner_name:50
    navaid_operator_name:50
    common_system_usage_y_or_n:1
    navaid_public_use_y_or_n:1
    class_of_navaid:11
    hours_of_operation_of_navaid:11
    identifier_of_artcc_with_high_altitude_boundary_that_the_navaid:4
    name_of_artcc_with_high_altitude_boundary_that_the_navaid_falls:30
    identifier_of_artcc_with_low_altitude_boundary_that_the_navaid:4
    name_of_artcc_with_low_altitude_boundary_that_the_navaid_falls:30
    navaid_latitude_formatted:14
    navaid_latitude_all_seconds:11
    navaid_longitude_formatted:14
    navaid_longitude_all_seconds:11
    latitude_longitude_survery_accuracy_code:1
    latitude_of_tacan_portion_of_vortac_when_tacan_is_not_sited_witF:14
    latitude_of_tacan_portion_of_vortac_when_tacan_is_not_sited_witS:11
    longitude_of_tacan_portion_of_vortac_when_tacan_is_not_sited_wiF:14
    longitude_of_tacan_portion_of_vortac_when_tacan_is_not_sited_wiS:11
    elevation_in_tenth_of_a_foot_msl:7
    magnetic_variation_degrees_00_99:5
    magnetic_variation_epoch_year_00_99:4
    simultaneous_voice_feature_y_n_or_null:3
    power_output_in_watts:4
    automatic_voice_identification_feature:3
    monitoring_category:1
    radio_voice_call_name:30
    channel_tacan_navaid_transmits_on:4
    frequency_the_navaid_transmits_on_except_tacan:6
    transmitted_fan_marker_marine_radio_beacon_identifier:24
    fan_marker_type_bone_or_elliptical:10
    true_bearing_of_major_axis_of_fan_marker:3
    vor_standard_service_volume:2
    dme_standard_service_volume:2
    low_altitude_facility_used_in_high_structure:3
    navaid_z_marker_available_y_n_or_null:3
    transcribed_weather_broadcast_hours_tweb:9
    transcribed_weather_broadcast_phone_number:20
    associated_controlling_fss_ident:4
    associated_controlling_fss_name:30
    hours_of_operation_of_controlling_fss:100
    notam_accountability_code_ident:4
    quadrant_identification_and_range_leg_bearing:16
    navigation_aid_status:30
    pitch_flag_y_or_n:1
    catch_flag_y_or_n:1
    sua_atcaa_flag_y_or_n:1
    navaid_restriction_flag:1
    hiwas_flag:1
    transcribed_weather_broadcast_tweb_restriction:1
    ',
    'NAV2' => '
        record_type_indicator:4
    navaid_facility_identifier:4
    navaid_facitity_type:20
    navaid_remarks_free_form_text:600
    filler:177
    ',
    'NAV3' => '
        record_type_indicator:4
    navaid_facility_identifier:4
    navaid_facitity_type:20
    names_of_fixes_fix_file:36
    space_allocated_for_20_more_fixes:720
    blanks:21
    ',
    'NAV4' => '
        record_type_indicator:4
    navaid_facility_identifier:4
    navaid_facitity_type:20
    names_of_holding_patterns_and_the_state_in_which_the_holding:80
    pattern_number_of_the_holding_pattern:3
    space_allocated_for_8_more_holding_patterns:664
    blanks:30
    ',
    'NAV5' => '
        record_type_indicator:4
    navaid_facility_identifier:4
    navaid_facitity_type:20
    names_of_fan_marker_s:30
    space_allocated_for_23_more_fan_markers:690
    blanks:57
    ',
    'NAV6' => '
        record_type_indicator:4
    navaid_facility_identifier:4
    navaid_facitity_type:20
    air_ground_code:2
    bearing_of_checkpoint:3
    altitude_only_when_checkpoint_is_in_air:5
    airport_id:4
    state_code_in_which_associated_city_is_located:2
    narrative_description_associated_with_the_checkpoint_in_air:75
    narrative_description_associated_with_the_checkpoint_on_ground:75
    blanks:611
    ',
  },
  PFR => {
    'PFR1' => '
        record_type_indicator:4
    origin_facility_location_identifier:5
    destination_facility_location_identifier:5
    type_of_preferred_route_code:3
    route_identifier_sequence_number_1_99:2
    type_of_preferred_route_description:30
    preferred_route_area_description:75
    preferred_route_altitude_description:40
    aircraft_allowed_limitations_description:50
    effective_hours_gmt_description_1:15
    effective_hours_gmt_description_2:15
    effective_hours_gmt_description_3:15
    route_direction_limitations_description:20
    nar_type:20
    designator:5
    destination_city:40
    ',
    'PFR2' => '
        record_type_indicator:4
    origin_facility_location_identifier:5
    destination_facility_location_identifier:5
    type_of_preferred_route_code:3
    route_identifier_sequence_number_1_99:2
    segment_sequence_number_within_the_route:3
    segment_identifier:48
    segment_type:7
    fix_state_code_post_office_alpha_code:2
    icao_region_code:2
    navaid_facility_type_code:2
    navaid_facility_type_described:20
    radial_and_distance_from_navaid:7
    blank:234
    ',
  },
  PJA => {
    'PJA1' => '
        record_type_indicator:4
    pja_id:6
    navaid_identifier:4
    navaid_facility_type_code:2
    navaid_facility_type_described:25
    azimuth_degrees_from_navaid:6
    distance_in_nautical_miles_from_navaid:8
    navaid_name:30
    pja_state_abbreviation_two_letter_post_office:2
    pja_state_name:30
    pja_associated_city_name:30
    pja_latitude_formatted:14
    pja_latitude_seconds:12
    pja_longitude_formatted:15
    pja_longitude_seconds:12
    associated_airport_name:50
    associated_airport_site_number:11
    pja_drop_zone_name:50
    pja_maximum_altitude_allowed:8
    pja_area_radius_in_nautical_miles_from_center_point:5
    sectional_charting_required:3
    area_to_be_published_in_airport_facility_directory:3
    additional_descriptive_text_for_area:100
    associated_fss_ident:4
    associated_fss_name:30
    pja_use:8
    volume:1
    ',
    'PJA2' => '
        record_type_indicator:4
    pja_id:6
    times_of_use_description:75
    blanks:388
    ',
    'PJA3' => '
        record_type_indicator:4
    pja_id:6
    pja_user_group_name:75
    description:75
    blanks:313
    ',
    'PJA4' => '
        record_type_indicator:4
    pja_id:6
    contact_facility_id:4
    contact_facility_name_type_of_facility_concatenated_to_name:48
    related_loc_id:4
    commercial_frequency:8
    commercial_chart_flag:1
    military_frequency:8
    military_chart_flag:1
    sector:30
    altitude:20
    blanks:339
    ',
    'PJA5' => '
        record_type_indicator:4
    pja_id:6
    additional_remarks:300
    blanks:163
    ',
  },

  #SSD => {
  #  'SSD' => '
  #      internal_sequence_number:5
  #  not_used1:5
  #  fix_facility_type_code:2
  #  not_used2:1
  #  fix_navaid_airport_latitude_format_xddmmsst:8
  #  fix_navaid_airport_longitude_format_xdddmmsst:9
  #  fix_navaid_airport_identifier:6
  #  icao_region_code_fix_only:2
  #  star_sid_computer_code:13
  #  star_sid_transition_name:110
  #  airways_navaids_using_numbered_fix:62
  #  ',
  #},
  STARDP => {
    'STARDP' => '
        internal_sequence_number:5
    not_used1:5
    fix_facility_type_code:2
    not_used2:1
    fix_navaid_airport_latitude_format_xddmmsst:8
    fix_navaid_airport_longitude_format_xdddmmsst:9
    fix_navaid_airport_identifier:6
    icao_region_code_fix_only:2
    star_dp_computer_code:13
    star_dp_transition_name:110
    airways_navaids_using_numbered_fix:62
    ',
  },
  TWR => {
    'TWR1' => '
    record_type_indicator:4
    terminal_communications_facility_identifier:4
    information_effective_date_mm_dd_yyyy:10
    landing_facility_site_number:11
    faa_region_code:3
    associated_state_name:30
    associated_state_post_office_code:2
    associated_city_name:40
    official_airport_name:50
    airport_reference_point_latitude_formatted:14
    airport_reference_point_latitude_seconds:11
    airport_reference_point_longitude_formatted:14
    airport_reference_point_longitude_seconds:11
    tie_in_flight_service_station_fss_identifier:4
    tie_in_flight_service_station_fss_name:30
    facility_type:12
    number_of_hours_of_daily_operation:2
    number_of_hours_of_daily_operation_indication_of_regularity:3
    master_airport_location_identifier:4
    name_of_master_airport_furnishing_services_if_this_facility_is:50
    direction_finding_equipment_type:15
    name_of_associated_landing_facility_when_the_terminal_facility:50
    name_of_the_associated_city_when_the_facility_not_located_on_th:40
    name_of_the_state_or_province_when_the_facility_not_located_on:20
    name_of_state_country_when_facility_not_located_on_airport_or_w:25
    country_state_post_office_code_when_facility_not_located_on_air:2
    faa_region_code_when_the_facility_not_located_on_the_airport_or:3
    airport_surveillance_radar_latitude_formatted:14
    airport_surveillance_radar_latitude_seconds:11
    airport_surveillance_radar_longitude_formatted:14
    airport_surveillance_radar_longitude_seconds:11
    latitude_of_direction_finding_antenna_formatted:14
    latitude_of_direction_finding_antenna_seconds:11
    longitude_of_direction_finding_antenna_formatted:14
    longitude_of_direction_finding_antenna_seconds:11
    name_of_the_agency_that_operates_the_tower:40
    name_of_the_agency_that_operates_military_operations:40
    name_of_the_agency_that_operates_the_primary_approach_control_f:40
    name_of_the_agency_operating_the_secondary_approach_control_fac:40
    name_of_the_agency_operating_the_primary_departure_control_faci:40
    name_of_the_agency_operating_the_secondary_departure_control_fa:40
    radio_call_used_by_pilot_to_contact_tower:26
    radio_call_name_for_military_operations_at_this_airport:26
    radio_call_of_facility_that_furnishes_primary_approach_control:26
    radio_call_of_facility_that_takes_over_approach_control_when_pr:26
    radio_call_of_facility_that_furnishes_primary_departure_control:26
    radio_call_of_facility_that_takes_over_departure_control_when_p:26
    blank:648
    ',
    'TWR2' => '
    record_identifier:4
    terminal_communications_facility_identifier:4
    hours_of_operation_of_the_military_pilot_to_metro_service_pmsv:200
    hours_of_operation_of_the_military_aircraft_command_post_macp_l:200
    hours_of_military_operations_conducted_each_day:200
    hours_of_operation_of_primary_approach_control_facility_in_loca:200
    hours_of_operation_of_secondary_approach_control_facility_in_lo:200
    hours_of_operation_of_primary_departure_control_facility_in_loc:200
    hours_of_operation_of_secondary_departure_control_facility_in_l:200
    hours_of_tower_operation_in_local_time:200
    ',
    'TWR3' => '
    record_identifier:4
    terminal_communications_facility_identifier:4
    frequencys_for_master_airport_use_only_and_sectorization_1:44
    frequency_use_1:50
    frequencys_for_master_airport_use_only_and_sectorization_2:44
    frequency_use_2:50
    frequencys_for_master_airport_use_only_and_sectorization_3:44
    frequency_use_3:50
    frequencys_for_master_airport_use_only_and_sectorization_4:44
    frequency_use_4:50
    frequencys_for_master_airport_use_only_and_sectorization_5:44
    frequency_use_5:50
    frequencys_for_master_airport_use_only_and_sectorization_6:44
    frequency_use_6:50
    frequencys_for_master_airport_use_only_and_sectorization_7:44
    frequency_use_7:50
    frequencys_for_master_airport_use_only_and_sectorization_8:44
    frequency_use_8:50
    frequencys_for_master_airport_use_only_and_sectorization_9:44
    frequency_use_9:50
    frequencys_for_master_airport_use_only_and_sectorization_not_1:60
    frequencys_for_master_airport_use_only_and_sectorization_not_2:60
    frequencys_for_master_airport_use_only_and_sectorization_not_3:60
    frequencys_for_master_airport_use_only_and_sectorization_not_4:60
    frequencys_for_master_airport_use_only_and_sectorization_not_5:60
    frequencys_for_master_airport_use_only_and_sectorization_not_6:60
    frequencys_for_master_airport_use_only_and_sectorization_not_7:60
    frequencys_for_master_airport_use_only_and_sectorization_not_8:60
    frequencys_for_master_airport_use_only_and_sectorization_not_9:60
    blank:214
    ',
    'TWR4' => '
    record_identifier:4
    terminal_communications_facility_identifier:4
    master_airport_services:100
    blank:1500
    ',
    'TWR5' => '
    record_identifier:4
    terminal_communications_facility_identifier:4
    radar_or_non_radar_primary_approach_call:9
    radar_or_non_radar_secondary_approach_call:9
    radar_or_non_radar_primary_departure_call:9
    radar_or_non_radar_secondary_departure_call:9
    type_of_radar_at_the_tower_1:10
    radar_hours_of_operation_1:200
    type_of_radar_at_the_tower_2:10
    radar_hours_of_operation_2:200
    type_of_radar_at_the_tower_3:10
    radar_hours_of_operation_3:200
    type_of_radar_at_the_tower_4:10
    radar_hours_of_operation_4:200
    blank:724
    ',
    'TWR6' => '
    record_identifier:4
    terminal_communications_facility_identifier:4
    tower_element_number:5
    tower_remark_text:800
    blank:795
    ',
    'TWR7' => '
    record_identifier:4
    terminal_communications_facility_identifier:4
    satellite_frequency:44
    satellite_frequency_use:50
    satellite_airport_site_number:11
    satellite_airport_location_identifier_may_be_used_as_a_link_to:4
    satellite_faa_region_code:3
    satellite_associated_state_name:30
    satellite_associated_state_post_office_code:2
    satellite_associated_city:40
    satellite_arpt_name:50
    airport_latitude_formatted:14
    airport_latitude_seconds:11
    airport_longitude_formatted:14
    airport_longitude_seconds:11
    flight_service_station_identifier:4
    flight_service_station_name:30
    master_airport_information:11
    master_airport_faa_region_code:3
    master_airport_associated_state_name:30
    master_airport_associated_state_post_office:2
    master_airport_associated_city:40
    master_airport_name:50
    satellite_frequency_not_truncated:60
    blank:1086
    ',
    'TWR8' => '
    record_identifier:4
    terminal_communications_facility_identifier:4
    class_b_airspace:1
    class_c_airspace:1
    class_d_airspace:1
    class_e_airspace:1
    airspace_hours:300
    blank:1296
    ',
    'TWR9' => '
    record_identifier:4
    terminal_communications_facility_identifier:4
    atis_serial_number:4
    atis_hours_of_operation_in_local_time:200
    optional_description_of_purpose_fulfilled_by_atis:100
    atis_phone_number:18
    blank:1278
    ',
  },
  WXL => {
    'WXL' => '
    weather_reporting_location_identifier:5
    latitude_of_the_weather_reporting_location:8
    longitude_of_the_weather_reporting_location:9
    associated_city:40
    associated_state_post_office_code:2
    associated_country_numeric_code_non_us_only:3
    weather_reporting_location_elevation_value_whole_feet_msl:5
    weather_reporting_location_elevation_accuracy:1
    weather_services_available_at_location_up_to:60
    blank:2
    ',

    #bug: these aren't implemented yet
    '*1' => '
    continuation_record_indicator:1
    collective_weather_service_type:5
    collective_number:1
    blank:114
    ',
    '*2' => '
    continuation_record_indicator:1
    affected_area_weather_service_type:5
    affected_areas_states_areas:114
    blank:1
    ',
  },

  OBSTACLE => {
    'OBSTACLE' => '
        ors_code:2
        dash:1
        obstacle_number:6
        blank1:1
        verification_status:1
        blank2:1
        country_identifier:2
        blank3:1
        state_identifier:2
        blank4:1
        city_name:16
        blank5:1
        latitude_degrees:2
        blank6:1
        latitude_minutes:2
        blank7:1
        latitude_seconds:5
        latitude_hemisphere:1
        blank8:1
        longitude_degrees:3
        blank9:1
        longitude_minutes:2
        blank10:1
        longitude_seconds:5
        longitude_hemisphere:1
        blank11:1
        obstacle_type:18
        blank12:1
        quantity:1
        blank13:1
        agl_ht:5
        blank14:1
        amsl_ht:5
        blank15:1
        lighting:1
        blank16:1
        horizontal_accuracy:1
        blank17:1
        vertical_accuracy:1
        blank18:1
        mark_indicator:1
        blank19:1
        faa_study_number:14
        blank20:1
        action:1
        blank21:1
        julian_date:7
        blank22:1
        '

      #Daily DOF doesn't use the datchk code, uncomment if using the periodic data
      #datchk_code:6
      #'
  }
