import modules.Helpers as h


def parse_geometry(base_file: str, record_type: str, line_dict: dict) -> dict | None:
    result = None
    base_group = parsers.get(base_file)
    if base_group != None:
        function_reference = base_group.get(record_type)
        if function_reference != None:
            result = function_reference(line_dict)

    return result


def check_for_position_data(line_dict: dict, keys: list) -> bool:
    return all(key in line_dict for key in keys)


def geometry_OBSTACLE_OBSTACLE(line_dict: dict) -> dict:
    keys = [
        "latitude_degrees",
        "latitude_minutes",
        "latitude_seconds",
        "latitude_hemisphere",
        "longitude_degrees",
        "longitude_minutes",
        "longitude_seconds",
        "longitude_hemisphere",
    ]
    if check_for_position_data(line_dict, keys):

        latitude = h.coordinateToDecimal2(
            line_dict["latitude_degrees"],
            line_dict["latitude_minutes"],
            line_dict["latitude_seconds"],
            line_dict["latitude_hemisphere"],
        )

        longitude = h.coordinateToDecimal2(
            line_dict["longitude_degrees"],
            line_dict["longitude_minutes"],
            line_dict["longitude_seconds"],
            line_dict["longitude_hemisphere"],
        )

        line_dict["obstacle_latitude"] = latitude
        line_dict["obstacle_longitude"] = longitude

    return line_dict


def geometry_AFF_AFF1(line_dict: dict) -> dict:
    keys = [
        "site_latitude_formatted",
        "site_longitude_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["site_latitude_formatted"])
        longitude = h.coordinateToDecimal(line_dict["site_longitude_formatted"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_AFF_AFF3(line_dict: dict) -> dict:
    keys = [
        "latitude_of_the_airport_formatted",
        "longitude_of_the_airport_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["latitude_of_the_airport_formatted"])
        longitude = h.coordinateToDecimal(
            line_dict["longitude_of_the_airport_formatted"]
        )

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_ARB_ARB(line_dict: dict) -> dict:
    keys = [
        "latitude_of_the_boundary_point",
        "longitude_of_the_boundary_point",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["latitude_of_the_boundary_point"])
        longitude = h.coordinateToDecimal(line_dict["longitude_of_the_boundary_point"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_ATS_ATS2(line_dict: dict) -> dict:
    keys = [
        "navaid_facility_fix_latitude",
        "navaid_facility_fix_longitude",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["navaid_facility_fix_latitude"])
        longitude = h.coordinateToDecimal(line_dict["navaid_facility_fix_longitude"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_ATS_ATS3(line_dict: dict) -> dict:
    keys = [
        "navaid_facility_latitude",
        "navaid_facility_longitude",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["navaid_facility_latitude"])
        longitude = h.coordinateToDecimal(line_dict["navaid_facility_longitude"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_AWY_AWY2(line_dict: dict) -> dict:
    keys = [
        "navaid_facility_fix_latitude",
        "navaid_facility_fix_longitude",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["navaid_facility_fix_latitude"])
        longitude = h.coordinateToDecimal(line_dict["navaid_facility_fix_longitude"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_AWY_AWY3(line_dict: dict) -> dict:
    keys = [
        "navaid_facility_latitude",
        "navaid_facility_longitude",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["navaid_facility_latitude"])
        longitude = h.coordinateToDecimal(line_dict["navaid_facility_longitude"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_COM_COM(line_dict: dict) -> dict:
    keys = [
        "communications_outlet_latitude",
        "communications_outlet_longitude",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["communications_outlet_latitude"])
        longitude = h.coordinateToDecimal(line_dict["communications_outlet_longitude"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_FSS_FSS(line_dict: dict) -> dict:
    # Check if FSS on airport
    keys = [
        "airport_latitude_fss_on_arpt",
        "airport_longitude_fss_on_arpt",
    ]
    if check_for_position_data(line_dict, keys):
        line_dict["latitude"] = h.coordinateToDecimal(
            line_dict["airport_latitude_fss_on_arpt"]
        )
        line_dict["longitude"] = h.coordinateToDecimal(
            line_dict["airport_longitude_fss_on_arpt"]
        )

    # Check if FSS off airport
    keys = [
        "latitude_when_fss_is_not_on_airport_see_f6",
        "longitude_when_fss_is_not_on_airport_see_f6",
    ]
    if check_for_position_data(line_dict, keys):
        line_dict["latitude"] = h.coordinateToDecimal(
            line_dict["latitude_when_fss_is_not_on_airport_see_f6"]
        )
        line_dict["longitude"] = h.coordinateToDecimal(
            line_dict["longitude_when_fss_is_not_on_airport_see_f6"]
        )

    return line_dict


def geometry_HARFIX_HARFIX(line_dict: dict) -> dict:
    # This is fully commented in the original Perl
    # latitude = h.coordinateToDecimal(
    #     line_dict["airport_reference_point_latitude_formatted"]
    # )
    # longitude = h.coordinateToDecimal(
    #     line_dict["airport_reference_point_longitude_formatted"]
    # )

    return line_dict


def geometry_HPF_HP1(line_dict: dict) -> dict:
    # Check for associated fix
    keys = [
        "latitude_of_the_associated_fix_dd_mm_ss_sssn",
        "longitude_of_the_associated_fix_ddd_mm_ss_sssw",
    ]
    if check_for_position_data(line_dict, keys):
        fix_latitude = h.coordinateToDecimal(
            line_dict["latitude_of_the_associated_fix_dd_mm_ss_sssn"]
        )
        fix_longitude = h.coordinateToDecimal(
            line_dict["longitude_of_the_associated_fix_ddd_mm_ss_sssw"]
        )

        line_dict["latitude_of_the_associated_fix"] = fix_latitude
        line_dict["longitude_of_the_associated_fix"] = fix_longitude

    # Check for associated navaid
    keys = [
        "latitude_of_the_associated_navaid_dd_mm_ss_sssn",
        "longitude_of_the_associated_navaid_ddd_mm_ss_sssw",
    ]
    if check_for_position_data(line_dict, keys):
        navaid_latitude = h.coordinateToDecimal(
            line_dict["latitude_of_the_associated_navaid_dd_mm_ss_sssn"]
        )
        navaid_longitude = h.coordinateToDecimal(
            line_dict["longitude_of_the_associated_navaid_ddd_mm_ss_sssw"]
        )

        line_dict["latitude_of_the_associated_navaid"] = navaid_latitude
        line_dict["longitude_of_the_associated_navaid"] = navaid_longitude

    return line_dict


def geometry_ILS_ILS2(line_dict: dict) -> dict:
    keys = [
        "latitude_of_localizer_antenna_formatted",
        "longitude_of_localizer_antenna_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(
            line_dict["latitude_of_localizer_antenna_formatted"]
        )
        longitude = h.coordinateToDecimal(
            line_dict["longitude_of_localizer_antenna_formatted"]
        )

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_ILS_ILS3(line_dict: dict) -> dict:
    keys = [
        "latitude_of_glide_slope_transmitter_antenna_formatted",
        "longitude_of_glide_slope_transmitter_antenna_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(
            line_dict["latitude_of_glide_slope_transmitter_antenna_formatted"]
        )
        longitude = h.coordinateToDecimal(
            line_dict["longitude_of_glide_slope_transmitter_antenna_formatted"]
        )

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_ILS_ILS4(line_dict: dict) -> dict:
    keys = [
        "latitude_of_dme_transponder_antenna_formatted",
        "longitude_of_dme_transponder_antenna_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(
            line_dict["latitude_of_dme_transponder_antenna_formatted"]
        )
        longitude = h.coordinateToDecimal(
            line_dict["longitude_of_dme_transponder_antenna_formatted"]
        )

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_ILS_ILS5(line_dict: dict) -> dict:
    keys = [
        "latitude_of_marker_beacon_formatted",
        "longitude_of_marker_beacon_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(
            line_dict["latitude_of_marker_beacon_formatted"]
        )
        longitude = h.coordinateToDecimal(
            line_dict["longitude_of_marker_beacon_formatted"]
        )

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_MTR_MTR5(line_dict: dict) -> dict:
    keys = [
        "latitude_location_of_point",
        "longitude_location_of_point",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal3(line_dict["latitude_location_of_point"])
        longitude = h.coordinateToDecimal3(line_dict["longitude_location_of_point"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_NATFIX_NATFIX(line_dict: dict) -> dict:
    # This is fully commented in the original Perl
    # my $hashRef = shift;

    # #Calculate the decimal representation of lon/lat
    # my $latitude =
    # &coordinateToDecimal(
    # $hashRef->{airport_reference_point_latitude_formatted},
    # );
    # my $longitude =
    # &coordinateToDecimal(
    # $hashRef->{airport_reference_point_longitude_formatted} );

    # # #and save in the hash as a POINT
    #

    # $hashRef->{apt_latitude}  = $latitude;
    # $hashRef->{apt_longitude} = $longitude;
    return line_dict


def geometry_SSD_SSD(line_dict: dict) -> dict:
    keys = [
        "fix_navaid_airport_latitude_format_xddmmsst",
        "fix_navaid_airport_longitude_format_xdddmmsst",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = line_dict["fix_navaid_airport_latitude_format_xddmmsst"]
        longitude = line_dict["fix_navaid_airport_longitude_format_xdddmmsst"]

        if longitude and latitude:
            lat_declination = latitude[0:1]
            lat_deg = h.atoi(latitude[1:3])
            lat_min = h.atoi(latitude[3:5])
            lat_sec = h.atof(latitude[5:7] + "." + latitude[7:8])

            lon_declination = longitude[0:1]
            lon_deg = h.atoi(longitude[1:4])
            lon_min = h.atoi(longitude[4:6])
            lon_sec = h.atof(longitude[6:8] + "." + longitude[8:9])

        if not (lat_deg, lat_min, lat_sec, lon_deg, lon_min, lon_sec):
            print(f"Unable to convert point.")
            return None

        latitude = h.coordinateToDecimal2(lat_deg, lat_min, lat_sec, lat_declination)
        longitude = h.coordinateToDecimal2(lon_deg, lon_min, lon_sec, lon_declination)

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_STARDP_STARDP(line_dict: dict) -> dict:
    keys = [
        "fix_navaid_airport_latitude_format_xddmmsst",
        "fix_navaid_airport_longitude_format_xdddmmsst",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = line_dict["fix_navaid_airport_latitude_format_xddmmsst"]
        longitude = line_dict["fix_navaid_airport_longitude_format_xdddmmsst"]

        if longitude and latitude:
            lat_declination = latitude[0:1]
            lat_deg = h.atoi(latitude[1:3])
            lat_min = h.atoi(latitude[3:5])
            lat_sec = h.atof(latitude[5:7] + "." + latitude[7:8])

            lon_declination = longitude[0:1]
            lon_deg = h.atoi(longitude[1:4])
            lon_min = h.atoi(longitude[4:6])
            lon_sec = h.atof(longitude[6:8] + "." + longitude[8:9])

        if not (lat_deg, lat_min, lat_sec, lon_deg, lon_min, lon_sec):
            print(f"Unable to convert point.")
            return None

        latitude = h.coordinateToDecimal2(lat_deg, lat_min, lat_sec, lat_declination)
        longitude = h.coordinateToDecimal2(lon_deg, lon_min, lon_sec, lon_declination)

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_TWR_TWR1(line_dict: dict) -> dict:
    keys = [
        "airport_reference_point_latitude_formatted",
        "airport_reference_point_longitude_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(
            line_dict["airport_reference_point_latitude_formatted"]
        )
        longitude = h.coordinateToDecimal(
            line_dict["airport_reference_point_longitude_formatted"]
        )

        line_dict["airport_reference_point_latitude"] = latitude
        line_dict["airport_reference_point_longitude"] = longitude

    keys = [
        "airport_reference_point_latitude_formatted",
        "airport_reference_point_longitude_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(
            line_dict["airport_surveillance_radar_latitude_formatted"]
        )
        longitude = h.coordinateToDecimal(
            line_dict["airport_surveillance_radar_longitude_formatted"]
        )

        line_dict["airport_surveillance_radar_latitude"] = latitude
        line_dict["airport_surveillance_radar_longitude"] = longitude

    keys = [
        "latitude_of_direction_finding_antenna_formatted",
        "longitude_of_direction_finding_antenna_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(
            line_dict["latitude_of_direction_finding_antenna_formatted"]
        )
        longitude = h.coordinateToDecimal(
            line_dict["longitude_of_direction_finding_antenna_formatted"]
        )

        line_dict["latitude_of_direction_finding_antenna"] = latitude
        line_dict["longitude_of_direction_finding_antenna"] = longitude

    return line_dict


def geometry_TWR_TWR7(line_dict: dict) -> dict:
    keys = [
        "airport_latitude_formatted",
        "airport_longitude_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["airport_latitude_formatted"])
        longitude = h.coordinateToDecimal(line_dict["airport_longitude_formatted"])

        line_dict["airport_latitude"] = latitude
        line_dict["airport_longitude"] = longitude

    return line_dict


def geometry_APT_APT(line_dict: dict) -> dict:
    keys = [
        "airport_reference_point_latitude_formatted",
        "airport_reference_point_longitude_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(
            line_dict["airport_reference_point_latitude_formatted"]
        )
        longitude = h.coordinateToDecimal(
            line_dict["airport_reference_point_longitude_formatted"]
        )

        line_dict["apt_latitude"] = latitude
        line_dict["apt_longitude"] = longitude

    return line_dict


def geometry_APT_RWY(line_dict: dict) -> dict:
    keys = [
        "base_latitude_of_physical_runway_end_formatted",
        "base_longitude_of_physical_runway_end_formatted",
        "reciprocal_latitude_of_physical_runway_end_formatted",
        "reciprocal_longitude_of_physical_runway_end_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        base_latitude = h.coordinateToDecimal(
            line_dict["base_latitude_of_physical_runway_end_formatted"]
        )
        base_longitude = h.coordinateToDecimal(
            line_dict["base_longitude_of_physical_runway_end_formatted"]
        )
        reciprocal_latitude = h.coordinateToDecimal(
            line_dict["reciprocal_latitude_of_physical_runway_end_formatted"]
        )
        reciprocal_longitude = h.coordinateToDecimal(
            line_dict["reciprocal_longitude_of_physical_runway_end_formatted"]
        )

        line_dict["base_latitude"] = base_latitude
        line_dict["base_longitude"] = base_longitude
        line_dict["reciprocal_latitude"] = reciprocal_latitude
        line_dict["reciprocal_longitude"] = reciprocal_longitude

    keys = [
        "base_latitude_at_displaced_threshold_formatted",
        "base_longitude_at_displaced_threshold_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        base_displaced_threshold_latitude = h.coordinateToDecimal(
            line_dict["base_latitude_at_displaced_threshold_formatted"]
        )
        base_displaced_threshold_longitude = h.coordinateToDecimal(
            line_dict["base_longitude_at_displaced_threshold_formatted"]
        )

        line_dict["base_displaced_threshold_latitude"] = (
            base_displaced_threshold_latitude
        )
        line_dict["base_displaced_threshold_longitude"] = (
            base_displaced_threshold_longitude
        )

    keys = [
        "reciprocal_latitude_at_displaced_threshold_formatted",
        "reciprocal_longitude_at_displaced_threshold_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        reciprocal_displaced_threshold_latitude = h.coordinateToDecimal(
            line_dict["reciprocal_latitude_at_displaced_threshold_formatted"]
        )
        reciprocal_displaced_threshold_longitude = h.coordinateToDecimal(
            line_dict["reciprocal_longitude_at_displaced_threshold_formatted"]
        )

        line_dict["reciprocal_displaced_threshold_latitude"] = (
            reciprocal_displaced_threshold_latitude
        )
        line_dict["reciprocal_displaced_threshold_longitude"] = (
            reciprocal_displaced_threshold_longitude
        )

    return line_dict


def geometry_AWOS_AWOS1(line_dict: dict) -> dict:
    keys = [
        "station_latitude_dd_mm_ss_ssssh",
        "station_longitude_ddd_mm_ss_ssssh",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["station_latitude_dd_mm_ss_ssssh"])
        longitude = h.coordinateToDecimal(
            line_dict["station_longitude_ddd_mm_ss_ssssh"]
        )

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_FIX_FIX1(line_dict: dict) -> dict:
    keys = [
        "geographical_latitude_of_the_fix",
        "geographical_longitude_of_the_fix",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["geographical_latitude_of_the_fix"])
        longitude = h.coordinateToDecimal(
            line_dict["geographical_longitude_of_the_fix"]
        )

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_NAV_NAV1(line_dict: dict) -> dict:
    keys = [
        "navaid_latitude_formatted",
        "navaid_longitude_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["navaid_latitude_formatted"])
        longitude = h.coordinateToDecimal(line_dict["navaid_longitude_formatted"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_PJA_PJA1(line_dict: dict) -> dict:
    keys = [
        "pja_latitude_formatted",
        "pja_longitude_formatted",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = h.coordinateToDecimal(line_dict["pja_latitude_formatted"])
        longitude = h.coordinateToDecimal(line_dict["pja_longitude_formatted"])

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


def geometry_WXL_WXL(line_dict: dict) -> dict:
    keys = [
        "latitude_of_the_weather_reporting_location",
        "longitude_of_the_weather_reporting_location",
    ]
    if check_for_position_data(line_dict, keys):
        latitude = line_dict["latitude_of_the_weather_reporting_location"]
        longitude = line_dict["longitude_of_the_weather_reporting_location"]

        if longitude and latitude:
            lat_deg = h.atoi(latitude[0:2])
            lat_min = h.atoi(latitude[2:4])
            lat_sec = h.atof(latitude[4:6] + "." + latitude[6:7])
            lat_declination = latitude[7:8]

            lon_deg = h.atoi(longitude[0:3])
            lon_min = h.atoi(longitude[3:5])
            lon_sec = h.atof(longitude[5:7] + "." + longitude[7:8])
            lon_declination = longitude[8:9]

        if not (lat_deg, lat_min, lat_sec, lon_deg, lon_min, lon_sec):
            print(f"Unable to convert point.")
            return None

        latitude = h.coordinateToDecimal2(lat_deg, lat_min, lat_sec, lat_declination)
        longitude = h.coordinateToDecimal2(lon_deg, lon_min, lon_sec, lon_declination)

        line_dict["latitude"] = latitude
        line_dict["longitude"] = longitude

    return line_dict


parsers = {
    "OBSTACLE": {
        "OBSTACLE": geometry_OBSTACLE_OBSTACLE,
    },
    "AFF": {
        "AFF1": geometry_AFF_AFF1,
        "AFF3": geometry_AFF_AFF3,
    },
    "APT": {
        "APT": geometry_APT_APT,
        "RWY": geometry_APT_RWY,
    },
    "ARB": {
        "ARB": geometry_ARB_ARB,
    },
    "ATS": {
        "ATS2": geometry_ATS_ATS2,
        "ATS3": geometry_ATS_ATS3,
    },
    "AWOS": {
        "AWOS1": geometry_AWOS_AWOS1,
    },
    "AWY": {
        "AWY2": geometry_AWY_AWY2,
        "AWY3": geometry_AWY_AWY3,
    },
    "COM": {
        "COM": geometry_COM_COM,
    },
    "FIX": {"FIX1": geometry_FIX_FIX1},
    "FSS": {
        "FSS": geometry_FSS_FSS,
    },
    "HPF": {
        "HP1": geometry_HPF_HP1,
    },
    "ILS": {
        "ILS2": geometry_ILS_ILS2,
        "ILS3": geometry_ILS_ILS3,
        "ILS4": geometry_ILS_ILS4,
        "ILS5": geometry_ILS_ILS5,
    },
    "MTR": {"MTR5": geometry_MTR_MTR5},
    "NAV": {"NAV1": geometry_NAV_NAV1},
    "PJA": {"PJA1": geometry_PJA_PJA1},
    "SSD": {"SSD": geometry_SSD_SSD},
    "STARDP": {"STARDP": geometry_STARDP_STARDP},
    "TWR": {
        "TWR1": geometry_TWR_TWR1,
        "TWR7": geometry_TWR_TWR7,
    },
    "WXL": {
        "WXL": geometry_WXL_WXL,
    },
}
