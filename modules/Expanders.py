from modules.Helpers import atoi

from typing import Tuple, Union
import re


def parse_expander(base_file: str, record_type: str, line_dict: dict) -> dict | None:
    result = None
    base_group = parsers.get(base_file)
    if base_group != None:
        function_reference = base_group.get(record_type)
        if function_reference != None:
            result = function_reference(line_dict)

    return result


def safe_split(regex: str, input_string: str) -> Union[Tuple[str, str], Tuple[str]]:
    parts = re.split(regex, input_string, 2)
    if len(parts) == 3:
        element, _, specific = parts
        return element, specific
    else:
        return (input_string,)


remark_element_name = {
    "A1": "ASSOCIATED CITY NAME",
    "A2": "OFFICIAL FACILITY NAME",
    "A3": "CENTRAL BUSINESS DISTRICT",
    "A4": "ASSOCIATED STATE",
    "A5": "ASSOCIATED COUNTY",
    "A6": "FAA REGION CODE",
    "A7": "AERONAUTICAL SECTIONAL CHART ON WHICH FACILITY",
    "A10": "AIRPORT OWNERSHIP TYPE",
    "A11": "FACILITY OWNERS NAME",
    "A12A": "OWNERS CITY, STATE AND ZIP CODE",
    "A12": "OWNERS ADDRESS",
    "A13": "OWNERS PHONE NUMBER",
    "A14": "FACILITY MANAGERS NAME",
    "A15A": "MANAGERS CITY, STATE AND ZIP CODE",
    "A15": "MANAGERS ADDRESS",
    "A16": "MANAGERS PHONE NUMBER",
    "A17": "AIRPORT ATTENDANCE SCHEDULE",
    "A18": "FACILITY USE",
    "A19A": "AIRPORT REFERENCE POINT DETERMINATION METHOD",
    "A19": "AIRPORT REFERENCE POINT LATITUDE (FORMATTED)",
    "A19S": "AIRPORT REFERENCE POINT LATITUDE (SECONDS)",
    "A20": "AIRPORT REFERENCE POINT LONGITUDE (FORMATTED)",
    "A20S": "AIRPORT REFERENCE POINT LONGITUDE (SECONDS)",
    "A21": "AIRPORT ELEVATION ",
    "A22": "LAND AREA COVERED BY AIRPORT (ACRES)",
    "A23": "RIGHT HAND TRAFFIC PATTERN FOR LANDING AIRCRAFT",
    "A24": "LANDING FEE CHARGED TO NON-COMMERCIAL USERS OF",
    "A25": "NPIAS/FEDERAL AGREEMENTS CODE",
    "A26": "AIRPORT ARFF CERTIFICATION TYPE AND DATE",
    "A30A": "RUNWAY END IDENTIFIER",
    "A30": "RUNWAY IDENTIFICATION",
    "A31": "PHYSICAL RUNWAY LENGTH (NEAREST FOOT)",
    "A32": "PHYSICAL RUNWAY WIDTH (NEAREST FOOT)",
    "A33": "RUNWAY SURFACE TYPE AND CONDITION",
    "A34": "RUNWAY SURFACE TREATMENT",
    "A35": "RUNWAY WEIGHT-BEARING CAPACITY FOR SINGLE WHEEL",
    "A36": "RUNWAY WEIGHT-BEARING CAPACITY FOR DUAL WHEEL",
    "A37": "RUNWAY WEIGHT-BEARING CAPACITY FOR TWO DUAL WHEELS",
    "A38": "RUNWAY WEIGHT-BEARING CAPACITY FOR TWO DUAL WHEELS",
    "A39": "PAVEMENT CLASSIFICATION NUMBER (PCN)",
    "A40": "RUNWAY LIGHTS EDGE INTENSITY",
    "A42": "RUNWAY MARKINGS ",
    "A43": "VISUAL GLIDE SLOPE INDICATORS",
    "A44": "THRESHOLD CROSSING HEIGHT (FEET AGL)",
    "A45": "VISUAL GLIDE PATH ANGLE (HUNDREDTHS OF DEGREES)",
    "A46A": "RUNWAY END TOUCHDOWN LIGHTS AVAILABILITY",
    "A46": "RUNWAY CENTERLINE LIGHTS AVAILABILITY",
    "A47A": "RUNWAY VISIBILITY VALUE EQUIPMENT (RVV)",
    "A47": "RUNWAY VISUAL RANGE EQUIPMENT (RVR)",
    "A48": "RUNWAY END IDENTIFIER LIGHTS (REIL) AVAILABILITY",
    "A49": "APPROACH LIGHT SYSTEM",
    "A50": "FAA CFR PART 77 (OBJECTS AFFECTING NAVIGABLE AIRSPACE)",
    "A51": "DISPLACED THRESHOLD - LENGTH IN FEET FROM END",
    "A52": "CONTROLLING OBJECT DESCRIPTION",
    "A53": "CONTROLLING OBJECT MARKED/LIGHTED",
    "A54": "CONTROLLING OBJECT HEIGHT ABOVE RUNWAY",
    "A55": "CONTROLLING OBJECT DISTANCE FROM RUNWAY END",
    "A56": "CONTROLLING OBJECT CENTERLINE OFFSET",
    "A57": "CONTROLLING OBJECT CLEARANCE SLOPE",
    "A58": "CONTROLLING OBJECT",
    "A60": "TAKEOFF RUN AVAILABLE (TORA), IN FEET",
    "A61": "TAKEOFF DISTANCE AVAILABLE (TODA), IN FEET",
    "A62": "ACLT STOP DISTANCE AVAILABLE (ASDA), IN FEET",
    "A63": "LANDING DISTANCE AVAILABLE (LDA), IN FEET",
    "A6A": "FAA DISTRICT OR FIELD OFFICE CODE",
    "A70": "FUEL TYPES AVAILABLE FOR PUBLIC USE",
    "A71": "AIRFRAME REPAIR SERVICE AVAILABILITY/TYPE",
    "A72": "POWER PLANT (ENGINE) REPAIR AVAILABILITY/TYPE",
    "A73": "TYPE OF BOTTLED OXYGEN AVAILABLE (VALUE REPRESENTS",
    "A74": "TYPE OF BULK OXYGEN AVAILABLE (VALUE REPRESENTS",
    "A75": "TRANSIENT STORAGE FACILITIES",
    "A76": "OTHER AIRPORT SERVICES AVAILABLE",
    "A80": "LENS COLOR OF OPERABLE BEACON LOCATED ON THE AIRPORT",
    "A81": "AIRPORT LIGHTING",
    "A82": "UNICOM FREQUENCY AVAILABLE AT THE AIRPORT",
    "A83": "WIND INDICATOR",
    "A84": "SEGMENTED CIRCLE AIRPORT MARKER SYSTEM ON THE AIRPORT",
    "A85": "AIR TRAFFIC CONTROL TOWER LOCATED ON AIRPORT",
    "A86A": "ALTERNATE FSS ",
    "A86": "TIE-IN FLIGHT SERVICE STATION (FSS)",
    "A87": "TIE-IN FSS PHYSICALLY LOCATED ON FACILITY",
    "A88": "LOCAL PHONE NUMBER FROM AIRPORT TO FSS",
    "A89": "TOLL FREE PHONE NUMBER FROM AIRPORT TO FSS",
    "A90": "SINGLE ENGINE GENERAL AVIATION AIRCRAFT",
    "A91": "MULTI ENGINE GENERAL AVIATION AIRCRAFT",
    "A92": "JET ENGINE GENERAL AVIATION AIRCRAFT",
    "A93": "GENERAL AVIATION HELICOPTER",
    "A94": "OPERATIONAL GLIDERS",
    "A95": "OPERATIONAL MILITARY AIRCRAFT (INCLUDING HELICOPTERS)",
    "A96": "ULTRALIGHT AIRCRAFT",
    "A100": "COMMERCIAL SERVICES",
    "A101": "COMMUTER SERVICES",
    "A102": "AIR TAXI",
    "A103": "GENERAL AVIATION LOCAL OPERATIONS",
    "A104": "GENERAL AVIATION ITINERANT OPERATIONS",
    "A105": "MILITARY AIRCRAFT OPERATIONS",
    "A110": "GENERAL",
    "A111": "AGENCY/GROUP PERFORMING PHYSICAL INSPECTION",
    "A112": "LAST PHYSICAL INSPECTION DATE (MMDDYYYY)",
    "A113": "LAST DATE INFORMATION REQUEST WAS COMPLETED",
    "E2B": "IDENTIFIER OF THE FACILITY RESPONSIBLE FOR",
    "E3A": "TOLL FREE PHONE NUMBER FROM AIRPORT TO",
    "E7": "LOCATION IDENTIFIER",
    "E28": "MAGNETIC VARIATION",
    "E40": "RUNWAY END GRADIENT",
    "E46": "RUNWAY END TRUE ALIGNMENT",
    "E60": "TYPE OF AIRCRAFT ARRESTING DEVICE",
    "E67": "VERTICAL DATUM?",
    "E68": "LATITUDE OF PHYSICAL RUNWAY END (FORMATTED)",
    "E68S": "LATITUDE OF PHYSICAL RUNWAY END (SECONDS)",
    "E69": "LONGITUDE OF PHYSICAL RUNWAY END (FORMATTED)",
    "E69S": "LONGITUDE OF PHYSICAL RUNWAY END (SECONDS)",
    "E70": "ELEVATION (FEET MSL) AT PHYSICAL RUNWAY END",
    "E79": "FACILITY HAS BEEN DESIGNATED BY THE U.S. TREASURY",
    "E80": "FACILITY HAS BEEN DESIGNATED BY THE U.S. TREASURY",
    "E80A": "CUSTOMS?",
    "E100": "COMMON TRAFFIC ADVISORY FREQUENCY (CTAF)",
    "E111": "AIRPORT AIRSPACE ANALYSIS DETERMINATION",
    "E115": "FACILITY HAS MILITARY/CIVIL JOINT USE AGREEMENT",
    "E116": "AIRPORT HAS ENTERED INTO AN AGREEMENT THAT",
    "E139": "AVAILABILITY OF NOTAM D SERVICE AT AIRPORT",
    "E146A": "BOUNDARY ARTCC IDENTIFIER",
    "E146B": "BOUNDARY ARTCC (FAA) COMPUTER IDENTIFIER",
    "E146C": "BOUNDARY ARTCC NAME",
    "E147": "TRAFFIC PATTERN ALTITUDE (WHOLE FEET AGL)",
    "E155": "AIRPORT INSPECTION METHOD",
    "E156A": "RESPONSIBLE ARTCC IDENTIFIER",
    "E156B": "RESPONSIBLE ARTCC (FAA) COMPUTER IDENTIFIER",
    "E156C": "RESPONSIBLE ARTCC NAME",
    "E157": "AIRPORT ACTIVATION DATE (MM/YYYY)",
    "E160": "ELEVATION AT DISPLACED THRESHOLD (FEET MSL)",
    "E161": "LATITUDE AT DISPLACED THRESHOLD (FORMATTED)",
    "E161S": "LATITUDE AT DISPLACED THRESHOLD (SECONDS)",
    "E162": "LONGITUDE AT DISPLACED THRESHOLD (FORMATTED)",
    "E162S": "LONGITUDE AT DISPLACED THRESHOLD (SECONDS)",
    "E163": "ELEVATION AT TOUCHDOWN ZONE (FEET MSL)",
    "I22": "INSTRUMENT LANDING SYSTEM (ILS) TYPE",
}

navaid_types = {
    "C": "VORTAC",
    "D": "VOR/DME",
    "F": "FAN MARKER",
    "L": "LFR",
    "M": "MARINE NDB",
    "MD": "MARINE NDB/DME",
    "O": "VOT",
    "OD": "DME",
    "R": "NDB",
    "RD": "NDB/DME",
    "T": "TACAN",
    "U": "UHF NDB",
    "V": "VOR",
}

faa_region_code = {
    "AAL": "ALASKA",
    "ACE": "CENTRAL",
    "AEA": "EASTERN",
    "AGL": "GREAT LAKES",
    "AIN": "INTERNATIONAL",
    "ANE": "NEW ENGLAND",
    "ANM": "NORTHWEST MOUNTAIN",
    "ASO": "SOUTHERN",
    "ASW": "SOUTHWEST",
    "AWP": "WESTERN-PACIFIC",
}

airport_ownership_type = {
    "PU": "PUBLICLY OWNED",
    "PR": "PRIVATELY OWNED",
    "MA": "AIR FORCE OWNED",
    "MN": "NAVY OWNED",
    "MR": "ARMY OWNED",
    "CG": "COAST GUARD OWNED",
}

airport_status_code = {
    "CI": "CLOSED INDEFINITELY",
    "CP": "CLOSED PERMANENTLY",
    "O": "OPERATIONAL",
}

facility_type = {
    "ARSR": "AIR ROUTE SURVEILLANCE RADAR",
    "ARTCC": "AIR ROUTE TRAFFIC CONTROL CENTER",
    "CERAP": "CENTER RADAR APPROACH CONTROL FACILITY",
    "RCAG": "REMOTE COMMUNICATIONS, AIR/GROUND",
    "SECRA": "SECONDARY RADAR",
}

fuel_types = {
    "80": "GRADE 80 GASOLINE (RED)",
    "100": "GRADE 100 GASOLINE (GREEN)",
    "100LL": "GRADE 100LL GASOLINE (LOW LEAD BLUE)",
    "115": "GRADE 115 GASOLINE (PURPLE)",
    "A": "JET A - KEROSENE, FREEZE POINT -40C",
    "A1": "JET A-1 - KEROSENE, FREEZE POINT -50C",
    "A1+": "JET A-1 - KEROSENE, WITH ICING INHIBITOR FREEZE POINT -50C",
    "B": "JET B - WIDE-CUT TURBINE FUEL, FREEZE POINT -50C",
    "B+": "JET B - WIDE-CUT TURBINE FUEL WITH ICING INHIBITOR, FREEZE POINT -50C",
    "MOGAS": "AUTOMOTIVE GASOLINE",
}

lens_color_of_operable_beacon_located_on_the_airport = {
    "CG": "CLEAR-GREEN (LIGHTED LAND AIRPORT)",
    "CY": "CLEAR-YELLOW (LIGHTED SEAPLANE BASE)",
    "CGY": "CLEAR-GREEN-YELLOW (HELIPORT)",
    "SCG": "SPLIT-CLEAR-GREEN (LIGHTED MILITARY AIRPORT)",
    "C": "CLEAR (UNLIGHTED LAND AIRPORT)",
    "Y": "YELLOW (UNLIGHTED SEAPLANE BASE)",
    "G": "GREEN (LIGHTED LAND AIRPORT)",
    "N": "NONE",
}

other_airport_services_available = {
    "AFRT": "AIR FREIGHT SERVICES",
    "AGRI": "CROP DUSTING SERVICES",
    "AMB": "AIR AMBULANCE SERVICES",
    "AVNCS": "AVIONICS",
    "BCHGR": "BEACHING GEAR",
    "CARGO": "CARGO HANDLING SERVICES",
    "CHTR": "CHARTER SERVICE",
    "GLD": "GLIDER SERVICE",
    "INSTR": "PILOT INSTRUCTION",
    "PAJA": "PARACHUTE JUMP ACTIVITY",
    "RNTL": "AIRCRAFT RENTAL",
    "SALES": "AIRCRAFT SALES",
    "SURV": "ANNUAL SURVEYING",
    "TOW": "GLIDER TOWING SERVICES",
}

visual_glide_slope_indicators = {
    "S2L": "2-BOX SAVASI ON LEFT SIDE OF RUNWAY",
    "S2R": "2-BOX SAVASI ON RIGHT SIDE OF RUNWAY",
    "V2L": "2-BOX VASI ON LEFT SIDE OF RUNWAY",
    "V2R": "2-BOX VASI ON RIGHT SIDE OF RUNWAY",
    "V4L": "4-BOX VASI ON LEFT SIDE OF RUNWAY",
    "V4R": "4-BOX VASI ON RIGHT SIDE OF RUNWAY",
    "V6L": "6-BOX VASI ON LEFT SIDE OF RUNWAY",
    "V6R": "6-BOX VASI ON RIGHT SIDE OF RUNWAY",
    "V12": "12-BOX VASI ON BOTH SIDES OF RUNWAY",
    "V16": "16-BOX VASI ON BOTH SIDES OF RUNWAY",
    "P2L": "2-LGT PAPI ON LEFT SIDE OF RUNWAY",
    "P2R": "2-LGT PAPI ON RIGHT SIDE OF RUNWAY",
    "P4L": "4-LGT PAPI ON LEFT SIDE OF RUNWAY",
    "P4R": "4-LGT PAPI ON RIGHT SIDE OF RUNWAY",
    "NSTD": "NONSTANDARD VASI SYSTEM",
    "PVT": "PRIVATELY OWNED APPROACH SLOPE INDICATOR LIGHT SYSTEM ON A PUBLIC USE AIRPORT THAT IS INTENDED FOR PRIVATE USE ONLY",
    "VAS": "NON-SPECIFIC VASI SYSTEM",
    "NONE": "NO APPROACH SLOPE LIGHT SYSTEM",
    "N": "NO APPROACH SLOPE LIGHT SYSTEM",
    "TRIL": "TRI-COLOR VASI ON LEFT SIDE OF RUNWAY",
    "TRIR": "TRI-COLOR VASI ON RIGHT SIDE OF RUNWAY",
    "PSIL": "PULSATING/STEADY BURNING VASI ON LEFT SIDE OF RUNWAY",
    "PSIR": "PULSATING/STEADY BURNING VASI ON RIGHT SIDE OF RUNWAY",
    "PNIL": "SYSTEM OF PANELS ON LEFT SIDE OF RUNWAY THAT MAY OR MAY NOT BE LIGHTED",
    "PNIR": "SYSTEM OF PANELS ON RIGHT SIDE OF RUNWAY THAT MAY OR MAY NOT BE LIGHTED",
}


def expand_AFF_AFF1(line_dict: dict) -> dict:
    line_type = line_dict["facility_type"]
    type = facility_type.get(line_type)
    if type != None:
        line_dict["facility_type"] = facility_type[line_type]
    else:
        print(f"No facility type for {line_type}")
    return line_dict

    # ORIGINAL PERL:
    # Substitute expanded text into the original hash
    # $hashRef->{facility_type} = $facility_type{"$hashRef->{facility_type}"};

    # # say $hashRef->{FACILITY_TYPE};
    # # #Calculate the decimal representation of lon/lat
    # # my $latitude = &coordinateToDecimal2(
    # # $hashRef->{Latitude_Degrees}, $hashRef->{Latitude_Minutes},
    # # $hashRef->{Latitude_Seconds}, $hashRef->{Latitude_Hemisphere},
    # # );
    # # my $longitude = &coordinateToDecimal2(
    # # $hashRef->{Longitude_Degrees}, $hashRef->{Longitude_Minutes},
    # # $hashRef->{Longitude_Seconds}, $hashRef->{Longitude_Hemisphere},
    # # );


#
# # # #and save in the hash as a POINT
# # # $hashRef->{Geometry} = "POINT(" . $longitude . " " . $latitude . ")";
# # $hashRef->{OBSTACLE_latitude}= $latitude;
# # $hashRef->{OBSTACLE_longitude}= $longitude;


def expand_APT_APT1() -> None:
    # All of the original Perl was commented out.
    pass

    # ORIGINAL PERL:
    # my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # say $hashRef->{FACILITY_TYPE};
    # #Calculate the decimal representation of lon/lat
    # my $latitude = &coordinateToDecimal2(
    # $hashRef->{Latitude_Degrees}, $hashRef->{Latitude_Minutes},
    # $hashRef->{Latitude_Seconds}, $hashRef->{Latitude_Hemisphere},
    # );
    # my $longitude = &coordinateToDecimal2(
    # $hashRef->{Longitude_Degrees}, $hashRef->{Longitude_Minutes},
    # $hashRef->{Longitude_Seconds}, $hashRef->{Longitude_Hemisphere},
    # );

    # # #and save in the hash as a POINT
    # # $hashRef->{Geometry} = "POINT(" . $longitude . " " . $latitude . ")";
    # $hashRef->{OBSTACLE_latitude}= $latitude;
    # $hashRef->{OBSTACLE_longitude}= $longitude;


def expand_ILS_ILS1(line_dict: dict) -> dict:
    var_dir = line_dict["the_magnetic_variation_at_the_ils_facility"][-1:]
    mag_var = atoi(line_dict["the_magnetic_variation_at_the_ils_facility"][:-1])
    if var_dir == "W":
        mag_var = -(mag_var)
    line_dict["the_magnetic_variation_at_the_ils_facility_expanded"] = mag_var
    return line_dict

    # ORIGINAL PERL:
    # my $vardir = substr( $hashRef->{the_magnetic_variation_at_the_ils_facility}, -1, 1 )
    # my $magneticVariation = substr( $hashRef->{the_magnetic_variation_at_the_ils_facility}, 0, 2 )
    # $magneticVariation = -($magneticVariation) if $vardir eq "W"
    # $hashRef->{the_magnetic_variation_at_the_ils_facility_expanded} = $magneticVariation


def expand_APT_RMK(line_dict: dict) -> dict:
    remark_element_field = line_dict["remark_element_name"]
    split_values = safe_split(r"-|\*|\s", remark_element_field)

    if len(split_values) == 2:
        element = split_values[0]
        specific = split_values[1]
        line_dict["remark_element_name"] = remark_element_name[element] + "-" + specific
    else:
        print(f"Unknown Element: {remark_element_field} -> {line_dict['remark_text']}")

    return line_dict

    # ORIGINAL PERL:
    # my $remark_element_name_field = $hashRef->{remark_element_name};
    # my ( $element, $specific ) = split( /-|\*|\s/, $remark_element_name_field, 2 );
    # if ( exists $remark_element_name{$element} ) {
    #     #Avoid having to do this
    #     no warnings 'uninitialized';
    #     $hashRef->{remark_element_name_expanded} =
    #       $remark_element_name{$element} . "-" . $specific;
    # } else {
    #     no warnings 'uninitialized';
    #     say
    #       "Unknown Element: $remark_element_name_field -> $element : $specific : "
    #       . $hashRef->{remark_text};
    #     $hashRef->{remark_element_name_expanded} =
    #       "Unknown Element: $element : $specific";
    # }

    # say $hashRef->{FACILITY_TYPE};
    # #Calculate the decimal representation of lon/lat
    # my $latitude = &coordinateToDecimal2(
    # $hashRef->{Latitude_Degrees}, $hashRef->{Latitude_Minutes},
    # $hashRef->{Latitude_Seconds}, $hashRef->{Latitude_Hemisphere},
    # );
    # my $longitude = &coordinateToDecimal2(
    # $hashRef->{Longitude_Degrees}, $hashRef->{Longitude_Minutes},
    # $hashRef->{Longitude_Seconds}, $hashRef->{Longitude_Hemisphere},
    # );

    # # #and save in the hash as a POINT
    # # $hashRef->{Geometry} = "POINT(" . $longitude . " " . $latitude . ")";
    # $hashRef->{OBSTACLE_latitude}= $latitude;
    # $hashRef->{OBSTACLE_longitude}= $longitude;


def expand_ANR_ANR2() -> None:
    # All of the original Perl was commented out.
    pass

    # ORIGINAL PERL (All of this was commented out):
    # sub expand_ANR_ANR2 {
    # my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # $hashRef->{FACILITY_TYPE} = $facility_type{"$hashRef->{FACILITY_TYPE}"};
    # say $hashRef->{FACILITY_TYPE};

    # }


parsers = {
    "AFF": {"AFF1": expand_AFF_AFF1},
    "APT": {"APT1": expand_APT_APT1, "RMK": expand_APT_RMK},
    "ILS": {"ILS1": expand_ILS_ILS1},
}
