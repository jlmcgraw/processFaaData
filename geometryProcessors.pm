#!/usr/bin/perl
# Copyright (C) 2013  Jesse McGraw (jlmcgraw@gmail.com)

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see [http://www.gnu.org/licenses/].

package geometryProcessors;

use 5.018;
use strict;
use warnings;

use Params::Validate qw(:all);

use Exporter;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

use processFaaData;
$VERSION = 1.00;
@ISA     = qw(Exporter);
@EXPORT  = qw(
  geometry_AFF_AFF1
  geometry_AFF_AFF3
  geometry_APT_APT
  geometry_APT_RWY
  geometry_ARB_ARB
  geometry_ATS_ATS2
  geometry_ATS_ATS3
  geometry_AWOS_AWOS1
  geometry_AWY_AWY2
  geometry_AWY_AWY3
  geometry_COM_COM
  geometry_FIX_FIX1
  geometry_FSS_FSS
  geometry_HARFIX_HARFIX
  geometry_HPF_HP1
  geometry_ILS_ILS2
  geometry_ILS_ILS3
  geometry_ILS_ILS4
  geometry_ILS_ILS5
  geometry_MTR_MTR5
  geometry_NAV_NAV1
  geometry_NATFIX_NATFIX
  geometry_PJA_PJA1
  geometry_SSD_SSD
  geometry_STARDP_STARDP
  geometry_TWR_TWR1
  geometry_TWR_TWR7
  geometry_WXL_WXL
  geometry_OBSTACLE_OBSTACLE);

sub geometry_OBSTACLE_OBSTACLE {

    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude = &coordinatetodecimal2(
        $hashRef->{latitude_degrees}, $hashRef->{latitude_minutes},
        $hashRef->{latitude_seconds}, $hashRef->{latitude_hemisphere},
    );
    my $longitude = &coordinatetodecimal2(
        $hashRef->{longitude_degrees}, $hashRef->{longitude_minutes},
        $hashRef->{longitude_seconds}, $hashRef->{longitude_hemisphere},
    );

    # #and save in the hash as a POINT
    # $hashRef->{Geometry} = "POINT(" . $longitude . " " . $latitude . ")";
    $hashRef->{obstacle_latitude}  = $latitude;
    $hashRef->{obstacle_longitude} = $longitude;
}

sub geometry_AFF_AFF1 {

    # AIR ROUTE TRAFFIC CONTROL CENTER FACILITIES AND COMMUNICATIONS
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # #Calculate the decimal representation of lon/lat
    $hashRef->{latitude} =
      &coordinatetodecimal( $hashRef->{site_latitude_formatted} );

    $hashRef->{longitude} =
      &coordinatetodecimal( $hashRef->{site_longitude_formatted} );
}

sub geometry_AFF_AFF3 {

    # AIR ROUTE TRAFFIC CONTROL CENTER FACILITIES AND COMMUNICATIONS
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # #Calculate the decimal representation of lon/lat
    $hashRef->{latitude} =
      &coordinatetodecimal( $hashRef->{latitude_of_the_airport_formatted} );

    $hashRef->{longitude} =
      &coordinatetodecimal( $hashRef->{longitude_of_the_airport_formatted} );
}

sub geometry_ARB_ARB {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{latitude_of_the_boundary_point}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{longitude_of_the_boundary_point} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_ATS_ATS2 {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{navaid_facility_fix_latitude}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{navaid_facility_fix_longitude} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_ATS_ATS3 {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{navaid_facility_latitude}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{navaid_facility_longitude} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_AWY_AWY2 {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{navaid_facility_fix_latitude}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{navaid_facility_fix_longitude} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_AWY_AWY3 {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{navaid_facility_latitude}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{navaid_facility_longitude} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_COM_COM {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # associated_navaid_latitude:14
    # associated_navaid_longitude:14

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{communications_outlet_latitude}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{communications_outlet_longitude} );

    #and save in the hash

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_FSS_FSS {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # latitude_of_direction_finding_df_equipment:14
    # longitude_of_direction_finding_df_equipment:14
    # geographical_latitude_of_communication_facility_not_used_when_c:420
    # geographical_longitude_of_communication_facility_not_used_when:420

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{airport_latitude_fss_on_arpt}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{airport_longitude_fss_on_arpt} );

    my $latitudeNotOnAirport =
      &coordinatetodecimal(
        $hashRef->{latitude_when_fss_is_not_on_airport_see_f6},
      );
    my $longitudeNotOnAirport =
      &coordinatetodecimal(
        $hashRef->{longitude_when_fss_is_not_on_airport_see_f6} );

    if ( $latitude && $longitude ) {

        $hashRef->{latitude}  = $latitude;
        $hashRef->{longitude} = $longitude;
    }
    elsif ( $latitudeNotOnAirport && $longitudeNotOnAirport ) {

        $hashRef->{latitude}  = $latitudeNotOnAirport;
        $hashRef->{longitude} = $longitudeNotOnAirport;
    }
}

sub geometry_HARFIX_HARFIX {

    #TODO
    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # #Calculate the decimal representation of lon/lat
    # my $latitude =
    # &coordinatetodecimal(
    # $hashRef->{airport_reference_point_latitude_formatted},
    # );
    # my $longitude =
    # &coordinatetodecimal(
    # $hashRef->{airport_reference_point_longitude_formatted} );

    # # #and save in the hash as a POINT
    # # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    # $hashRef->{apt_latitude}  = $latitude;
    # $hashRef->{apt_longitude} = $longitude;
}

sub geometry_HPF_HP1 {

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # #Calculate the decimal representation of lon/lat
    $hashRef->{latitude_of_the_associated_fix} =
      &coordinatetodecimal(
        $hashRef->{latitude_of_the_associated_fix_dd_mm_ss_sssn} );

    $hashRef->{longitude_of_the_associated_fix} =
      &coordinatetodecimal(
        $hashRef->{longitude_of_the_associated_fix_ddd_mm_ss_sssw} );

    $hashRef->{latitude_of_the_associated_navaid} =
      &coordinatetodecimal(
        $hashRef->{latitude_of_the_associated_navaid_dd_mm_ss_sssn} );

    $hashRef->{longitude_of_the_associated_navaid} =
      &coordinatetodecimal(
        $hashRef->{longitude_of_the_associated_navaid_ddd_mm_ss_sssw} );

}

sub geometry_ILS_ILS2 {

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal(
        $hashRef->{latitude_of_localizer_antenna_formatted} );
    my $longitude =
      &coordinatetodecimal(
        $hashRef->{longitude_of_localizer_antenna_formatted} );

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;

}

sub geometry_ILS_ILS3 {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal(
        $hashRef->{latitude_of_glide_slope_transmitter_antenna_formatted} );
    my $longitude =
      &coordinatetodecimal(
        $hashRef->{longitude_of_glide_slope_transmitter_antenna_formatted} );

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_ILS_ILS4 {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal(
        $hashRef->{latitude_of_dme_transponder_antenna_formatted} );
    my $longitude =
      &coordinatetodecimal(
        $hashRef->{longitude_of_dme_transponder_antenna_formatted} );

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_ILS_ILS5 {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{latitude_of_marker_beacon_formatted} );
    my $longitude =
      &coordinatetodecimal( $hashRef->{longitude_of_marker_beacon_formatted} );

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_MTR_MTR5 {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
        $hashRef->{latitude} =
      &coordinateToDecimal3( $hashRef->{latitude_location_of_point}, );
   $hashRef->{longitude} =
      &coordinateToDecimal3( $hashRef->{longitude_location_of_point} );
      
}

sub geometry_NATFIX_NATFIX {

    #TODO
    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    # #Calculate the decimal representation of lon/lat
    # my $latitude =
    # &coordinatetodecimal(
    # $hashRef->{airport_reference_point_latitude_formatted},
    # );
    # my $longitude =
    # &coordinatetodecimal(
    # $hashRef->{airport_reference_point_longitude_formatted} );

    # # #and save in the hash as a POINT
    # # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    # $hashRef->{apt_latitude}  = $latitude;
    # $hashRef->{apt_longitude} = $longitude;
}

sub geometry_SSD_SSD {
    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude  = $hashRef->{fix_navaid_airport_latitude_format_xddmmsst};
    my $longitude = $hashRef->{fix_navaid_airport_longitude_format_xdddmmsst};

    if ( $longitude && $latitude ) {

        # say $longitude . " " . $latitude;
        # ( $deg, $min, $sec, $declination )
        my $latD = substr( $latitude, 1, 2 );
        my $latM = substr( $latitude, 3, 2 );
        my $latS = substr( $latitude, 5, 2 ) . "." . substr( $latitude, 7, 1 );
        my $latDeclination = substr( $latitude, 0, 1 );

        $latitude =
          coordinatetodecimal2( $latD, $latM, $latS, $latDeclination );

        my $lonD = substr( $longitude, 1, 3 );
        my $lonM = substr( $longitude, 4, 2 );
        my $lonS =
          substr( $longitude, 6, 2 ) . "." . substr( $longitude, 8, 1 );
        my $lonDeclination = substr( $longitude, 0, 1 );

        $longitude =
          coordinatetodecimal2( $lonD, $lonM, $lonS, $lonDeclination );
    }

    # 5614486N 13438533W
    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_STARDP_STARDP {

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude  = $hashRef->{fix_navaid_airport_latitude_format_xddmmsst};
    my $longitude = $hashRef->{fix_navaid_airport_longitude_format_xdddmmsst};

    if ( $longitude && $latitude ) {

        # say $longitude . " " . $latitude;
        # ( $deg, $min, $sec, $declination )
        my $latDeclination = substr( $latitude, 0, 1 );
        my $latD = substr( $latitude, 1, 2 );
        my $latM = substr( $latitude, 3, 2 );
        my $latS = substr( $latitude, 5, 2 ) . "." . substr( $latitude, 7, 1 );

        $latitude =
          coordinatetodecimal2( $latD, $latM, $latS, $latDeclination );
        
        my $lonDeclination = substr( $longitude, 0, 1 );
        my $lonD = substr( $longitude, 1, 3 );
        my $lonM = substr( $longitude, 4, 2 );
        my $lonS =
          substr( $longitude, 6, 2 ) . "." . substr( $longitude, 8, 1 );

        $longitude =
          coordinatetodecimal2( $lonD, $lonM, $lonS, $lonDeclination );
    
    }

    # 5614486N 13438533W
    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_TWR_TWR1 {

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Airport Reference Point
    $hashRef->{airport_reference_point_latitude} =
      &coordinatetodecimal(
        $hashRef->{airport_reference_point_latitude_formatted} );

    $hashRef->{airport_reference_point_longitude} =
      &coordinatetodecimal(
        $hashRef->{airport_reference_point_longitude_formatted} );

    #Airport Surveillance Radar
    $hashRef->{airport_surveillance_radar_latitude} =
      &coordinatetodecimal(
        $hashRef->{airport_surveillance_radar_latitude_formatted} );

    $hashRef->{airport_surveillance_radar_longitude} =
      &coordinatetodecimal(
        $hashRef->{airport_surveillance_radar_longitude_formatted} );

    #Direction Finding Antenna
    $hashRef->{latitude_of_direction_finding_antenna} =
      &coordinatetodecimal(
        $hashRef->{latitude_of_direction_finding_antenna_formatted} );

    $hashRef->{longitude_of_direction_finding_antenna} =
      &coordinatetodecimal(
        $hashRef->{longitude_of_direction_finding_antenna_formatted} );
}

sub geometry_TWR_TWR7 {

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    $hashRef->{airport_latitude} =
      &coordinatetodecimal( $hashRef->{airport_latitude_formatted} );
    $hashRef->{airport_longitude} =
      &coordinatetodecimal( $hashRef->{airport_longitude_formatted} );
}

sub geometry_APT_APT {

    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal(
        $hashRef->{airport_reference_point_latitude_formatted},
      );
    my $longitude =
      &coordinatetodecimal(
        $hashRef->{airport_reference_point_longitude_formatted} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{apt_latitude}  = $latitude;
    $hashRef->{apt_longitude} = $longitude;
}

sub geometry_APT_RWY {

    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $base_latitude =
      &coordinatetodecimal(
        $hashRef->{base_latitude_of_physical_runway_end_formatted} );
    my $base_longitude =
      &coordinatetodecimal(
        $hashRef->{base_longitude_of_physical_runway_end_formatted} );

    my $reciprocal_latitude =
      &coordinatetodecimal(
        $hashRef->{reciprocal_latitude_of_physical_runway_end_formatted} );
    my $reciprocal_longitude =
      &coordinatetodecimal(
        $hashRef->{reciprocal_longitude_of_physical_runway_end_formatted} );

    #Displaced threshold endpoints
    my $base_displaced_threshold_latitude =
      &coordinatetodecimal(
        $hashRef->{base_latitude_at_displaced_threshold_formatted} );
    my $base_displaced_threshold_longitude =
      &coordinatetodecimal(
        $hashRef->{base_longitude_at_displaced_threshold_formatted} );

    my $reciprocal_displaced_threshold_latitude =
      &coordinatetodecimal(
        $hashRef->{reciprocal_latitude_at_displaced_threshold_formatted} );
    my $reciprocal_displaced_threshold_longitude =
      &coordinatetodecimal(
        $hashRef->{reciprocal_longitude_at_displaced_threshold_formatted} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "LINESTRING("
    # . $base_longitude . " " . $base_latitude
    # . " , "
    # . $reciprocal_longitude . " " . $reciprocal_latitude
    # . ")";
    $hashRef->{base_latitude}        = $base_latitude;
    $hashRef->{base_longitude}       = $base_longitude;
    $hashRef->{reciprocal_latitude}  = $reciprocal_latitude;
    $hashRef->{reciprocal_longitude} = $reciprocal_longitude;

    $hashRef->{base_displaced_threshold_latitude} =
      $base_displaced_threshold_latitude;
    $hashRef->{base_displaced_threshold_longitude} =
      $base_displaced_threshold_longitude;
    $hashRef->{reciprocal_displaced_threshold_latitude} =
      $reciprocal_displaced_threshold_latitude;
    $hashRef->{reciprocal_displaced_threshold_longitude} =
      $reciprocal_displaced_threshold_longitude;

    # my $runwayLineTrueHeading =
    # round( trueHeading( $_x1, $_y1, $_x2, $_y2 ) );
}

sub geometry_AWOS_AWOS1 {

    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{station_latitude_dd_mm_ss_ssssh}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{station_longitude_ddd_mm_ss_ssssh} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_FIX_FIX1 {

    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{geographical_latitude_of_the_fix}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{geographical_longitude_of_the_fix} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_NAV_NAV1 {

    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude =
      &coordinatetodecimal( $hashRef->{navaid_latitude_formatted}, );
    my $longitude =
      &coordinatetodecimal( $hashRef->{navaid_longitude_formatted} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_PJA_PJA1 {

    # my $hashRef = shift;

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude  = &coordinatetodecimal( $hashRef->{pja_latitude_formatted}, );
    my $longitude = &coordinatetodecimal( $hashRef->{pja_longitude_formatted} );

    # #and save in the hash as a POINT
    # $hashRef->{geometry} = "POINT(" . $longitude . " " . $latitude . ")";

    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

sub geometry_WXL_WXL {

    my ($hashRef) = validate_pos( @_, { type => HASHREF } );

    #Calculate the decimal representation of lon/lat
    my $latitude  = $hashRef->{latitude_of_the_weather_reporting_location};
    my $longitude = $hashRef->{longitude_of_the_weather_reporting_location};

    if ( $longitude && $latitude ) {

        # ( $deg, $min, $sec, $declination )
        my $latD = substr( $latitude, 0, 2 );
        my $latM = substr( $latitude, 2, 2 );
        my $latS = substr( $latitude, 4, 2 ) . "." . substr( $latitude, 6, 1 );
        my $latDeclination = substr( $latitude, 7, 1 );

        $latitude =
          coordinatetodecimal2( $latD, $latM, $latS, $latDeclination );

        my $lonD = substr( $longitude, 0, 3 );
        my $lonM = substr( $longitude, 3, 2 );
        my $lonS =
          substr( $longitude, 5, 2 ) . "." . substr( $longitude, 7, 1 );
        my $lonDeclination = substr( $longitude, 8, 1 );

        $longitude =
          coordinatetodecimal2( $lonD, $lonM, $lonS, $lonDeclination );
    }

    # 5614486N 13438533W
    $hashRef->{latitude}  = $latitude;
    $hashRef->{longitude} = $longitude;
}

# sub trueHeading {
# # my ( ) = @_;
# my ( $_x1, $_y1, $_x2, $_y2) = validate_pos( @_, { type => SCALAR, type => SCALAR,type => SCALAR,type => SCALAR,} );
# return rad2deg( pi / 2 - atan2( $_y2 - $_y1, $_x2 - $_x1 ) );
# }

# sub WGS84toGoogleBing {
# # my (  ) = @_;
# my ( $lon, $lat) = validate_pos( @_, { type => SCALAR, type => SCALAR} );
# my $x = $lon * 20037508.34 / 180;
# my $y = log( tan( ( 90 + $lat ) * pi / 360 ) ) / ( pi / 180 );
# $y = $y * 20037508.34 / 180;
# return ( $x, $y );
# }
1;
