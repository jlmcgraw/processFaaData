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

package processFaaData;

use 5.010;
use strict;
use warnings;

use Params::Validate qw(:all);

use Exporter;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

$VERSION = 1.00;
@ISA     = qw(Exporter);
@EXPORT =
  qw(trim rtrim ltrim coordinatetodecimal coordinatetodecimal2 coordinateToDecimal3 is_vhf coordinateToDecimalCifpFormat);

#@EXPORT_OK   = qw(rtrim ltrim coordinatetodecimal);

my $debug = 0;

#sub trim {
#    my @out = @_;
#    for (@out) {
#        s/^\s+//;          # trim left
#        s/\s+$//;          # trim right
#    }
#    return @out == 1
#              ? $out[0]   # only one to return
#              : @out;     # or many
#}

sub trim {

    # 1. trim leading and trailing white space
    # 2. collapse internal whitespace to single space each
    # 3. take input from $_ if no arguments given
    # 4. join return list into single scalar with intervening spaces
    #     if return is scalar context

    my @out = @_ ? @_ : $_;
    $_ = join( ' ', split(' ') ) for @out;
    return wantarray ? @out : "@out";
}

# Right trim function to remove trailing whitespace
sub rtrim($) {
    my $string = shift;
    $string =~ s/\s+$//;
    return $string;
}

# Left trim function to remove leading whitespace
sub ltrim($) {
    my $string = shift;
    $string =~ s/^\s+//;
    return $string;
}

sub is_vhf($) {
    my $freq = shift;
    if ( $freq =~ m/(1[1-3][0-9]\.\d{1,3})/ && ( $1 >= 118 && $1 < 137 ) ) {

        # print "$freq is VHF";
        return 1;
    }
    else {
        #print "$freq is not VHF";
        return 0;
    }

}

sub coordinatetodecimal {

    # my ($coordinate) = @_;
    #Validate and set input parameters to this function
    my ($coordinate) = validate_pos( @_, { type => SCALAR } );
    my ( $deg, $min, $sec, $signeddegrees, $declination );

    #Remove any whitespace
    $coordinate =~ s/\s//g;

    $declination = substr( $coordinate, -1, 1 );

    return "0" if !( $declination =~ /[NSEW]/ );

    $coordinate =~ m/^(\d{1,3})-/;

    #print $1;
    $deg = $1 / 1;
    $coordinate =~ m/-(\d{2})-/;

    #print $1;
    $min = $1 / 60;
    $coordinate =~ m/-(\d{2}\.\d+)/;

    #print $1;
    $sec = $1 / 3600;

    $signeddegrees = ( $deg + $min + $sec );

    if ( ( $declination eq "S" ) || ( $declination eq "W" ) ) {
        $signeddegrees = -($signeddegrees);
    }

    given ($declination) {
        when (/NS/) {

            #Latitude is invalid if less than -90  or greater than 90
            $signeddegrees = "0" if ( abs($signeddegrees) > 90 );
        }
        when (/EW/) {

            #Longitude is invalid if less than -180 or greater than 180
            $signeddegrees = "0" if ( abs($signeddegrees) > 180 );
        }
        default {
        }

    }
    print "Coordinate: $coordinate to $signeddegrees\n"        if $debug;
    print "Deg: $deg, Min:$min, Sec:$sec, Decl:$declination\n" if $debug;
    return ($signeddegrees);
}

sub coordinatetodecimal2 {

    #Validate and set input parameters to this function
    my ( $deg, $min, $sec, $declination ) = validate_pos(
        @_,
        { type => SCALAR },
        { type => SCALAR },
        { type => SCALAR },
        { type => SCALAR },

    );

    my $signedDegrees;

    if ( !$declination =~ /[NSEW]/ ) {
        say "Bad declination parameter: $declination";
        return 0;
    }

    $deg = $deg / 1;

    $min = $min / 60;

    $sec = $sec / 3600;

    $signedDegrees = ( $deg + $min + $sec );

    given ($declination) {
        when (/S|W/) {
            $signedDegrees = -($signedDegrees);
        }

        when (/N|S/) {

            #Latitude is invalid if less than -90  or greater than 90
            if ( abs($signedDegrees) > 90 ) {
                say "$signedDegrees is out of valid range for latitude";
                $signedDegrees = 0;
            }
        }
        when (/E|W/) {

            #Longitude is invalid if less than -180 or greater than 180
            if ( abs($signedDegrees) > 180 ) {
                say "$signedDegrees is out of valid range for longitude";
                $signedDegrees = 0;
            }
        }
        default {
        }

    }
    return ($signedDegrees);
}

sub coordinateToDecimal3 {

    #Deal with coordinates with or without decimals
    #"36-04-00N"
    #Validate and set input parameters to this function
    my ($coordinate) = validate_pos( @_, { type => SCALAR } );

    my ( $deg, $min, $sec, $declination ) =
      $coordinate =~ m/^ \s* (\d+) - (\d+) - ([\d.]+) ([NESW]) \s* $/ix;

    my $signeddegrees;

    #Just die if all of our parameters aren't defined
    unless ( defined $deg
        && defined $min
        && defined $sec
        && defined $declination )
    {
        say "Deg: $deg, Min:$min, Sec:$sec, Decl:$declination";
        die "Error converting coordinate to decimal in coordinateToDecimal3";
        return 0;
    }
    say "Deg: $deg, Min:$min, Sec:$sec, Decl:$declination" if $main::debug;

    #Convert to decimal
    $deg = $deg / 1;
    $min = $min / 60;
    $sec = $sec / 3600;

    $signeddegrees = ( $deg + $min + $sec );

    #South and West declinations are negative
    if ( ( $declination eq "S" ) || ( $declination eq "W" ) ) {
        $signeddegrees = -($signeddegrees);
    }

    given ($declination) {
        when (/NS/) {

            #Latitude is invalid if less than -90  or greater than 90
            $signeddegrees = "0" if ( abs($signeddegrees) > 90 );
        }
        when (/EW/) {

            #Longitude is invalid if less than -180 or greater than 180
            $signeddegrees = "0" if ( abs($signeddegrees) > 180 );
        }
        default {
        }

    }
    say "Coordinate: $coordinate to $signeddegrees"        if $main::debug;
    say "Deg: $deg, Min:$min, Sec:$sec, Decl:$declination" if $main::debug;
    return ($signeddegrees);
}

sub coordinateToDecimalCifpFormat {

    #Convert a latitude or longitude in CIFP format to its decimal equivalent
    my ($coordinate) = shift;
    my ( $deg, $min, $sec, $signedDegrees, $declination, $secPostDecimal );
    my $data;

    #First parse the common information for a record to determine which more specific parser to use
    my $parser_latitude = Parse::FixedLength->new(
        [
            qw(
              Declination:1
              Degrees:2
              Minutes:2
              Seconds:2
              SecondsPostDecimal:2
              )
        ]
    );
    my $parser_longitude = Parse::FixedLength->new(
        [
            qw(
              Declination:1
              Degrees:3
              Minutes:2
              Seconds:2
              SecondsPostDecimal:2
              )
        ]
    );

    #Get the first character of the coordinate and parse accordingly
    $declination = substr( $coordinate, 0, 1 );

    given ($declination) {
        when (/[NS]/) {
            $data = $parser_latitude->parse_newref($coordinate);
            die "Bad input length on parser_latitude"
              if ( $parser_latitude->length != 9 );

            #Latitude is invalid if less than -90  or greater than 90
            # $signedDegrees = "" if ( abs($signedDegrees) > 90 );
        }
        when (/[EW]/) {
            $data = $parser_longitude->parse_newref($coordinate);
            die "Bad input length on parser_longitude"
              if ( $parser_longitude->length != 10 );

            #Longitude is invalid if less than -180 or greater than 180
            # $signedDegrees = "" if ( abs($signedDegrees) > 180 );
        }
        default {
            return -1;

        }
    }

    $declination    = $data->{Declination};
    $deg            = $data->{Degrees};
    $min            = $data->{Minutes};
    $sec            = $data->{Seconds};
    $secPostDecimal = $data->{SecondsPostDecimal};

    # print Dumper($data);

    $deg = $deg / 1;
    $min = $min / 60;

    #Concat the two portions of the seconds field with a decimal between
    $sec = ( $sec . "." . $secPostDecimal );

    # say "Sec: $sec";
    $sec           = ($sec) / 3600;
    $signedDegrees = ( $deg + $min + $sec );

    #Make coordinate negative if necessary
    if ( ( $declination eq "S" ) || ( $declination eq "W" ) ) {
        $signedDegrees = -($signedDegrees);
    }

    # say "Coordinate: $coordinate to $signedDegrees";           #if $debug;
    # say "Decl:$declination Deg: $deg, Min:$min, Sec:$sec";    #if $debug;

    return ($signedDegrees);
}
1;
