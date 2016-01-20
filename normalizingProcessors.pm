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

package normalizingProcessors;

use 5.018;
use strict;
use warnings;

use Params::Validate qw(:all);

use Exporter;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK %EXPORT_TAGS);

use processFaaData;
$VERSION = 1.00;
@ISA     = qw(Exporter);
@EXPORT  = qw( normalize_TWR_TWR3   );

sub normalize_TWR_TWR3 {

    #Make a new table with separate row for each
    #frequency/frequency_use pair for each airport
    my ( $hashRef, $dbh ) =
      validate_pos( @_, { type => HASHREF }, { type => HASHREF } );

    my %new_table;
    state $haveCreatedThisTable;
    my $baseFile   = "TWR";
    my $recordType = "TWR3A";

    for ( my $i = 1 ; $i < 10 ; $i++ ) {
        my $frequency = $hashRef->{
            "frequencys_for_master_airport_use_only_and_sectorization_$i"};

        my $frequency_not_truncated = $hashRef->{
            "frequencys_for_master_airport_use_only_and_sectorization_not_$i"};

        my $frequency_use = $hashRef->{"frequency_use_$i"};

        my $terminal_communications_facility_identifier =
          $hashRef->{terminal_communications_facility_identifier};

        $new_table{frequency}               = $frequency;
        $new_table{frequency_not_truncated} = $frequency_not_truncated;
        $new_table{frequency_use}           = $frequency_use;
        $new_table{terminal_communications_facility_identifier} =
          $terminal_communications_facility_identifier;

        {
            no warnings 'uninitialized';
            my ( $freq, $sector ) =
              split( /\s|\(/, $frequency_not_truncated, 2 );
            $sector =~ s/[\(\)]//g;
            $sector =~ s/(\d\d\d-\d\d\d)/($1)/g;

            # say "$freq - $sector";
            $new_table{freq}   = $freq;
            $new_table{sector} = $sector;
        }

        #Create the table for each recordType if we haven't already
        #uses all the sorted keys in the hash as column names
        unless ($haveCreatedThisTable) {

            #Drop existing table
            my $drop = "DROP TABLE IF EXISTS " . $baseFile . "_" . $recordType;
            $dbh->do($drop);

            #Makes a "CREATE TABLE" statement based on the keys of the hash, columns sorted alphabetically
            my $createStmt =
                'CREATE TABLE '
              . $baseFile . "_"
              . $recordType
              . '(_id INTEGER PRIMARY KEY AUTOINCREMENT,'
              . join( ',', sort { lc $a cmp lc $b } keys %new_table ) . ')';

            # Create the table
            say $createStmt;
            $dbh->do($createStmt);

            #Mark it as created
            $haveCreatedThisTable = 1;

        }

        #Only add a row if frequency is defined
        if ($frequency) {

            #-------------------
            #Make an "INSERT INTO" statement based on the keys and values of the hash
            my $insertStmt =
                'INSERT INTO '
              . $baseFile . "_"
              . $recordType . '('
              . join( ',', keys %new_table )
              . ') VALUES ('
              . join( ',', ('?') x keys %new_table ) . ')';

            #Insert the values into the database
            my $sth = $dbh->prepare($insertStmt);
            $sth->execute( values %new_table );
        }

    }

}

1;
