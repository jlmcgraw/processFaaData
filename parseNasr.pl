#!/usr/bin/perl
# Copyright (C) 2014  Jesse McGraw (jlmcgraw@gmail.com)

# Process data provided by the FAA
# This current just slurps all of the data into an sqlite database
#
# TODO
# Link secondary and continuation records to primary records
# FOREIGN KEY(app_id) REFERENCES apps(id),  in create
# $db.execute("PRAGMA foreign_keys = ON;") in connect
#
#SELECT last_insert_rowid()
# Expand more text
#
# Done
#   Convert to spatialite
#
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

use 5.018;
use strict;
use warnings;

#Standard libraries
use File::Basename;
use Getopt::Std;
use Data::Dumper;
$Data::Dumper::Sortkeys = 1;
use File::Slurp;
use vars qw/ %opt /;

#Allow use of locally installed libraries in conjunction with Carton
use FindBin '$Bin';
use lib "$FindBin::Bin/local/lib/perl5";
#Non-standard libaries
use Parse::FixedLength;
use Params::Validate qw(:all);
use DBI;
use processFaaData;

#Uncomment to show debugging statements
#use Smart::Comments;

#Subroutines to calculate geometry, called via dispatch table %hash_of_geometry_creators
use geometryProcessors;

#Subroutines to expand text, called via dispatch table %hash_of_expanders
use textExpanders;

#Subroutines to normalize tables, called via dispatch table %hash_of_normalizers
use normalizingProcessors;



my $opt_string = 'veg';

my $arg_num = scalar @ARGV;

unless ( getopts( "$opt_string", \%opt ) ) {
    usage();

}
if ( $arg_num < 1 ) {
    usage();
}

#Get the target data directory from command line options
my $targetdir = $ARGV[0];

my $debug                = $opt{v};
my $shouldExpandText     = $opt{e};
my $shouldCreateGeometry = $opt{g};

#Parameters for the FixedLength parser
my %parameters = (

    # 'autonum' => 'false',
    # 'trim'    => 'false',
);

# my %sections = (
# 'APT' => '',
# 'ATT' => '',
# 'RWY' => '',
# 'APT' => '',
# 'ARS' => '',
# 'RMK' => '',

# );

#Hash to hold whether we have already created table for this file and recordType
my %haveCreatedTable = ();

#These are parsers for each section/subsection combo we expect to find
#This is really the meat of the whole program
#They've been moved to an external file to reduce clutter here
my %hash_of_parsers = do 'hash_of_parsers.pl';

#Subroutines to expand text codes for the relevant file-record_type pair
my %hash_of_expanders = do 'hash_of_expanders.pl';

#Subroutines to create geometry columns for the relevant file-record_type pair
my %hash_of_geometry_creators = do 'hash_of_geometry_creators.pl';

#Subroutines to normalize data for the relevant file-record_type pair
my %hash_of_normalizers = do 'hash_of_normalizers.pl';

#connect to the database
my $dbfile = "./56day.db";
my $dbh = DBI->connect( "dbi:SQLite:dbname=$dbfile", "", "" );

#Set some parameters
$dbh->do("PRAGMA page_size=4096");
$dbh->do("PRAGMA synchronous=OFF");

# $dbh->do("PRAGMA count_changes=OFF");
# $dbh->do("PRAGMA temp_store=MEMORY");
# $dbh->do("PRAGMA journal_mode=MEMORY");

#Create base tables
my $create_metadata_table  = "CREATE TABLE android_metadata ( locale TEXT );";
my $insert_metadata_record = "INSERT INTO android_metadata VALUES ( 'en_US' );";

$dbh->do("DROP TABLE IF EXISTS android_metadata");
$dbh->do($create_metadata_table);
$dbh->do($insert_metadata_record);

#Load each data file in turn
foreach my $key ( sort keys %hash_of_parsers ) {

    #Open appropriate data file in the target directory
    my ( $filename, $dir, $ext ) = fileparse( $targetdir, qr/\.[^.]*/ );

    my $datafile = "$dir" . "$key.txt";
    my $baseFile = $key;

    my $file;
    open $file, '<', $datafile or die "Could not open $datafile: $!";

    say "";

    #For testing, anything we want to skip
    #AFF|APT|ARB|ATS|AWOS|AWY|COM|FIX|FSS|HARFIX|HPF|ILS|LID|MTR|NATFIX|NAV|OBSTACLE|PFR|PJA|SSD|STARDP|TWR|WXL
    # next if ( $key =~ /^()$/gi );    #|ARB|ATS|AWOS|AWY|COM|FIX
    # next
    # unless ( $key =~
    # /AFF|APT|ARB|ATS|AWOS|AWY|COM|FIX|FSS|HARFIX|HPF|ILS|LID|MTR|NATFIX|NAV|OBSTACLE|PFR|PJA|SSD|STARDP|TWR|WXL/
    # );

    #     next unless ($key =~ /OBSTACLE/);

    ###Open an SQL transaction...
    $dbh->begin_work();

    #This is used as a explicit foreign key for child records
    my $master_record_row_id = 0;

    while (<$file>) {

        my $textOfCurrentLine = $_;

        # my $textOfCurrentLine = $lines[$currentLineNumber];
        my $recordType;

        my $currentLineNumber = $.;

        #Update user every 1000 records
        say "Loading $baseFile: $currentLineNumber..."
          if ( $currentLineNumber % 1000 == 0 );

        #Remove linefeed characters
        $textOfCurrentLine =~ s/\R//g;

        #Set up the recordType because input files don't all follow the same
        #format
        given ($baseFile) {
            when (/ARB|COM|HARFIX|NATFIX|SSD|STARDP|OBSTACLE/x) {
                $recordType = $baseFile;
            }
            when (/WXL/x) {
                $recordType = $baseFile;

                #BUG TODO Handle the oddball continuation records
                #We'll just skip them for now since they don't contain much interesting data
                next if ( $textOfCurrentLine =~ m/^\*/ );
            }
            when (/FSS/) {
                $recordType = $baseFile;

                #BUG TODO Handle the oddball continuation records
                #We'll just skip them for now since
                next if ( substr( $textOfCurrentLine, 0, 4 ) =~ m/\*/ );
            }
            when (/AWOS/) {
                $recordType = trim( substr( $textOfCurrentLine, 0, 5 ) );
            }
            when (/AFF|ANR|ATS|AWY|FIX|HPF|ILS|MTR|NAV|PJA|PFR|TWR/x) {
                $recordType = trim( substr( $textOfCurrentLine, 0, 4 ) );
            }
            when (/APT/x) {
                $recordType = trim( substr( $textOfCurrentLine, 0, 3 ) );
            }
            when (/LID/x) {
                $recordType = trim( substr( $textOfCurrentLine, 0, 1 ) );
            }
            default {
                die "I don't recognize $baseFile while reading $datafile";
            }

        }

        #Die if there is not a parse format for this input file and recordType
        die
          "$datafile line # $currentLineNumber : No parser defined for this recordType: $recordType"
          unless exists $hash_of_parsers{$baseFile}{$recordType};

        #Remove any spaces from the recordType
        $recordType =~ s/\s+//g;

        #Create an array to feed to Parse::FixedLength from the parser format
        #we looked up in the hash_of_parsers
        my @parserArray =
          split( ' ', $hash_of_parsers{$baseFile}{$recordType} );

        #Create the specific parser for this recordType
        my $parser_specific =
          Parse::FixedLength->new( [@parserArray], \%parameters );

        #Check for mismatch between expected and actual lengths
        die "Line # $currentLineNumber - Bad parse for $recordType: Expected "
          . $parser_specific->length
          . " characters but read "
          . length($textOfCurrentLine) . "\n"
          unless $parser_specific->length == length($textOfCurrentLine);

        #Parse with specific parser
        my $data2 = $parser_specific->parse_newref($textOfCurrentLine);

        #Normalize data, will create new tables
        if ( exists $hash_of_normalizers{$baseFile}{$recordType} ) {

            #Call the appropriate subroutine, passing a reference to our hash and the database handler
            $hash_of_normalizers{$baseFile}{$recordType}->( $data2, $dbh );
        }

        #Expand text if requested, may add columns to hash
        if ( $shouldExpandText
            && exists $hash_of_expanders{$baseFile}{$recordType} )
        {

            #Call the appropriate subroutine, passing a reference to our hash
            $hash_of_expanders{$baseFile}{$recordType}->($data2);
        }

        #If requested, provide decimalized lon/lat columns in order to create spatialite geometry, may add columns to hash
        if ( $shouldCreateGeometry
            && exists $hash_of_geometry_creators{$baseFile}{$recordType} )
        {

            #Call the appropriate subroutine, passing a reference to our hash
            $hash_of_geometry_creators{$baseFile}{$recordType}->($data2);
        }

        #Delete any keys/columns with "blank" in the name
        {
            my @unwanted;
            foreach my $key ( sort keys %{ $data2 } ) {
                if ( $key =~ /blank/i ) {

                    #Save this key to our array of entries to delete
                    push @unwanted, $key;
                }
            }

            foreach my $key (@unwanted) {
                delete $data2->{$key};
            }
        }

        #Create the table for each recordType if we haven't already
        #uses all the sorted keys in the hash as column names
        unless ( $haveCreatedTable{$baseFile}{$recordType} ) {

            #Drop existing table
            my $drop = "DROP TABLE IF EXISTS " . $baseFile . "_" . $recordType;
            $dbh->do($drop);

            #Makes a "CREATE TABLE" statement based on the keys of the hash, columns sorted alphabetically
            #Include the master_record_row_id as an explicit foreign key to master record
            #The inclusion of " NONE" here is  to force sqlite to not assign affinity to columns, since that is making it "TEXT" by default
            my $createStmt =
                'CREATE TABLE '
              . $baseFile . "_"
              . $recordType
              . '(_id INTEGER PRIMARY KEY AUTOINCREMENT,'
              . 'master_record_row_id INTEGER,'
              . join( ' NONE,', sort { lc $a cmp lc $b } keys %$data2 )
              . ' NONE)';

            ### $createStmt: $createStmt
            # Create the table
            $dbh->do($createStmt);

            #Mark it as created
            $haveCreatedTable{$baseFile}{$recordType} = 1;
        }

        #Master records have a master_record_row_id of 0
        if ( $recordType =~ m/1$/ || $recordType =~ m/^APT$/ ) {
            $master_record_row_id = 0;
        }

        #-------------------
        #Make an "INSERT INTO" statement based on the keys and values of the hash
        #Include the master_record_row_id as an explicit foreign key to master record
        my $insertStmt =
            'INSERT INTO '
          . $baseFile . "_"
          . $recordType . '('
          . 'master_record_row_id,'
          . join( ',', keys %{ $data2 } )
          . ') VALUES ('
          . $master_record_row_id . ','
          . join( ',', ('?') x keys %{ $data2 } ) . ')';

        # $insertStmt: $insertStmt
        #Insert the values into the database
        my $sth = $dbh->prepare($insertStmt);
        $sth->execute( values %{ $data2 } );

        #If we just inserted a master record
        #Then get and save it's rowId to be used as a foreign key for its child records
        if ( $recordType =~ m/1$/ || $recordType =~ m/^APT$/ ) {

            # my $getLastRowIdStatement = "SELECT last_insert_rowid()";
            # my $lastRowId = $dbh->do($getLastRowIdStatement);
            $master_record_row_id = $dbh->func('last_insert_rowid')
              ;    ### Last row was $master_record_row_id...
        }
    }
    ### Transaction commit...
    $dbh->commit();
}

sub usage {
    say "Usage: $0 -v -e <data directory>\n";
    say "-v: enable debug output";
    say "-e: expand text";
    say "-g: create geometry for spatialite";
    exit(1);
}
