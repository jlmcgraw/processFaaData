#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS=$(printf '\n\t')  # Always put this in Bourne shell scripts

# Where to save files we create
outputdir=.

# Check count of command line parameters
if [ "$#" -ne 1 ] ; then
  echo "Usage: $0 28_Day_Subscription_Zip_file" >&2
  echo "eg. $0 28DaySubscription_Effective_2017-06-22.zip" >&2
  echo " Download most recent data from https://nfdc.faa.gov/xwiki/bin/view/NFDC/28+Day+NASR+Subscription" >&2
  exit 1
fi

# Get command line parameters
nasr28dayFileName=$1

if [ ! -f "$nasr28dayFileName" ]; 	then
	echo "No 28-day source zip file found" >&2
	exit 1
	fi

# Get command line parameter and construct the full path to the unzipped data
datadir=$(readlink -m "$(dirname "$nasr28dayFileName")")
datadir+="/"
datadir+=$(basename "$nasr28dayFileName" .zip)
datadir+="/"

readonly nasr_database="$outputdir/nasr.sqlite"
readonly nasr_spatialite_database="$outputdir/spatialite_nasr.sqlite"
readonly controlled_airspace_spatialite_database="$outputdir/controlled_airspace_spatialite.sqlite"
readonly special_use_airspace_spatialite_database="$outputdir/special_use_airspace_spatialite.sqlite"

# Location of airspace files
sua_input_directory=$datadir/Additional_Data/AIXM/SAA-AIXM_5_Schema/SaaSubscriberFile/SaaSubscriberFile/Saa_Sub_File
controlled_airspace_input_directory=$datadir/Additional_Data/Shape_Files

#-------------------------------------------------------------------------------

# Recursively unzip NASR .zip file to $datadir
echo "---------- Recursively unzipping $nasr28dayFileName"
./recursiveUnzip.sh "$nasr28dayFileName"

if [ ! -d "$sua_input_directory" ]; 	then
	echo "No Special Use Airspace information found in ${sua_input_directory}" >&2
	exit 1
	fi

if [ ! -d "$controlled_airspace_input_directory" ]; 	then
	echo "No Controlled Airspace information found in ${controlled_airspace_input_directory}" >&2
	exit 1
 	fi

# Delete any existing files
rm --force "$nasr_database"
rm --force "$nasr_spatialite_database"
rm --force ./DAILY_DOF.ZIP ./DOF.DAT
rm --force "$controlled_airspace_spatialite_database"
rm --force "$special_use_airspace_spatialite_database"

# Get the daily obstacle file
echo "---------- Download and process daily obstacle file"
wget --timestamping http://tod.faa.gov/tod/DAILY_DOF.ZIP
unzip DAILY_DOF.ZIP

# Remove the header lines from obstacle file and put output in $datadir as "OBSTACLE.txt"
sed '1,4d' ./DOF.DAT > $datadir/OBSTACLE.txt

echo "---------- Create the database"
# Create the new sqlite database
# options for Create geometry and expand text
./parse_nasr.pl -g -e "$datadir" "$nasr_database"

echo "---------- Adding indexes"
# Add indexes
sqlite3 "$nasr_database" < add_indexes.sql

echo "---------- Create the spatialite version of database"
cp "$nasr_database" "$nasr_spatialite_database"

# Convert the copy to spatialite
set +e
sqlite3 "$nasr_spatialite_database" < sqlite_to_spatialite.sql
set -e

# Lump the airspaces into spatialite databases
echo "---------- Convert controlled and special use airspaces into spatialite databases"

# GML related environment variables for the conversion
export GML_FETCH_ALL_GEOMETRIES=YES
export GML_SKIP_RESOLVE_ELEMS=NONE

# dbfile=SpecialUseAirspace.sqlite
#if [ -e $outputdir/$dbfile ]; then (rm $outputdir/$dbfile) fi
find "$sua_input_directory" \
  -iname "*.xml"    \
  -type f           \
  -print            \
  -exec ogr2ogr     \
    -f SQLite       \
    "$special_use_airspace_spatialite_database" \
    {}              \
    -explodecollections     \
    -a_srs WGS84            \
    -update                 \
    -append                 \
    -wrapdateline           \
    -fieldTypeToString ALL  \
    -dsco SPATIALITE=YES    \
    -skipfailures           \
    -lco SPATIAL_INDEX=YES  \
    -lco LAUNDER=NO         \
    --config OGR_SQLITE_SYNCHRONOUS OFF \
    --config OGR_SQLITE_CACHE 128       \
    -gt 65536                           \
    \;
    
ogrinfo "$special_use_airspace_spatialite_database" -sql "VACUUM"
    
# dbfile=ControlledAirspace.sqlite
#if [ -e $outputdir/$dbfile ]; then (rm $outputdir/$dbfile) fi

find "$controlled_airspace_input_directory" \
  -iname "*.shp"    \
  -type f           \
  -print            \
  -exec ogr2ogr     \
    -f SQLite       \
    "$controlled_airspace_spatialite_database" \
    {}              \
    -explodecollections     \
    -update                 \
    -append                 \
    -wrapdateline           \
    -dsco SPATIALITE=YES    \
    -skipfailures           \
    -lco SPATIAL_INDEX=YES  \
    -lco LAUNDER=NO         \
    --config OGR_SQLITE_SYNCHRONOUS OFF \
    --config OGR_SQLITE_CACHE 128       \
    -gt 65536                           \
  \;

# Vacuum the database if needed
ogrinfo "$controlled_airspace_spatialite_database" -sql "VACUUM"
