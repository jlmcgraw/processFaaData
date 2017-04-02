#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS=$(printf '\n\t')  # Always put this in Bourne shell scripts

#Check count of command line parameters
if [ "$#" -ne 1 ] ; then
  echo "Usage: $0 56_Day_Subscription_Zip_file" >&2
  echo "eg. $0 56DySubscription_December_10__2015_-_February_04__2016.zip" >&2
  echo " Download most recent data from https://nfdc.faa.gov/xwiki/bin/view/NFDC/56+Day+NASR+Subscription" >&2
  exit 1
fi

#Get command line parameters
nasr56dayFileName=$1

if [ ! -f "$nasr56dayFileName" ]; 	then
	echo "No 56-day database found" >&2
	exit 1
	fi
	
# nasr56dayBaseUrl=https://nfdc.faa.gov/webContent/56DaySub
# nasr56dayFileName=56DySubscription_December_10__2015_-_February_04__2016.zip

# BUG TODO Currently not working for some reason
# #get current datafile
# wget --timestamping $nasr56dayBaseUrl/$nasr56dayFileName
# wget https://nfdc.faa.gov/webContent/56DaySub/56DySubscription_December_10__2015_-_February_04__2016.zip

# #Where the 56 day data is unzipped to
# datadir=$(basename $nasr56dayFileName .zip)
# #Ensure trailing /
# datadir+="/"

# Get command line parameter and construct the full path to the unzipped data
datadir=$(readlink -m "$(dirname "$nasr56dayFileName")")
datadir+="/"
datadir+=$(basename "$nasr56dayFileName" .zip)
datadir+="/"

# Recursizely unzip NASR .zip file to $datadir
./recursiveUnzip.sh "$nasr56dayFileName"

#Where to save files we create
outputdir=.

sua=$datadir/Additional_Data/AIXM/SAA-AIXM_5_Schema/SaaSubscriberFile/Saa_Sub_File
controlledairspace=$datadir/Additional_Data/Shape_Files

if [ ! -d "$sua" ]; 	then
	echo "No Special Use Airspace information found in ${sua}" >&2
	exit 1
	fi

if [ ! -d "$controlledairspace" ]; 	then
	echo "No Controlled Airspace information found in ${controlledairspace}" >&2
	exit 1
 	fi

# Delete any existing files
rm --force $outputdir/56day.db
rm --force $outputdir/spatial56day.db
rm --force ./DAILY_DOF.ZIP ./DOF.DAT
rm --force $outputdir/ControlledAirspace.sqlite 
rm --force $outputdir/SpecialUseAirspace.sqlite

# Get the daily obstacle file
echo "---------- Download and process daily obstacle file"
wget --timestamping http://tod.faa.gov/tod/DAILY_DOF.ZIP
unzip DAILY_DOF.ZIP

# Remove the header lines from obstacle file and put output in $datadir as "OBSTACLE.txt"
sed '1,4d' ./DOF.DAT > $datadir/OBSTACLE.txt

echo "---------- Create the database"
# Create the new sqlite database
# options for Create geometry and expand text
./parseNasr.pl -g -e "$datadir"

echo "---------- Adding indexes"
# Add indexes
sqlite3 $outputdir/56day.db < addIndexes.sql

echo "---------- Create the spatialite version of database"
cp ./56day.db $outputdir/spatial56day.db

# Convert the copy to spatialite
set +e
sqlite3 $outputdir/spatial56day.db < sqliteToSpatialite.sql
set -e

# Lump the airspaces into spatialite databases
echo "---------- Convert controlled and special use airspaces into spatialite databases"

# GML related environment variables for the conversion
export GML_FETCH_ALL_GEOMETRIES=YES
export GML_SKIP_RESOLVE_ELEMS=NONE

dbfile=SpecialUseAirspace.sqlite
#if [ -e $outputdir/$dbfile ]; then (rm $outputdir/$dbfile) fi
find $sua \
  -iname "*.xml" \
  -type f \
  -print \
  -exec ogr2ogr \
    -f SQLite \
    $outputdir/$dbfile \
    {} \
    -explodecollections \
    -a_srs WGS84 \
    -update \
    -append \
    -wrapdateline \
    -fieldTypeToString ALL \
    -dsco SPATIALITE=YES \
    -skipfailures \
    -lco SPATIAL_INDEX=YES \
    -lco LAUNDER=NO \
    --config OGR_SQLITE_SYNCHRONOUS OFF \
    --config OGR_SQLITE_CACHE 128 \
    -gt 65536 \
    \;
    
ogrinfo $dbfile -sql "VACUUM"
    
dbfile=ControlledAirspace.sqlite
#if [ -e $outputdir/$dbfile ]; then (rm $outputdir/$dbfile) fi

find $controlledairspace \
  -iname "*.shp" \
  -type f \
  -print \
  -exec ogr2ogr \
    -f SQLite \
    $outputdir/$dbfile \
    {} \
    -explodecollections \
    -update \
    -append \
    -wrapdateline \
    -dsco SPATIALITE=YES \
    -skipfailures \
    -lco SPATIAL_INDEX=YES \
    -lco LAUNDER=NO \
    --config OGR_SQLITE_SYNCHRONOUS OFF \
    --config OGR_SQLITE_CACHE 128 \
    -gt 65536 \
  \;

# Vacuum the database if needed
ogrinfo $dbfile -sql "VACUUM"
