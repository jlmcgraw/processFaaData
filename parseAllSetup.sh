#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS=$(printf '\n\t')  # Always put this in Bourne shell scripts


nasr56dayBaseUrl=https://nfdc.faa.gov/webContent/56DaySub
nasr56dayFileName=56DySubscription_August_20__2015_-_October_15__2015.zip

# BUG TODO Currently not working for some reason
# #get current datafile
# wget --timestamping $nasr56dayBaseUrl/$nasr56dayFileName

#Where the 56 day data is unzipped to
datadir=$(basename $nasr56dayFileName .zip)
#Ensure trailing /
datadir+="/"

#recursizely unzip to datadir
./recursiveUnzip.sh $nasr56dayFileName

#Where to save files we create
outputdir=.

sua=$datadir/Additional_Data/AIXM/SAA-AIXM_5_Schema/SaaSubscriberFile/Saa_Sub_File
controlledairspace=$datadir/Additional_Data/Shape_Files

if [ ! -d "$sua" ]; 	then
	echo "No Special Use Airspace information found"
	exit 1
	fi

if [ ! -d "$controlledairspace" ]; 	then
	echo "No Controlled Airspace information found"
	exit 1
 	fi

#delete any existing files
set +e
rm $outputdir/56day.db
rm $outputdir/spatial56day.db
rm ./DAILY_DOF.ZIP ./DOF.DAT
rm $outputdir/ControlledAirspace.sqlite 
rm $outputdir/SpecialUseAirspace.sqlite
set -e

#get the daily obstacle file
echo "---------- Download and process daily obstacle file"
wget --timestamping http://tod.faa.gov/tod/DAILY_DOF.ZIP
unzip DAILY_DOF.ZIP

#remove the header lines from obstacle file and put output in $datadir as "OBSTACLE.txt"
sed '1,4d' ./DOF.DAT > $datadir/OBSTACLE.txt


echo "---------- Create the database"
#create the new sqlite database
#Create geometry and expand text
./parseNasr.pl -g -e $datadir

echo "---------- Adding indexes"
#add indexes
sqlite3 $outputdir/56day.db < addIndexes.sql

echo "---------- Create the spatialite version of database"
cp ./56day.db $outputdir/spatial56day.db

#convert the copy to spatialite
set +e
sqlite3 $outputdir/spatial56day.db < sqliteToSpatialite.sql
set -e

#Lump the airspaces into spatialite databases
echo "---------- Convert controlled and special use airspaces into spatialite databases"

# #Given a version of gdal >= 2.0 you can convert the AIXM .xml files into other vector formats
# #(try a trunk build https://github.com/OSGeo/gdal)
# 
# #Edit this to point to where you cloned the GDAL repository to
# export GDAL_DATA="/home/jlmcgraw/Documents/github/gdal/gdal/data/"
#GML related environment variables for the conversion
export GML_FETCH_ALL_GEOMETRIES=YES
export GML_SKIP_RESOLVE_ELEMS=NONE

dbfile=SpecialUseAirspace.sqlite
#if [ -e $outputdir/$dbfile ]; then (rm $outputdir/$dbfile) fi
find $sua \
  -name "*.xml" \
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
    -lco LAUNDER=NO \
    --config OGR_SQLITE_SYNCHRONOUS OFF \
    --config OGR_SQLITE_CACHE 128 \
    -gt 65536 \
    \;

    
dbfile=ControlledAirspace.sqlite
#if [ -e $outputdir/$dbfile ]; then (rm $outputdir/$dbfile) fi

find $controlledairspace \
  -name "*.shp" \
  -type f \
  -print \
  -exec ogr2ogr \
    -f SQLite \
    $outputdir/$dbfile \
    {} \
    -explodecollections \
    -update \
    -append \
    -dsco SPATIALITE=YES \
    -skipfailures \
    -lco SPATIAL_INDEX=YES \
    -lco LAUNDER=NO \
    --config OGR_SQLITE_SYNCHRONOUS OFF \
    --config OGR_SQLITE_CACHE 128 \
    -gt 65536 \
  \;