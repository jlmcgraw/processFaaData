#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS="`printf '\n\t'`"  # Always put this in Bourne shell scripts


#Input and output directories
#56dayCurrent = 
#56dayUrl = https://nfdc.faa.gov/webContent/56DaySub/56DySubscription_July_24__2014_-_September_18__2014.zip

#Where the 56 day data is unzipped to
datadir=./56DySubscription_September_18__2014_-_November_13__2014/

#Where to save files we create
outputdir=.

sua=$datadir/Additional_Data/AIXM/SAA-AIXM_5_Schema/SaaSubscriberFile/Saa_Sub_File
controlledairspace=$datadir/Additional_Data/Shape_Files

#if [ ! -d "$sua" ]; 	then
#	echo "No Special Use Airspace information found"
#	exit
# 	fi

if [ ! -d "$controlledairspace" ]; 	then
	echo "No Controlled Airspace information found"
	exit
 	fi

#get current datafile
#unzip to datadir

#delete any existing files
set +e
rm $outputdir/56day.db
rm $outputdir/spatial56day.db
rm ./DAILY_DOF.ZIP ./DOF.DAT
set -e

#get the daily obstacle file
  wget http://tod.faa.gov/tod/DAILY_DOF.ZIP
  unzip DAILY_DOF.ZIP

#remove the header lines from obstacle file and put output in $datadir as "OBSTACLE.txt"
  sed '1,4d' ./DOF.DAT > $datadir/OBSTACLE.txt


#create the new sqlite database
./parseAll.pl -g -e $datadir

#add indexes
#TBD

cp ./56day.db $outputdir/spatial56day.db

#convert the copy to spatialite
sqlite3 $outputdir/spatial56day.db < sqliteToSpatialite.sql

#Lump the airspaces into spatialite databases
echo "#Convert controlled and special use airspaces into spatialite databases"

#dbfile=SpecialUseAirspace.sqlite
#if [ -e $outputdir/$dbfile ]; then (rm $outputdir/$dbfile) fi
#
#find $sua -name "*.xml" -type f -print -exec ogr2ogr -f SQLite $outputdir/$dbfile {} -explodecollections -update -append -dsco SPATIALITE=YES -skipfailures -lco SPATIAL_INDEX=YES -lco LAUNDER=NO --config OGR_SQLITE_SYNCHRONOUS OFF --config OGR_SQLITE_CACHE 128 -gt 65536 \;

dbfile=ControlledAirspace.sqlite
if [ -e $outputdir/$dbfile ]; then (rm $outputdir/$dbfile) fi

find $controlledairspace -name "*.shp" -type f -print -exec ogr2ogr -f SQLite $outputdir/$dbfile {} -explodecollections -update -append -dsco SPATIALITE=YES -skipfailures -lco SPATIAL_INDEX=YES -lco LAUNDER=NO --config OGR_SQLITE_SYNCHRONOUS OFF --config OGR_SQLITE_CACHE 128 -gt 65536 \;


