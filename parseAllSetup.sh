#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS="`printf '\n\t'`"  # Always put this in Bourne shell scripts


#Input and output directories
#56dayCurrent = 
#56dayUrl = https://nfdc.faa.gov/webContent/56DaySub/56DySubscription_July_24__2014_-_September_18__2014.zip

datadir=./56DySubscription_July_24__2014_-_September_18__2014/
outputdir=./outputData



#get current datafile
#unzip to datadir

#get obstacle file
#unzip to datadir
#remove the header lines from obstacle file

set +e
rm ./56day.db
set -e

./parseAll.pl -g -e $datadir

cp 56day.db spatial56day.db

#convert to spatialite
sqlite3 spatial56day.db < sqliteToSpatialite.sql

#add indexes

