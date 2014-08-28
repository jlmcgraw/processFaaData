#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS="`printf '\n\t'`"  # Always put this in Bourne shell scripts


#Input and output directories
#56dayCurrent = 
#56dayUrl = https://nfdc.faa.gov/webContent/56DaySub/56DySubscription_July_24__2014_-_September_18__2014.zip

datadir=./56DySubscription_September_18__2014_-_November_13__2014/
outputdir=./outputData

#get current datafile
#unzip to datadir

#delete any existing files
set +e
rm ./56day.db
rm ./spatial56day.db
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

cp 56day.db spatial56day.db

#convert the copy to spatialite
sqlite3 spatial56day.db < sqliteToSpatialite.sql



