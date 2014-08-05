#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS="`printf '\n\t'`"  # Always put this in Bourne shell scripts


#Input and output directories
56dayCurrent = 
56dayUrl = https://nfdc.faa.gov/webContent/56DaySub/56DySubscription_July_24__2014_-_September_18__2014.zip

datadir=./56DySubscription_May_29__2014_-_July_24__2014/
outputdir=./outputData

#Clear out old CSV files
set +e
rm $outputdir/*.csv
set -e

get current datafile
unzip to datadir

get obstacle file
unzip to datadir
remove the header lines from obstacle file

./parseAll.pl

add indexes

convert to spatialite