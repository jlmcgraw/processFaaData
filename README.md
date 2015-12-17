Create sqlite and spatialite databases from the 56-day data provided by the FAA 

The "spatial56day.db" this creates is used by the Aviation Map project

These instructions are based on using Ubuntu 14.04+

How to get this utility up and running:

	Enable the "universe" repository in "Software & Updates" section of System Settings and update

	Install git
		sudo apt-get install git

	Download the repository
		git clone https://github.com/jlmcgraw/processFaaData.git

	Run setup.sh to install necessary dependencies
		./setup.sh

	Download the current 56-day data from https://nfdc.faa.gov/xwiki/bin/view/NFDC/56+Day+NASR+Subscription
		wget https://nfdc.faa.gov/webContent/56DaySub/56DySubscription_December_10__2015_-_February_04__2016.zip


	Download the current obstacle data (Daily DOF downloaded automatically by create_databases.sh)
		http://tod.faa.gov/tod/public/TOD_DOF.html
			or
		http://tod.faa.gov/tod/DAILY_DOF.ZIP

	Requires perl version > 5.018

How to use these utilities

	create_databases.sh <name of 56 day .zip file>
                Usage: ./create_databases.sh <name of 56 day .zip file>
                    eg: "create_databases.sh 56DySubscription_December_10__2015_-_February_04__2016.zip"
                    
		Creates the sqlite database, expanding text and creating spatialite geometries.  Then converts to spatialite database.  Also creates airspace spatialite databases

	parseNasr.pl
		Usage: ./parseNasr.pl -v -e <data directory>

		-v: enable debug output
		-e: expand text
		-g: create geometry for spatialite
 
Running

        Download the most recent 56 day data from https://nfdc.faa.gov/xwiki/bin/view/NFDC/56+Day+NASR+Subscription
	
	run "./create_databases.sh <name of 56 day .zip file>"


This software and the data it produces come with no guarantees about accuracy or usefulness whatsoever!  Don't use it when your life may be on the line!

Thanks for trying this out!  If you have any feedback, ideas or patches please submit them to github.

-Jesse McGraw
jlmcgraw@gmail.com
