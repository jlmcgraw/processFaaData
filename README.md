Create sqlite and spatialite databases from the 28-day NASR data freely provided by the FAA 

The "spatialite_nasr.sqlite" this creates is used by my Aviation Map project and the 
sqlite database could be directly used in any Electronic Flight Bag (EFB) program.

See "Sample SQL queries.sql" for some examples of querying this database

A sample spatialite version of the database can be found here: 
    https://www.dropbox.com/s/j018ph69x9nduv4/data.tar.xz

These instructions are based on using Ubuntu 16.04

How to get this utility up and running:

	Install git
		sudo apt-get install git

	Download the repository
		git clone https://github.com/jlmcgraw/processFaaData.git

	Run setup.sh to install necessary dependencies using Carton
		./setup.sh

    Download the latest NASR and obstacle data
        ./freshen_local_nasr.sh ./local_data
    
    Create the sqlite and spatialite databases from the source FAA data
        create_databases.sh <name of 28 day .zip file>
            eg: "create_databases.sh ./local_data/nfdc.faa.gov/webContent/28DaySub/28DaySubscription_Effective_2017-10-12.zip"

This software and the data it produces come with no guarantees about accuracy or usefulness whatsoever!  Don't use it when your life may be on the line!

Thanks for trying this out!  If you have any feedback, ideas or patches please submit them to github.

-Jesse McGraw
jlmcgraw@gmail.com
