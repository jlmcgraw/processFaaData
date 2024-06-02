This is a pure-python fork of the [processFaaData](https://github.com/jlmcgraw/processFaaData.git)
project by [jlmcgraw](https://github.com/jlmcgraw).

Create sqlite and spatialite databases from the 28-day NASR data freely provided by the FAA

The `spatialite_nasr.sqlite` and the `nasr.sqlite` that this creates can be directly used
in any Electronic Flight Bag (EFB) program.

See `Sample SQL queries.sql` in the `sql` directory for some examples of querying this database.

### Ubuntu

These instructions are based on using Ubuntu 16.04

How to get this utility up and running:

- Install git: `sudo apt-get install git`
- Clone this repository
- Run setup.sh to install necessary dependencies: `./setup.sh`
- Run the python script: `python main.py`

The script has three optional flags:

- `v`: Verbose. This should show/suppress debug behavior. The original Perl did not use this.
- `e`: Expand. This expands abbreviations in various files to their full values (`RMK` to `Remark`, for example).
- `g`: Geometry. This converts the NASR data to spatialite.

While the script will automatically purge the old data before downloading new data, there is an additional `clean.sh` script available if you would like to quickly purge the data without waiting until the next import.

### NOTE

This software and the data it produces come with no guarantees about accuracy or usefulness whatsoever! Don't use it when your life may be on the line!
