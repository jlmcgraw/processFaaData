#!/bin/bash

#Install necessary software
apt install -y \
                    sqlite3 \
                    libspatialite-dev \
                    spatialite-bin \
                    libsqlite3-mod-spatialite \
                    gdal-bin \
                    unzip

#Install necessary libraries
pip install requests
