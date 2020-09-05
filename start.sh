#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS=$(printf '\n\t')   # Always put this in Bourne shell scripts
mkdir -p /data/downloads
./freshen_local_nasr.sh /data/downloads && \
  FILE=$(ls /data/downloads) && \
  ./create_databases.sh "/data/downloads/$FILE" && \
  cp *.sqlite /data
