#!/bin/bash
set -eu                 # Always put this in Bourne shell scripts
IFS=$(printf '\n\t')    # Always put this in Bourne shell scripts

# Download latest documents from from FAA

# The script begins here
# Set some basic variables
declare -r PROGNAME=$(basename "$0")
declare -r PROGDIR=$(readlink -m "$(dirname "$0")")

# Get the number of remaining command line arguments
NUMARGS=$#

# Validate number of command line parameters
if [ "$NUMARGS" -ne 1 ] ; then
    echo "Usage: $PROGNAME <DOWNLOAD_ROOT_DIR>" >&2
    exit 1
fi

# Get command line parameter
DOWNLOAD_ROOT_DIR=$(readlink -f "$1")

# Name of file used as last refresh marker
REFRESH_MARKER="${PROGDIR}/last_nasr_refresh"

if [ ! -d "$DOWNLOAD_ROOT_DIR" ]; then
    echo "$DOWNLOAD_ROOT_DIR doesn't exist" >&2
    exit 1
fi

# Exit if we ran this command within the last 24 hours (adjust as you see fit)
if [ -e "${REFRESH_MARKER}" ] && [ "$(date +%s -r "${REFRESH_MARKER}")" -gt "$(date +%s --date="24 hours ago")" ]; then
 echo "Documents updated within last 24 hours, exiting"
 exit 0
fi 

# Update the time of this file so we can check when we ran this last
touch "${REFRESH_MARKER}"

# Update local cache of 28 and 56 day files

set +e
    wget \
        --directory-prefix="$DOWNLOAD_ROOT_DIR"    \
        --recursive     \
        -l2             \
        --span-hosts    \
        --domains=nfdc.faa.gov   \
        --timestamping      \
        --ignore-case       \
        --accept-regex '.*/xwiki/bin/view/NFDC/56.*|.*56DySubscription_.*.zip'    \
        https://nfdc.faa.gov/xwiki/bin/view/NFDC/56+Day+NASR+Subscription
        
    wget \
        --directory-prefix="$DOWNLOAD_ROOT_DIR"    \
        --recursive     \
        -l2             \
        --span-hosts    \
        --domains=nfdc.faa.gov   \
        --timestamping      \
        --ignore-case       \
        --accept-regex '.*/xwiki/bin/view/NFDC/28.*|.*28DaySubscription.*.zip'    \
        https://nfdc.faa.gov/xwiki/bin/view/NFDC/28+Day+NASR+Subscription
set -e
