#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS=$(printf '\n\t')  # Always put this in Bourne shell scripts

#Modified from original version at http://www.dbforums.com/showthread.php?1619154-how-to-unzip-files-recursively

#
# Function  : runzip zip_file rm_flag
# Parameters: zip_file : File to unzip
#             rm_flag  = If set, remove zip file after unzip
#

#Validate number of command line parameters
if [ "$#" -ne 1 ] ; then
  echo "Usage: $0 ZIP_FILE" >&2
  exit 1
fi



function runzip()
    {
        #
        # Get parameters
        local zip_file=$1
        local rm_flag=$2

        #echo "zip_file: $zip_file"
        
        # Exit if target .zip file doesn't exist
        if [[ ! -e ${zip_file} ]]
            then
                echo "$0 - Zip file not found : ${zip_file}" >&2
                return 1
            fi

        #Where to unzip the target .zip file to
        local zip_dir
        local new_zip_file
        local unzip_error_code

        #Destination subdirectory named after file under its directory
        #removing the .zip suffix
        zip_dir=$(dirname "${zip_file}")/$(basename "${zip_file}" .zip)


        #
        # Create unzip destination directory
        #
        #echo "zip_dir: $zip_dir"

        #Exit if we couldn't create the directory
        if [ ! -d "${zip_dir}" ]
        then
            if ! mkdir "${zip_dir}"
                then
                    echo "$0 - Failed to create directory : ${zip_dir}"
                    return 1
                fi
        fi

        #
        # Unzip into unzip directory
        #

        if ! unzip -qq "${zip_file}" -d "${zip_dir}"
            then
                echo "$0 - Unzip error for file : ${zip_file}"
                return 1
            fi

        #
        # Recursive unzip of new zip files
        #

        unzip_error_code=0


        #Read the list of zip files in zip_dir and extract them using process 
        #substitution instead of a temp file for the find
        #Note that there must be a space between the two < symbols to avoid confusion with the "here-doc" syntax of <<word. 
        while read -r new_zip_file
            do
                if ! runzip "${new_zip_file}" remove_zip
                    then
                        unzip_error_code=$?
                        break
                    fi
            done < <(find "${zip_dir}" -type f -name '*.zip' -print)

        #
        # Remove zip file if required
        #
    #     echo "delete file : ${zip_file}"
        if [ -n "${rm_flag}" -a ${unzip_error_code} -eq 0 ]
            then
                if ! rm "${zip_file}"
                    then
                        echo "$0 - Failed to delete file : ${zip_file}"
                    fi
            fi

        return 0
    }

runzip "$1" remove_zip