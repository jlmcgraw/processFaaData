#!/bin/bash
set -eu                # Always put this in Bourne shell scripts
IFS=$(printf '\n\t')  # Always put this in Bourne shell scripts
#Recursively extract archives
#Yes, there are plenty of utilities that do this already (dtrx, for example)
#Modified from original version at http://www.dbforums.com/showthread.php?1619154-how-to-unzip-files-recursively
#Other bits from http://tuxtweaks.com/2014/05/bash-getopts/

#
# Function  : runzip zip_file rm_flag
# Parameters: zip_file : File to unzip
#             rm_flag  = If set, remove zip file after unzip
#

#Help function
function HELP {
  echo -e \\n"${BOLD}${SCRIPT}${NORM}"
  echo -e \\n"Recursively unzip files and any archives inside them"\\n
  echo -e "Basic usage:"\\n
  echo -e \\t"${BOLD}$SCRIPT <switches> file1.zip file2.zip ... fileN.zip${NORM}"\\n
  echo -e "Command line switches are optional. The following switches are recognized:"\\n
  echo -e \\t"${REV}-d${NORM}  --Delete the input file(s) after processing"
  echo -e \\t"${REV}-h${NORM}  --Displays this help message. No further functions are performed"\\n
  echo -e "Example: ${BOLD}$SCRIPT -d test.zip${NORM}"\\n
  exit 1
}

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
        #full_path=$(readlink -f "${zip_file}")
        
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
                if ! runzip "${new_zip_file}" TRUE
                    then
                        unzip_error_code=$?
                        break
                    fi

            done < <(find \
                        "${zip_dir}" \
                        -type f \
                            \(  \
                            -iname '*.tar.bz2' \
                            -o -iname '*.tar.gz' \
                            -o -iname '*.bz2' \
                            -o -iname '*.gz' \
                            -o -iname '*.xz' \
                            -o -iname '*.tar' \
                            -o -iname '*.tbz2' \
                            -o -iname '*.tgz' \
                            -o -iname '*.txz' \
                            -o -iname '*.zip' \
                            -o -iname '*.Z' \
                            -o -iname '*.rar' \
                            -o -iname '*.jar' \
                            \) \
                        -print)
    
        #
        # Remove zip file if required
        #
    #     echo "delete file : ${zip_file}"
        if [ "${rm_flag}" == "TRUE" -a ${unzip_error_code} -eq 0 ]
#         if [ -n "${rm_flag}" -a ${unzip_error_code} -eq 0 ]
            then
                if ! rm "${zip_file}"
                    then
                        echo "$0 - Failed to delete file : ${zip_file}"
                    fi
            fi

        return 0
    }

# JLM: I copied this from http://askubuntu.com/questions/338758/how-to-quickly-extract-all-kinds-of-archived-files-from-command-line
# I found the following function at http://unix.stackexchange.com/a/168/37944
# which I improved it a little. Many thanks to sydo for this idea.
extract () {
    for arg in $@ ; do
        if [ -f $arg ] ; then
            case $arg in
                *.tar.bz2)  tar xjf $arg      ;;
                #*.tar.bz2)  tar xjf $arg --directory $directory     ;;
                *.tar.gz)   tar xzf $arg      ;;
                #*.tar.gz)   tar xzf $arg --directory $directory      ;;
                *.tar.xz)   tar --xz -xvf $arg      ;;
                #*.tar.xz)   tar --xz -xvf $arg --directory $directory        ;;
                *.bz2)      bunzip2 $arg      ;;
                #*.bz2)     tar xjf $arg --directory $directory     ;;
                *.gz)       gunzip $arg       ;;
                #*.gz)       tar xzf $arg --directory $directory        ;;
                *.xz)       xz -d $arg       ;;
                #*.xz)       xz -d $arg       ;;
                *.tar)      tar xf $arg       ;;
                #*.tar)      tar xf $arg  --directory $directory      ;;
                *.tbz2)     tar xjf $arg      ;;
                #*.tbz2)     tar xjf $arg  --directory $directory     ;;
                *.tgz)      tar xzf $arg      ;;
                #*.tgz)      tar xzf $arg  --directory $directory     ;;
                *.txz)      tar Jxvf $arg      ;;
                #*.txz)      tar Jxvf $arg  --directory $directory     ;;
                *.zip)      unzip $arg        ;;
                #*.zip)      unzip $arg -d $directory       ;;
                *.Z)        uncompress $arg   ;;
                #*.Z)        TODO uncompress $arg   ;;
                *.rar)      rar x $arg        ;;  # 'rar' must to be installed
                #*.rar)      TODO rar x $arg        ;;  # 'rar' must to be installed
                *.jar)      jar -xvf $arg     ;;  # 'jdk' must to be installed
                #*.jar)      TODO jar -xvf $arg     ;;  # 'jdk' must to be installed
                *)          echo "'$arg' cannot be extracted via extract()" ;;
            esac
        else
            echo "'$arg' is not a valid file"
        fi
    done
}


#Set Script Name variable
SCRIPT=$(basename "${BASH_SOURCE[0]}")

#Initialize variables to default values.
SHOULD_DELETE=FALSE

#Set fonts for Help.
NORM=$(tput sgr0)
BOLD=$(tput bold)
REV=$(tput smso)


#Check the number of arguments. If none are passed, print help and exit.
NUMARGS=$#
# echo -e \\n"Number of arguments: $NUMARGS"
if [ "$NUMARGS" -eq 0 ]; then
  HELP
fi

### Start getopts code ###

#Parse command line flags
#If an option should be followed by an argument, it should be followed by a ":".
#Notice there is no ":" after "h". The leading ":" suppresses error messages from
#getopts. This is required to get my unrecognized option code to work.

while getopts :dh FLAG; do
  case $FLAG in
    d)  #set option "d"
      SHOULD_DELETE="TRUE"
#       echo "-d used:"
#       echo "SHOULD_DELETE = $SHOULD_DELETE"
      ;;
    h)  #show help
      HELP
      ;;
    \?) #unrecognized option - show help
      echo -e \\n"Option -${BOLD}$OPTARG${NORM} not allowed."
      HELP
      ;;
  esac
done

shift $((OPTIND-1))  #This tells getopts to move on to the next argument.

### End getopts code ###


### Main loop to process files ###
while [ $# -ne 0 ]; do
  FILE=$1
  #Call the recursive unzip function with supplied file and delete parameters
  runzip "$FILE" $SHOULD_DELETE
  shift  #Move on to next input file.
done

### End main loop ###

exit 0