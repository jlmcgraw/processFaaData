#!/usr/bin/python3 

import json
from urllib.request import Request, urlopen
import errno, sys

# The URL to get data from
# "edition=next" to get the upcoming edition
url="https://soa.smext.faa.gov/apra/nfdc/nasr/chart?edition=current"

# Some sample JSON for testing with
sample_json="""
    {
    "status": {
        "code": 200,
        "message": "OK"
    },
    "edition": [
        {
        "editionName": "CURRENT",
        "format": "ZIP",
        "editionDate": "11/09/2017",
        "editionNumber": 12
        }
    ]
    }
    """

def get_jsonparsed_data(url):
    # Build the request
    request = Request(url)
    request.add_header('accept', 'application/json')
    
    # Get the json
    try:
        json_response = urlopen(request).read().decode("utf-8")
    except:
        print("Error getting NASR information from {}".format(url))
        return None
    
    # Parse it
    response_dictionary = (json.loads(json_response))

    # Return the dictionary
    return response_dictionary

# Get the JSON data and return a dictionary of values from it
response_dictionary = get_jsonparsed_data(url)

# Print some of the values
if response_dictionary:
    edition_date = response_dictionary['edition'][0]['editionDate']
    edition_url  = response_dictionary['edition'][0]['product']['url']
    #print(edition_date)
    print(edition_url)
else:
    sys.exit(1)
