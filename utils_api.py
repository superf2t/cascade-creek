from flask import jsonify
import vars
import utils
import utils_db
from classes.place import Place
import time
import requests
from decimal import Decimal
import urllib

import pdb

#######################
# 3rd PARTY API CALLS #
#######################

# call google api and return a place
def get_google_place(place):
    time_start = time.time()

    # call google api and bring it on back
    url = "https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s" % (place, vars.google_api_key_geocode)
    google_result = requests.get(url)
    """
    r.status_code               200
    r.headers["content-type"]   application/json; charset=UTF-8
    r.encoding                  UTF-8
    r.content                   json returned from api
    r.json()                    json returned from api
    """

    r = google_result.json()["results"][0]

    p = Place()
    place = Place(r["place_id"], r["formatted_address"], 
                    r["geometry"]["location"]["lat"], r["geometry"]["location"]["lng"], 
                    r["geometry"]["bounds"]["northeast"]["lat"], r["geometry"]["bounds"]["northeast"]["lng"],
                    r["geometry"]["bounds"]["southwest"]["lat"], r["geometry"]["bounds"]["southwest"]["lng"])

    #log
    elapsed_time = time.time() - time_start
    utils.log('get_google_place', 'Getting place info from G', url, elapsed_time)
    
    return place

# call airbnb search page and return listings
def get_place_search(place_search_url):    
    time_start = time.time()

    result = requests.get(place_search_url)

    #log
    elapsed_time = time.time() - time_start
    utils.log('get_place_search', 'api call', place_search_url, elapsed_time)

    return result.json()

# call trulia and get for sale listings
def get_trulia_for_sale(ne_lat, ne_lng, sw_lat, sw_lng, min_beds, max_beds, max_price):
    time_start = time.time()

    filters = build_trulia_json(ne_lat, ne_lng, sw_lat, sw_lng, min_beds, max_beds, max_price)
    filters = filters.replace("'", '"')
    
    # call trulia and bring it on back
    url = "https://www.trulia.com/json/search/filters/?filters=%s" % urllib.quote(filters)
    trulia_result = requests.get(url)

    #log
    elapsed_time = time.time() - time_start
    utils.log('get_trulia_for_sale', 'Getting "for sale" listings from Trulia', url, elapsed_time)
    
    return trulia_result.content


####################
# HELPER FUNCTIONS #
####################

# build querystring param for Trulia search
def build_trulia_json(ne_lat, ne_lng, sw_lat, sw_lng, 
                        min_beds, max_beds,
                        max_price):

    template = {
        "searchType":"for_sale",
        "location": {
            "coordinates": {
                "southWest": {
                    "latitude":0,
                    "longitude":0
                },
                "northEast": {
                    "latitude":0,
                    "longitude":0
                },
                "southEast": {
                    "latitude":0,
                    "longitude":0
                },
                "northWest": {
                    "latitude":0,
                    "longitude":0
                },
                "center": {
                    "latitude":0,
                    "longitude":0
                }
            }
        },
        "filters": {
            "page":1,
            "view":"map",
            "limit":30,
            "sort": {
                "type":"best",
                "ascending":"true"
            },
            "offset":0,
            "zoom":13,
            "bedrooms": {
                "min": 0,
                "max": "*"
            },
            "price": {
                "min": "0",
                "max": "*"
            },
            "propertyTypes":["single_family_home","multi_family"]
        }
    }

    #{"searchType":"for_sale","location":{"cities":null,"commute":null,"coordinates":{"southWest":{"latitude":40.52894919494794,"longitude":-105.1189418245508},"northEast":{"latitude":40.60015121542612,"longitude":-105.04444078695315},"southEast":{"latitude":40.60015121542612,"longitude":-105.1189418245508},"northWest":{"latitude":40.52894919494794,"longitude":-105.04444078695315},"center":{"latitude":40.56455020518703,"longitude":-105.08169130575197}},"customArea":null,"pointOfInterest":null,"radiusSize":null,"school":null,"university":null,"street":null,"counties":null,"neighborhoods":null,"zips":null,"schoolDistricts":null},"filters":{"page":1,"view":"map","limit":30,"sort":{"type":"best","ascending":true},"offset":0,"zoom":13}}
    #"cities":null,"commute":null,"customArea":null,"pointOfInterest":null,"radiusSize":null,"school":null,"university":null,"street":null,"counties":null,"neighborhoods":null,"zips":null,"schoolDistricts":null

    template['location']['coordinates']['southWest']['latitude'] = str(sw_lat)
    template['location']['coordinates']['southWest']['longitude'] = str(sw_lng)

    template['location']['coordinates']['northEast']['latitude'] = str(ne_lat)
    template['location']['coordinates']['northEast']['longitude'] = str(ne_lng)

    template['location']['coordinates']['southEast']['latitude'] = str(sw_lat)
    template['location']['coordinates']['southEast']['longitude'] = str(ne_lng)

    template['location']['coordinates']['northWest']['latitude'] = str(ne_lat)
    template['location']['coordinates']['northWest']['longitude'] = str(sw_lng)

    template['location']['coordinates']['center']['latitude'] = str((Decimal(ne_lat) + Decimal(sw_lat)) / 2)
    template['location']['coordinates']['center']['longitude'] = str((Decimal(ne_lng) + Decimal(sw_lng)) / 2)

    template['filters']['bedrooms']['min'] = str(min_beds);
    template['filters']['bedrooms']['max'] = str(max_beds);

    template['filters']['price']['max'] = str(max_price);

    return str(template)
