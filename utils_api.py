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

    utils.log("get_place_search", place_search_url)
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36'}
    #cookies = {'cooies': 'sdid=; ftv=1510164990833; __ssid=b18bbb4a-03a4-41bd-aa7b-5c6ed432c7ce; hli=1; har=1; fbm_138566025676=base_domain=.airbnb.com; prrv2=control; has_predicted_user_langs=1; 251aa4ff0=control; 3fbb8aeb0=treatment; 7af62ae29=new_marquee; 278c2c202=control; c98f39478=control; 76cfc8758=control; xplwp2=new_marquee_2; 0241b3c76=control; fblo_138566025676=y; _csrf_token=V4%24.airbnb.com%24kHIw8WbMf1g%24_LwXPHl_gK_9HFl2Co7y3B_tT9THFdDstep_9wOe4ws%3D; li=1; _aat=0%7CJMYpofeKenvW3FkmemmF8duZwlCX6lyCx5u5NbhV%2BPbH5NXa%2B8%2FQTaYnuEwELPIb; abb_fa2=%7B%22user_id%22%3A%2210%7C1%7CM8g%2B4%2BjpZ1%2B%2FmhoZemD7TopcZw7TIrlqIVtb7wM9zgL4mmghYVZ6%22%7D; rclmd=%7B%222706251%22%3D%3E%22email%22%2C+%2212101375%22%3D%3E%22email%22%7D; roles=0; _airbed_session_id=d566eb920626a8dc5baacbe29337cf73; flags=806756612; ed1dc13cb=treatment; mdr_browser=desktop; _user_attributes=%7B%22curr%22%3A%22USD%22%2C%22guest_exchange%22%3A1.0%2C%22device_profiling_session_id%22%3A%221510164987--28133b4a024b36a58f7cfc0f%22%2C%22giftcard_profiling_session_id%22%3A%221512407451-2706251-63a99e7a7d26d009283b1773%22%2C%22reservation_profiling_session_id%22%3A%221512407451-2706251-dcdb53f8645e6983c6494888%22%2C%22id%22%3A2706251%2C%22hash_user_id%22%3A%225e734e2f36d9b9845f49a600cfeb3e1931380f27%22%2C%22eid%22%3A%22AcRf4LQddbyIWOak2SJFxQ%3D%3D%22%2C%22num_msg%22%3A101%2C%22num_notif%22%3A3%2C%22num_alert%22%3A4%2C%22num_h%22%3A0%2C%22num_pending_requests%22%3A0%2C%22num_trip_notif%22%3A0%2C%22name%22%3A%22Travis%22%2C%22num_action%22%3A0%2C%22pellet_to%22%3A10%2C%22is_admin%22%3Afalse%2C%22can_access_photography%22%3Afalse%7D; AMP_TOKEN=%24NOT_FOUND; _ga=GA1.2.1569909426.1510164990; _gid=GA1.2.732328158.1512324997; jitney_client_session_id=8e9fad9c-adf8-4bbc-aa60-178273f7b043; jitney_client_session_created_at=1512407776; jitney_client_session_updated_at=1512407776; cbkp=3; _pt=1--WyI1ZTczNGUyZjM2ZDliOTg0NWY0OWE2MDBjZmViM2UxOTMxMzgwZjI3Il0%3D--a9710a3b2457e7e3e044605512e13affdd943b5f; bev=1510164986_DpJ7wPrIZfm%2F%2Fcf7; jitney_client_session_id=44cc38b0-65be-4c62-973f-0d2eaab7d9c2; jitney_client_session_created_at=1512410102; jitney_client_session_updated_at=1512410102'}
    result = requests.get(place_search_url, headers=headers) #, cookies=cookies)

    print('-------------- result ---------------')
    print(result)
    print(result.content)
    print('-------------- end: result ---------------')
    #utils.log("get_place_search", result.json())

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
