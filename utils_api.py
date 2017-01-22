import vars
import utils
import utils_db
from classes.place import Place
import time
import requests

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
    utils.log(None, 'get_google_place', 'Getting place info from G', url, elapsed_time)
    
    return place

# call airbnb search page and return listings
def get_place_search(session_id, place_search_url):    
    time_start = time.time()

    print '\nplace_search_url:\n%s\n' % place_search_url
    result = requests.get(place_search_url)

    #log
    elapsed_time = time.time() - time_start
    utils.log(session_id, 'get_place_search', 'api call', place_search_url, elapsed_time)

    return result.json()
