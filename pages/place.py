from flask import Blueprint, render_template, jsonify, request, redirect
import vars
import utils
import utils_db
import utils_api
import utils_sqs
#import sys
#sys.path.append("classes")
from classes.place import Place
import requests
import datetime
import time
import math
from decimal import Decimal

import pdb

place = Blueprint('place', __name__, template_folder='templates')

#########
# PAGES #
#########

@place.route("/add_place")
def add_place_page():
    utils.log(None, 'add_place_page', 'page load')
    return render_template("add_place.html")

@place.route("/view_place/<place_id>")
def view_place_page(place_id):
    utils.log(None, 'view_place_page(%s)' % place_id, None)
    place = utils_db.get_place(place_id)

    return render_template("view_place.html", place=place, google_api_key_js_map=vars.google_api_key_js_map) #, trulia_json=trulia_json)

@place.route("/view_place/<place_id>/queue")
def initiate_place_scrape_page(place_id):
    utils.log(None, 'initiate_place_scrape_page(%s)' % place_id, 'page load')
    place = utils_db.get_place(place_id)
    session_id = utils_db.get_session_id(place_id, place.name)
    utils_sqs.insert_sqs_place_message(session_id, place)

    #redirect with session_id
    return redirect("/queue/process_place_queue")


##################
# Non-page URL"s #
##################

# call google geocode service and get the name and coords for a location
@place.route("/_get_place/<place>")
def get_place_google_api(place):
    utils.log(None, 'get_place_google_api', 'place_id = %s' % place)

    place = utils_api.get_google_place(place)

    # see if this place already exists in the db
    response = utils_db.get_place(place.place_id) 
    
    # if an Item was returned, this place is already in the db    
    if response.place_id == '':
        return jsonify(place.img_url)
    else:
        return jsonify("place already in database")


# insert a place into database
@place.route("/_insert_place/", methods=["POST"])
def insert_place_api():
    utils.log(None, 'insert_place_api', None)

    str_place = request.form["hidden_place"]
    
    place = utils_api.get_google_place(str_place)
    utils_db.insert_place(place)

    return redirect("/")


@place.route("/_listings_geojson/<place_id>")
def get_listings_geojson_api(place_id):
    utils.log(None, 'get_listings_geojson_api', 'Return GeoJSON for listings in place: %s' % place_id)
    listings = utils_db.get_listings(place_id)
    return jsonify(listings)

@place.route("/_for_sale/<ne_lat>/<ne_lng>/<sw_lat>/<sw_lng>")
def get_for_sale_api(ne_lat, ne_lng, sw_lat, sw_lng):

    # get trulia listings for the passed in geo
    results = utils_api.get_trulia_for_sale(ne_lat, ne_lng, sw_lat, sw_lng)

    return results

####################
# HELPER FUNCTIONS #
####################

# none

