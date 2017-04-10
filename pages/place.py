from flask import Blueprint, render_template, jsonify, request, redirect
import vars
import utils
import utils_db
import utils_api
import utils_sqs
from classes.place import Place
import requests
import datetime
import time
import math
from decimal import Decimal
import flask_login

import pdb

place = Blueprint('place', __name__, template_folder='templates')
login_manager = flask_login.LoginManager()

#########
# PAGES #
#########

@place.route("/add_place")
@flask_login.login_required
def add_place_page():
    utils.log('add_place_page', 'page load')
    return render_template("add_place.html")

@place.route("/view_place/<place_id>")
@flask_login.login_required
def view_place_page(place_id):
    utils.log('view_place_page(%s)' % place_id, None)
    place = utils_db.get_place(place_id)
    #avg_bookings_by_bedrooms = utils_db.get_avg_bookings_by_bedrooms(place_id)
    #return render_template("view_place.html", place=place, avg_bookings_by_bedrooms=avg_bookings_by_bedrooms, google_api_key_js_map=vars.google_api_key_js_map, colors=vars.colors)
    return render_template("view_place.html", place=place, google_api_key_js_map=vars.google_api_key_js_map, colors=vars.colors)

@place.route("/view_place/<place_id>/queue")
@flask_login.login_required
def initiate_place_scrape_page(place_id):
    utils.log('initiate_place_scrape_page(%s)' % place_id, 'page load')
    place = utils_db.get_place(place_id)
    utils_sqs.insert_sqs_place_message(place)

    #redirect
    return redirect("/queue/process_place_queue")


##################
# Non-page URL"s #
##################

# call google geocode service and get the name and coords for a location
@place.route("/_get_place/<place>")
@flask_login.login_required
def get_place_google_api(place):
    utils.log('get_place_google_api', 'place_id = %s' % place)

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
@flask_login.login_required
def insert_place_api():
    utils.log('insert_place_api', None)

    str_place = request.form["hidden_place"]
    
    place = utils_api.get_google_place(str_place)
    utils_db.insert_place(place)

    return redirect("/")

@place.route("/_get_avg_bookings_by_bedroom/<place_id>/<ne_lat>/<ne_lng>/<sw_lat>/<sw_lng>")
@flask_login.login_required
def get_avg_bookings_by_bedroom_api(place_id, ne_lat, ne_lng, sw_lat, sw_lng):
    utils.log('get_avg_bookings_by_bedroom_api', 'Get averages and eighty pct')
    avg_bookings_by_bedrooms = utils_db.get_avg_bookings_by_bedrooms(place_id, ne_lat, ne_lng, sw_lat, sw_lng)
    return jsonify(avg_bookings_by_bedrooms)

@place.route("/_listings_geojson/<place_id>/<ne_lat>/<ne_lng>/<sw_lat>/<sw_lng>")
@flask_login.login_required
def get_listings_geojson_api(place_id, ne_lat, ne_lng, sw_lat, sw_lng):
    utils.log('get_listings_geojson_api', 'Return GeoJSON for listings in place: %s' % place_id)
    listings = utils_db.get_listings(place_id, ne_lat, ne_lng, sw_lat, sw_lng)
    return jsonify(listings)

@place.route("/_for_sale/<ne_lat>/<ne_lng>/<sw_lat>/<sw_lng>")
@flask_login.login_required
def get_for_sale_api(ne_lat, ne_lng, sw_lat, sw_lng):

    # get trulia listings for the passed in geo
    results = utils_api.get_trulia_for_sale(ne_lat, ne_lng, sw_lat, sw_lng)

    return results

@place.route("/_submit_monthly_fee_form/<place_id>")
@flask_login.login_required
def submit_monthly_fee_form_api(place_id):
    monthly_property_tax_per_100k = None
    monthly_mortgage_insurance_per_100k = None
    monthly_short_term_insurance_per_100k = None
    monthly_utilities_per_sq_ft = None
    monthly_internet_fee = None
    monthly_management_fee = None

    if request.args.get('monthly_property_tax_per_100k'):
        monthly_property_tax_per_100k = request.args.get('monthly_property_tax_per_100k')

    if request.args.get('monthly_mortgage_insurance_per_100k'):
        monthly_mortgage_insurance_per_100k = request.args.get('monthly_mortgage_insurance_per_100k')

    if request.args.get('monthly_short_term_insurance_per_100k'):
        monthly_short_term_insurance_per_100k = request.args.get('monthly_short_term_insurance_per_100k')

    if request.args.get('monthly_utilities_per_sq_ft'):
        monthly_utilities_per_sq_ft = request.args.get('monthly_utilities_per_sq_ft')

    if request.args.get('monthly_internet_fee'):
        monthly_internet_fee = request.args.get('monthly_internet_fee')

    if request.args.get('monthly_management_fee'):
        monthly_management_fee = request.args.get('monthly_management_fee')

    result = utils_db.save_place_monthly_costs(place_id, 
                        monthly_property_tax_per_100k,
                        monthly_mortgage_insurance_per_100k,
                        monthly_short_term_insurance_per_100k,
                        monthly_utilities_per_sq_ft,
                        monthly_internet_fee,
                        monthly_management_fee)

    return jsonify(result)


####################
# HELPER FUNCTIONS #
####################

# none

