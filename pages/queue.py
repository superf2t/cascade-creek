from flask import Blueprint, render_template, jsonify, request, redirect
import vars
import utils
import utils_api
import utils_db
import utils_sqs
from place import Place
import datetime
import time
import math
from decimal import Decimal
import requests
import flask_login

import pdb

queue = Blueprint('queue', __name__, template_folder='templates')
login_manager = flask_login.LoginManager()

#########
# PAGES #
#########

# get search listings from airbnb
@queue.route("/process_place_queue")
@flask_login.login_required
def process_place_queue_page():
    utils.log('process_place_queue_page', 'page load')
    #try checking to see if we're at home
    check_yo_self = am_i_at_home()
    return render_template("process_place_queue.html", check_yo_self=check_yo_self)

@queue.route("/queue_all_calendar")
@flask_login.login_required
def queue_all_calendar_page():
    utils.log('queue_all_calendar_page', None)
    places = utils_db.get_places_history()
    places_count = len(places)

    queued = False
    queued_count = 0
    if request.args.get('place_id'):
        queued = request.args.get('place_id')
        queued_count = request.args.get('count')

    return render_template("queue_all_calendar.html", places=places, count=places_count, queued=queued, queued_count=queued_count)

@queue.route("/queue_all_calendar/<place_id>")
@flask_login.login_required
def queue_all_calendar_for_place_id_page(place_id):
    # queue all existing calendar listings
    count = utils_db.queue_calendar_sqs_for_place(place_id)
    # queue the base geo search so we can get new places
    place = utils_db.get_place(place_id)
    utils_sqs.insert_sqs_place_message(place)
    return redirect("/queue/queue_all_calendar?place_id=%s&count=%s" % (place_id, count))


##################
# Non-page URL"s #
##################

@queue.route("/_process_place_queue") #@flask_login.login_required
def process_place_queue_api():
    utils.log('process_place_queue_api', 'Preparing to process queue')
    output = get_a_message_and_process_it()        

    return jsonify(output)




####################
# HELPER FUNCTIONS #
####################

# do what the name says
def get_a_message_and_process_it():
    total_message_count = utils_sqs.get_sqs_message_count()
    output = {
        "message_count": total_message_count,
        "message": ""
    }

    # ? is this a place? Yes =
    #     get results
    #       ? are there more than 300 reults?
    #         break out quadrants, re-queue
    #       ? are there less than 300 results?
    #         save/queue the listings
    # ? is this a listings page? Yes =
    #     get the listings page
    #       loop through every listing on this page and
    #         save/update property
    #         ? is this a new listing? Yes =
    #           queue listing
    #           queue calendar
    # ? is this a listing? Yes =
    #     get the listing
    #       update the listing with necessary info
    #       queue a calendar request
    # ? is this a listing calendar request? Yes = 
    #     get the availability
    #     save/update the availability

    if total_message_count == '0':
        utils.log('get_a_message_and_process_it', 'No messages found in queue')

        output['message'] = "No messages in queue, exiting"

    else:
        message = utils_sqs.get_one_sqs_place_message()

        m = message.message_attributes
        m_type = m['type']['StringValue']

        
        if m_type == 'place':
            utils.log('get_a_message_and_process_it', 'm_type == place')

            place = Place(m['place_id']['StringValue'], m['name']['StringValue'], 0.0, 0.0, 
                          m['ne_lat']['StringValue'], m['ne_lng']['StringValue'],
                          m['sw_lat']['StringValue'], m['sw_lng']['StringValue'])

        
            # get results
            results = utils_api.get_place_search(place.search_url)

            #are there more than 300 listings?
            # yes = break into 4 quadrants, queue each
            if results['results_json']['metadata']['listings_count'] >= 300:
                utils.log('get_a_message_and_process_it', 'place, >300 results for %s' % m['name']['StringValue'])

                insert_four_new_place_quadrants(place)
                utils_sqs.delete_sqs_place_message(message)
                output['message'] = "More than 300 results for place '%s'. Queued 4 new searches." % m['name']['StringValue']


            # no = queue listing pages
            elif results['results_json']['metadata']['listings_count'] < 300:
                utils.log('get_a_message_and_process_it', 'place, <300 results for %s' % m['name']['StringValue'])

                listings_count = results['results_json']['metadata']['listings_count']

                pages_inserted = insert_sqs_listing_overview_pages(listings_count, place)

                utils_sqs.delete_sqs_place_message(message)

                output['message'] = "less than 300, inserted %s pages into queue" % pages_inserted

            else:
                utils.log('get_a_message_and_process_it', 'Unknown value for results[results_json][metadata][listings_count]')

                output['message'] = "Unknown value for results[results_json][metadata][listings_count]"


        # if this is a listing overview page
        #   save all the listings on this page, queue up a listing search
        elif m_type == 'listing overview':
        
            place = Place(m['place_id']['StringValue'], m['name']['StringValue'], 0.0, 0.0, 
                          m['ne_lat']['StringValue'], m['ne_lng']['StringValue'],
                          m['sw_lat']['StringValue'], m['sw_lng']['StringValue'])

            utils.log('get_a_message_and_process_it', 'listing overview, preparing to save listings')

            time_start = time.time()

            # get results
            results = utils_api.get_place_search(place.search_url)

            search_results = results['results_json']['search_results']

            # loop through results and save to db
            # the sqs message to get detail and calendar is also performed in here
            listings_count = save_listings(place.place_id, search_results)

            # delete this listing overview sqs
            utils_sqs.delete_sqs_place_message(message)

            #log
            time_end = time.time()
            utils.log('get_a_message_and_process_it', 'listing overview, saved %s listings' % listings_count, None, time_end - time_start)

            output['message'] = 'Listing overview, saved %s places, queued individual listing pull' % listings_count

        # if this is a listing
        #   tbd
        elif m_type == 'listing detail':
            utils.log('get_a_message_and_process_it', 'listing detail, preparing to save listing detail', m['url']['StringValue'])

            listing = utils_api.get_place_search(m['url']['StringValue'])
            if listing.get('error_type', 'no error') == 'no error':
                utils_db.save_listing_detail(listing)
                output['message'] = 'Listing details saved/updated, listing_id: %s' % listing['listing']['id']
            else:
                utils.log('get_a_message_and_process_it', 'error %s: %s' % (listing['error_type'], listing['error_message']), m['url']['StringValue'])
                output['message'] = 'No permission to access listing detail for %s' % m['url']['StringValue']

            utils_sqs.delete_sqs_place_message(message)
            


        # if this is a calendar request
        #   tbd
        elif m_type == 'calendar':
            listing_id = m['listing_id']['StringValue']
            utils.log('get_a_message_and_process_it', 'calendar, preparing to save calendar detail', m['url']['StringValue'])
            
            calendar = utils_api.get_place_search(m['url']['StringValue'])

            #pdb.set_trace()

            #if error_type != "no_access"
            if calendar.get('error_type', 'no error') == 'no error':
                utils_db.save_calendar_detail(listing_id, calendar['calendar_months'])
                output['message'] = 'Processed a calendar request for listing id %s' % listing_id
            else:
                utils.log('get_a_message_and_process_it', 'error %s: %s' % (calendar['error_type'], calendar['error_message']), m['url']['StringValue'])
                output['message'] = 'No permission to access calendar for listing id %s' % listing_id

            utils_sqs.delete_sqs_place_message(message)            

        else:
            utils.log('get_a_message_and_process_it', 'Unknown value for message.message_attributes[type][StringValue]')

            output['message'] = 'm_type == ??? not sure what kind of sqs message this was ???'
    
    return output



#break a place up into 4 new quadrants, save each to the queue
def insert_four_new_place_quadrants(place):
    utils.log('insert_four_new_place_quadrants', 'Create 4 new quadrants, save to queue')

    nw = build_place_quadrant(place, 'nw')
    ne = build_place_quadrant(place, 'ne')
    sw = build_place_quadrant(place, 'sw')
    se = build_place_quadrant(place, 'se')

    utils_sqs.insert_sqs_place_message(nw)
    utils_sqs.insert_sqs_place_message(ne)
    utils_sqs.insert_sqs_place_message(sw)
    utils_sqs.insert_sqs_place_message(se)

# create a new place which is a quadrant of a previous place
def build_place_quadrant(place, quadrant):
    utils.log('build_place_quadrant', 'create a new quadrant, q_%s' % quadrant)

    place_quadrant = Place(place.place_id, "%s | q_%s" % (place.name, quadrant))

    mid_lat = str((Decimal(place.ne_lat) + Decimal(place.sw_lat)) / 2)
    mid_lng = str((Decimal(place.ne_lng) + Decimal(place.sw_lng)) / 2)

    if quadrant == "nw":
        place_quadrant.ne_lat = place.ne_lat
        place_quadrant.ne_lng = mid_lng
        place_quadrant.sw_lat = mid_lat
        place_quadrant.sw_lng = place.sw_lng
    elif quadrant == "ne":
        place_quadrant.ne_lat = place.ne_lat
        place_quadrant.ne_lng = place.ne_lng
        place_quadrant.sw_lat = mid_lat
        place_quadrant.sw_lng = mid_lng
    elif quadrant == "sw":
        place_quadrant.ne_lat = mid_lat
        place_quadrant.ne_lng = mid_lng
        place_quadrant.sw_lat = place.sw_lat
        place_quadrant.sw_lng = place.sw_lng
    elif quadrant == "se":
        place_quadrant.ne_lat = mid_lat
        place_quadrant.ne_lng = place.ne_lng
        place_quadrant.sw_lat = place.sw_lat
        place_quadrant.sw_lng = mid_lng

    place_quadrant.lat = (Decimal(place_quadrant.ne_lat) + Decimal(place_quadrant.sw_lat)) / 2
    place_quadrant.lng = (Decimal(place_quadrant.ne_lng) + Decimal(place_quadrant.sw_lng)) / 2
    place_quadrant.set_img_url()
    place_quadrant.set_parent_place(place)

    return place_quadrant

# receive a place and insert as many listing overview pages as necessary
def insert_sqs_listing_overview_pages(listings_count, place):
    
    # get how many pages of results there are in the place quadrant and insert sqs messages for each
    total_pages = math.ceil(listings_count / 18)

    utils.log('insert_sqs_listing_overview_pages', 'insert %s listing overview pages' % total_pages)

    
    this_page = 1
    while True:
        utils_sqs.insert_sqs_listing_overview_message(place, this_page)
        
        this_page += 1
        if this_page > total_pages:
            break

    return total_pages   



# loop through json from /search and save all listings to db
def save_listings(place_id, listings):
    utils.log('save_listings', 'loop through and save listings')

    time_start = time.time()

    i = 0
    for listing in listings:
        listing_id = listing['listing']['id']

        _l = utils_db.get_listing(listing_id)
        
        # no listing is returned, this is a new one, so insert
        if len(_l) == 0:
            # insert
            utils.log('save_listings', 'Upserting new listing id %s' % listing_id)
            utils_db.upsert_listing(place_id, listing)

            # queue a listing detail search
            utils_sqs.insert_sqs_listing_detail_page(listing_id)
            utils_sqs.insert_sqs_listing_calendar(listing_id)

            i += 1

        # if a listing is returned...
        else:
            # already inserted this listing
            utils.log('save_listings', 'Listing id %s exists. Moving on' % listing_id)

        

    #log
    elapsed_time = time.time() - time_start
    utils.log('save_listings', 'Inserted %s items into Listing table' % i, None, elapsed_time)

    return i

#get current ip address and check against list
def am_i_at_home():
    ip_blacklist = ['73.231.']
    response = requests.get('http://icanhazip.com')

    if response.content[:7] in ip_blacklist:
        return True
    else:
        return False





