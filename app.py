from flask import Flask, render_template, jsonify, request, redirect
import vars
import utils
import sys
sys.path.append("classes")
from place import Place
import requests
import json
import datetime
import time
import math
from decimal import Decimal

import pdb


##########
# CONFIG #
##########

app = Flask(__name__)


#########
# PAGES #
#########

@app.route("/")
def hello_world():
    utils.log(None, 'page hello_world', 'page load')
    places = get_places()
    return render_template("home.html", places=places, count=len(places))

@app.route("/add_place")
def add_place():
    utils.log(None, 'page add_place', 'page load')
    return render_template("add_place.html")

@app.route("/view_place/<place_id>")
def view_place(place_id):
    utils.log(None, 'page view_place(%s)' % place_id, None)
    place = get_place(place_id)
    return render_template("view_place.html", place=place)

@app.route("/view_place/<place_id>/queue")
def initiate_place_scrape(place_id):
    utils.log(None, 'page initiate_place_scrape(%s)' % place_id, 'page load')
    place = get_place(place_id)
    session_id = get_session_id(place_id, place.name)
    insert_sqs_place_message(session_id, place)

    #redirect to get_listings_airbnb with session_id
    return redirect("/process_place_queue")

# get search listings from airbnb
@app.route("/process_place_queue")
def process_place_queue():
    utils.log(None, 'page process_place_queue', 'page load')
    return render_template("process_place_queue.html")

@app.route("/queue_all_calendar")
def queue_all_calendar():
    utils.log(None, 'page queue_all_calendar', 'page load')
    sessions = get_sessions()
    sessions_count = len(sessions)

    queued = False
    queued_count = 0
    if request.args.get('session_id'):
        queued = request.args.get('session_id')
        queued_count = request.args.get('count')

    return render_template("queue_all_calendar.html", sessions=sessions, count=sessions_count, queued=queued, queued_count=queued_count)

@app.route("/queue_all_calendar/<session_id>")
def queue_all_calendar_go(session_id):
    count = queue_calendar_sqs_for_session(session_id)
    return redirect("/queue_all_calendar?session_id=%s&count=%s" % (session_id, count))

@app.route('/log')
def show_log(minutes=30):
    utils.log(None, 'page show_log()', 'page load')

    if request.args.get('minutes') == 'recent':
        minutes = 9999
        logs = get_log_most_recent()

    else:
        if request.args.get('minutes') != None:
            minutes = int(request.args.get('minutes'))

        logs = get_log(time_delta=minutes)

    return render_template("show_log.html", logs=logs, minutes=int(minutes))

##################
# Non-page URL"s #
##################

# call google geocode service and get the name and coords for a location
@app.route("/_get_place/<place>")
def get_place_google(place):
    utils.log(None, 'get_place_google', 'place_id = %s' % place)

    place = get_google_place(place)

    # see if this place already exists in the db
    response = utils.pg_sql("select * from place where s_google_place_id = %s", (place.place_id,))
    # if an Item was returned, this place is already in the db
    
    if len(response) == 0:
        return jsonify(place.img_url)
    else:
        return jsonify("place already in database")


# insert a place into database
@app.route("/_insert_place/", methods=["POST"])
def insert_place():
    utils.log(None, 'insert_place', None)

    str_place = request.form["hidden_place"]
    
    place = get_google_place(str_place)

    sql = "insert into place (s_google_place_id, s_name, s_lat, s_lng, s_ne_lat, s_ne_lng, s_sw_lat, s_sw_lng, dt_insert) " \
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    params = (place.place_id, place.name, place.lat, place.lng, place.ne_lat, place.ne_lng, place.sw_lat, place.sw_lng, place.insert_date)
    utils.pg_sql(sql, params)

    return redirect("/")


@app.route("/_process_place_queue")
def _process_place_queue():
    utils.log(None, '_process_place_queue', 'Preparing to process queue')

    message = get_one_sqs_place_message()

    # ? is this a place?
    #   a: yes, this is a geo coord place
    #     get results
    #       ? are there more than 300 reults?
    #         break out quadrants, re-queue
    #       ? are there less than 300 results?
    #         save/queue the listings
    # ? is this a listings page?
    #   a: yes, this is a listings page
    #     get the listings page
    #       loop through every listing on this page and
    #         save/update property
    #         queue listing
    # ? is this a listing
    #   a: yes, this is a listing
    #     get the listing
    #       update the listing with necessary info
    #       queue a calendar request
    # ? is this a listing calendar request?
    #   yes, this is a listing calendar request
    #     get the availability
    #     save/update the availability
    
    #try:
    # is
    
    if message == '':
        utils.log(None, '_process_place_queue', 'No messages found in queue')

        output = "No messages found in queue, exiting"

    else:
        m = message.message_attributes
        session_id = m['session_id']['StringValue']
        m_type = m['type']['StringValue']

        
        if m_type == 'place':
            utils.log(session_id, '_process_place_queue', 'm_type == place')

            place = Place(m['place_id']['StringValue'], m['name']['StringValue'], 0.0, 0.0, 
                          m['ne_lat']['StringValue'], m['ne_lng']['StringValue'],
                          m['sw_lat']['StringValue'], m['sw_lng']['StringValue'])

        
            # get results
            results = get_place_search(session_id, place.search_url)

            #are there more than 300 listings?
            # yes = break into 4 quadrants, queue each
            if results['results_json']['metadata']['listings_count'] >= 300:
                utils.log(session_id, '_process_place_queue', 'place, >300 results for %s' % m['name']['StringValue'])

                insert_four_new_place_quadrants(session_id, place)
                delete_sqs_place_message(message)
                output = "More than 300 results for place '%s'. Queued 4 new searches." % m['name']['StringValue']


            # no = queue listing pages
            elif results['results_json']['metadata']['listings_count'] < 300:
                utils.log(session_id, '_process_place_queue', 'place, <300 results for %s' % m['name']['StringValue'])

                listings_count = results['results_json']['metadata']['listings_count']

                pages_inserted = insert_sqs_listing_overview_pages(session_id, listings_count, place)

                delete_sqs_place_message(message)

                output = "less than 300, inserted %s pages into queue" % pages_inserted

            else:
                utils.log(session_id, '_process_place_queue', 'Unknown value for results[results_json][metadata][listings_count]')

                output = "Unknown value for results[results_json][metadata][listings_count]"


        # if this is a listing overview page
        #   save all the listings on this page, queue up a listing search
        elif m_type == 'listing overview':
        
            place = Place(m['place_id']['StringValue'], m['name']['StringValue'], 0.0, 0.0, 
                          m['ne_lat']['StringValue'], m['ne_lng']['StringValue'],
                          m['sw_lat']['StringValue'], m['sw_lng']['StringValue'])

            utils.log(session_id, '_process_place_queue', 'listing overview, preparing to save listings')

            time_start = time.time()

            # get results
            results = get_place_search(session_id, place.search_url)

            search_results = results['results_json']['search_results']

            # loop through results and save to db
            # the sqs message to get detail and calendar is also performed in here
            listings_count = save_listings(place.place_id, session_id, search_results)

            # delete this listing overview sqs
            delete_sqs_place_message(message)

            #log
            time_end = time.time()
            utils.log(session_id, '_process_place_queue', 'listing overview, saved %s listings' % listings_count, None, time_end - time_start)

            output = 'Listing overview, saved %s places, queued individual listing pull' % listings_count

        # if this is a listing
        #   tbd
        elif m_type == 'listing detail':
            utils.log(session_id, '_process_place_queue', 'listing detail, preparing to save listing detail', m['url']['StringValue'])

            listing = get_place_search(session_id, m['url']['StringValue'])
            save_listing_detail(session_id, listing)

            delete_sqs_place_message(message)

            output = 'Listing details saved/updated'


        # if this is a calendar request
        #   tbd
        elif m_type == 'calendar':
            listing_id = m['listing_id']['StringValue']
            utils.log(session_id, '_process_place_queue', 'calendar, preparing to save calendar detail', m['url']['StringValue'])
            
            calendar = get_place_search(session_id, m['url']['StringValue'])

            save_calendar_detail(session_id, listing_id, calendar['calendar_months'])

            delete_sqs_place_message(message)

            output = 'Processed a calendar request for listing id %s' % listing_id

        else:
            utils.log(session_id, '_process_place_queue', 'Unknown value for message.message_attributes[type][StringValue]')

            output = 'm_type == ??? not sure what kind of sqs message this was ???'
        

    return jsonify(output)


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
    utils.log(session_id, 'get_place_search', 'Get airbnb listings page', place_search_url, elapsed_time)

    return result.json()



###########
# HELPERS #
###########

# get one place from db
def get_place(place_id):
    utils.log(None, 'get_place', 'get one place from db')

    p = Place()
    p.get_place_from_id(place_id)
    return p

# get all places from db, return an array of class Place
def get_places():
    utils.log(None, 'get_places', 'get all places from db')

    places = []

    results = utils.pg_sql("select * from place")

    for place in results:
        p = Place(place["s_google_place_id"], place["s_name"], place["s_lat"], place["s_lng"], place["s_ne_lat"], place["s_ne_lng"], place["s_sw_lat"], place["s_sw_lng"])
        places.append(p)
    

    return places

# get log items
def get_log(num_items=10000, time_delta=30):
    results_after = datetime.datetime.now() - datetime.timedelta(minutes=time_delta)
    response = utils.pg_sql("select * from qbnb_log where dt_insert > %s order by dt_insert", (str(results_after),))
    
    return response

# get most recent log items
def get_log_most_recent():
    utils.log(None, 'get_log_most_recent', 'get recent log items')

    response = utils.pg_sql("select * from (select * from qbnb_log order by dt_insert desc limit 500) t1 order by dt_insert")

    return response


#get all existing sessions in the db
def get_sessions():
    utils.log(None, 'get_sessions', 'getting all sessions')


    response = utils.pg_sql("select * from session")
    return response


#if this function is called, we will insert a row into the database to log the session
def get_session_id(place_id, place_name):
    utils.log(None, 'get_session_id', 'get fresh session_id')
    session_id = utils.get_random_string(5)
    insert_session(session_id, place_id, place_name)
    return session_id


#break a place up into 4 new quadrants, save each to the queue
def insert_four_new_place_quadrants(session_id, place):
    utils.log(session_id, 'insert_four_new_place_quadrants', 'Create 4 new quadrants, save to queue')

    nw = build_place_quadrant(place, 'nw')
    ne = build_place_quadrant(place, 'ne')
    sw = build_place_quadrant(place, 'sw')
    se = build_place_quadrant(place, 'se')

    insert_sqs_place_message(session_id, nw)
    insert_sqs_place_message(session_id, ne)
    insert_sqs_place_message(session_id, sw)
    insert_sqs_place_message(session_id, se)

# create a new place which is a quadrant of a previous place
def build_place_quadrant(place, quadrant):
    utils.log(None, 'build_place_quadrant', 'create a new quadrant, q_%s' % quadrant)

    place_quadrant = Place("%s | q_%s" % (place.place_id, quadrant), "%s | q_%s" % (place.name, quadrant))

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
def insert_sqs_listing_overview_pages(session_id, listings_count, place):
    
    # get how many pages of results there are in the place quadrant and insert sqs messages for each
    total_pages = math.ceil(listings_count / 18)

    utils.log(session_id, 'insert_sqs_listing_overview_pages', 'insert %s listing overview pages' % total_pages)

    
    this_page = 1
    while True:
        insert_sqs_listing_overview_message(session_id, place, this_page)
        
        this_page += 1
        if this_page > total_pages:
            break

    return total_pages
        

def insert_sqs_listing_overview_message(session_id, place, page):
    time_start = time.time()

    sqs = get_sqs_place_queue()

    message_body = "listing overview page %s, session %s, %s" % (page, session_id, place.name)
    message_attributes = build_sqs_place_attributes(session_id, place, 'listing overview', page)
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    #log
    elapsed_time = time.time() - time_start
    utils.log(session_id, 'insert_sqs_listing_overview_message', 'Sent an sqs for %s, listing overview page %s' % (place.name, page))

    return True


def get_sqs_place_queue():
    utils.log(None, 'get_sqs_place_queue', None)
    return vars.sqs.Queue(url=vars.sqs_place)


# insert sqs for listing detail pages
def insert_sqs_listing_detail_page(session_id, listing_id):

    sqs = get_sqs_place_queue()

    listing_url = 'https://www.airbnb.com/api/v2/listings/%s?key=%s&_format=v1_legacy_for_p3' % (listing_id, vars.airbnb_key)

    message_body = "listing detail, session %s, listing id %s" % (session_id, listing_id)
    message_attributes = {
        'type': {
            'StringValue': 'listing detail',
            'DataType': 'String'
        },
        'session_id': {
            'StringValue': str(session_id),
            'DataType': 'String'
        },
        'listing_id': {
            'StringValue': str(listing_id),
            'DataType': 'String'
        },
        'url': {
            'StringValue': str(listing_url),
            'DataType': 'String'
        }
    }

    utils.log(session_id, 'insert_sqs_listing_detail_pages', 'Inserting listing detail sqs for listing_id %s' % listing_id, listing_url)
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    return True

# insert an sqs item to grab calendar information
def insert_sqs_listing_calendar(session_id, listing_id):

    sqs = get_sqs_place_queue()
    
    # get 3 months ago
    _month = (datetime.datetime.today() - datetime.timedelta(days=60)).month
    _year = (datetime.datetime.today() - datetime.timedelta(days=60)).year
    
    listing_url = 'https://www.airbnb.com/api/v2/calendar_months?key=%s&currency=USD&locale=en&listing_id=%s&month=%s&year=%s&count=3&_format=with_conditions' % (vars.airbnb_key, listing_id, _month, _year)

    message_body = "calendar, session %s, listing id %s" % (session_id, listing_id)
    message_attributes = {
        'type': {
            'StringValue': 'calendar',
            'DataType': 'String'
        },
        'session_id': {
            'StringValue': str(session_id),
            'DataType': 'String'
        },
        'listing_id': {
            'StringValue': str(listing_id),
            'DataType': 'String'
        },
        'url': {
            'StringValue': listing_url,
            'DataType': 'String'
        }
    }

    utils.log(session_id, 'insert_sqs_listing_calendar', 'Inserting calendar sqs for listing_id %s' % listing_id, listing_url)
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    return True


def insert_sqs_place_message(session_id, place):
    time_start = time.time()

    sqs = get_sqs_place_queue()

    message_body = "place session %s, %s" % (session_id, place.name)
    message_attributes = build_sqs_place_attributes(session_id, place, 'place')
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    #log
    elapsed_time = time.time() - time_start
    utils.log(session_id, 'insert_sqs_place_message', 'Inserted sqs for %s' % place.name, None, elapsed_time)

    return True

def build_sqs_place_attributes(session_id, place, process_type, page=1):
    utils.log(session_id, 'build_sqs_place_attributes', 'place attributes page %s' % page)

    message_attributes = {
        'type': {
            'StringValue': str(process_type),
            'DataType': 'String'
        },
        'session_id': {
            'StringValue': str(session_id),
            'DataType': 'String'
        },
        'place_id': {
            'StringValue': str(place.place_id),
            'DataType': 'String'
        },
        'name': {
            'StringValue': str(place.name),
            'DataType': 'String'
        },
        'page': {
            'StringValue': str(page),
            'DataType': 'String'
        },
        'ne_lat': {
            'StringValue': str(place.ne_lat),
            'DataType': 'String'
        },
        'ne_lng': {
            'StringValue': str(place.ne_lng),
            'DataType': 'String'
        },
        'sw_lat': {
            'StringValue': str(place.sw_lat),
            'DataType': 'String'
        },
        'sw_lng': {
            'StringValue': str(place.sw_lng),
            'DataType': 'String'
        },
        'img_url': {
            'StringValue': str(place.img_url),
            'DataType': 'String'
        }
    }
    return message_attributes

def get_one_sqs_place_message():
    
    sqs = get_sqs_place_queue()
    messages = sqs.receive_messages(MaxNumberOfMessages=1, MessageAttributeNames=['All'])
    if len(messages) > 0:
        utils.log(None, 'get_one_sqs_place_message', 'return type = %s' % messages[0].message_attributes['type']['StringValue'])
        return messages[0]
    else:
        utils.log(None, 'get_one_sqs_place_message', 'Nothing returned')
        return ''


def delete_sqs_place_message(message):

    m = message.message_attributes
    utils.log(m['session_id']['StringValue'], 'delete_sqs_place_message', 'Preparing to delete an sqs message, %s' % message.body)

    message.delete()

def insert_session(session_id, place_id, place_name):
    utils.log(session_id, 'insert_session', 'Insert row into Session table')

    sql = "insert into session (s_session_id, s_google_place_id, dt_insert) values (%s, %s, %s)"
    params = (session_id, place_id, str(datetime.datetime.now()))

    utils.pg_sql(sql, params)


# get a listing from the database
def get_listing(session_id, listing_id):

    sql = "select * from listing where listing_id = %s and session_id = %s"
    params = (listing_id, session_id)
    response = utils.pg_sql(sql, params)

    if len(response) == 0:
        utils.log(session_id, 'get_listing', 'Listing does not exist, session_id = %s, listing_id = %s' % (session_id, listing_id))
    else:
        utils.log(session_id, 'get_listing', 'Found listing, session_id = %s, listing_id = %s' % (session_id, listing_id))

    return response

# loop through json from /search and save all listings to db
def save_listings(place_id, session_id, listings):
    utils.log(session_id, 'save_listings', 'loop through and save listings')

    time_start = time.time()

    i = 0
    for listing in listings:
        listing_id = listing['listing']['id']

        

        # 1. get listing_id from db
        # 2. if empty data set or session_id <> session_id
        #       insert, and queue up detail and calendar
        # 3. if session_id is same as this session id, 
        #       do not re-save, do not insert detail/calendar

        l = get_listing(session_id, listing_id)
        
        # no listing is returned, this is a new one, so insert
        if l['Count'] == 0:
            # insert
            utils.log(session_id, 'save_listings', 'Inserting new listing id %s' % listing_id)
            insert_listing(place_id, session_id, listing)

            # queue a listing detail search
            insert_sqs_listing_detail_page(session_id, listing_id)
            insert_sqs_listing_calendar(session_id, listing_id)

            i += 1

        # if a listing is returned...
        else:
            # but does not have the same session_id, then insert
            if session_id != l['Items'][0]['session_id']:
                # insert
                utils.log(session_id, 'save_listings', 'Updating listing id %s' % listing_id)
                insert_listing(place_id, session_id, listing)

                # queue a listing detail search
                insert_sqs_listing_detail_page(session_id, listing_id)
                insert_sqs_listing_calendar(session_id, listing_id)

                i += 1
            
            # and has the same session_id, then do not re-insert
            else:
                # already inserted this listing in this session
                utils.log(session_id, 'save_listings', 'Listing id %s exists for session %s. Moving on' % (listing_id, session_id))

        

    #log
    elapsed_time = time.time() - time_start
    utils.log(session_id, 'save_listings', 'Inserted %s items into Listing table' % i, None, elapsed_time)

    return i

def insert_listing(place_id, session_id, listing):
    
    listing_id = listing['listing']['id']
    listing_name = listing['listing']['name']
    star_rating = listing['listing']['star_rating']
    room_type = listing['listing']['room_type']
    rate = listing['pricing_quote']['rate']['amount']
    reviews_count = listing['listing']['reviews_count']
    person_capacity = listing['listing']['person_capacity']
    is_business_travel_ready = listing['listing']['is_business_travel_ready']
    lat = listing['listing']['lat']
    lng = listing['listing']['lng']
    is_new_listing = listing['listing']['is_new_listing']
    can_instant_book = listing['pricing_quote']['can_instant_book']
    picture_url = listing['listing']['picture_url']
    localized_city = listing['listing']['localized_city']
    picture_count = listing['listing']['picture_count']
    host_id = listing['listing']['primary_host']['id']
    host_name = listing['listing']['primary_host']['first_name']
    beds = listing['listing']['beds']
    bedrooms = listing['listing']['bedrooms']

    # upsert: if this listing_id does not exist then insert, otherwise update
    sql = "insert into listing (i_listing_id, s_google_place_id, s_session_id, s_listing_name, " \
                                "d_star_rating, s_room_type, d_rate, i_reviews_count, i_person_capacity, " \
                                "b_is_business_travel_ready, s_lat, s_lng, b_is_new_listing, " \
                                "b_can_instant_book, s_picture_url, s_localized_city, i_picture_count, " \
                                "i_host_id, s_host_name, i_beds, i_bedrooms, dt_insert) " \
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
            "on conflict (i_listing_id, s_google_place_id) " \
            "do update set (s_session_id, s_listing_name, d_star_rating, s_room_type, d_rate, i_reviews_count, " \
                            "i_person_capacity, b_is_business_travel_ready, s_lat, l_lng, b_is_new_listing, " \
                            "b_can_instant_book, s_picture_url, s_localized_city, i_picture_count, " \
                            "i_host_id, s_host_name, i_beds, i_bedrooms, dt_insert) " \
                            " = " \
                            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    params = (listing_id, place_id, session_id, listing_name, 
                star_rating, room_type, rate, reviews_count, person_capacity,
                is_business_travel_ready, lat, lng, is_new_listing, 
                can_instant_book, picture_url, localized_city, picture_count,
                host_id, host_name, beds, bedrooms, str(datetime.datetime.now()),
                session_id, listing_name, 
                star_rating, room_type, rate, reviews_count, person_capacity,
                is_business_travel_ready, lat, lng, is_new_listing, 
                can_instant_book, picture_url, localized_city, picture_count,
                host_id, host_name, beds, bedrooms, str(datetime.datetime.now()))

    utils.pg_sql(sql, params)
    
# save/update listing detail
def save_listing_detail(session_id, listing):
    listing_id = listing['listing']['id']

    utils.log(session_id, 'save_listing_detail', 'Saving listing detail for listing_id %s' % listing_id)

    d_bathrooms = listing['listing']['bathrooms']
    s_bed_type = listing['listing']['bed_type']
    b_has_availability = listing['listing']['has_availability']
    i_min_nights = listing['listing']['min_nights']
    s_neighborhood = listing['listing']['neighborhood']
    s_property_type = listing['listing']['property_type']
    s_zipcode = listing['listing']['zipcode']
    s_calendar_updated_at = listing['listing']['calendar_updated_at']
    s_check_in_time = listing['listing']['check_in_time']
    s_check_out_time = listing['listing']['check_out_time']
    i_cleaning_fee = listing['listing']['cleaning_fee_native']
    s_description = listing['listing']['description'],
    b_is_location_exact = listing['listing']['is_location_exact']

    sql = "update listing " \
            "set (d_bathrooms, s_bed_type, b_has_availability, i_min_nights, s_neighborhood, " \
                "s_property_type, s_zipcode, s_calendar_updated_at, s_check_in_time, " \
                "s_check_out_time, i_cleaning_fee, s_description, b_is_location_exact) " \
            " = " \
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    params = (d_bathrooms, s_bed_type, b_has_availability, i_min_nights, s_neighborhood,
                s_property_type, s_zipcode, s_calendar_updated_at, s_check_in_time,
                s_check_out_time, i_cleaning_fee, s_description, b_is_location_exact)

    utils.pg_sql(sql, params)

    return True

#save/update calendar detail for a listing
def save_calendar_detail(session_id, listing_id, calendar_months):
    utils.log(session_id, 'save_calendar_detail', 'Saving calendar detail for listing_id %s' % listing_id)

    i = 0
    for month in calendar_months:
        for day in month['days']:

            booking_date = day['date']
            available = day['available']
            price = day['price']['local_price']

            # call the postgres function created to check to see if something already exists before inserting
            sql = "INSERT INTO calendar(i_listing_id, dt_booking_date, b_available, i_price, s_session_id, dt_insert) " \
                "VALUES (%s, %s, %s, %s, %s, now()) " \
                "ON CONFLICT (i_listing_id, dt_booking_date) " \
                "DO UPDATE SET (b_available, i_price, s_session_id, dt_insert) = (%s, %s, %s, now());"
            params = (listing_id, booking_date, available, price, session_id, available, price, session_id)

            i += 1

    utils.log(session_id, 'save_calendar_detail', 'Inserted %s calendar items into db' % i)

    return True

# queue a calendar request for every listing with a specific session_id
def queue_calendar_sqs_for_session(session_id):
    # get all listings with this session id
    
    listings = utils.pg_sql("select * from listing where s_session_id = %s")
    if len(listings) == 0:
        utils.log(session_id, 'queue_calendar_sqs_for_session', 'No listings returned for session_id = %s' % session_id)
    else:

        # loop through listings and queue a calendar sqs
        i = 0
        for listing in listings:
            insert_sqs_listing_calendar(session_id, listing['listing_id'])
            i += 1
            
        utils.log(session_id, 'queue_calendar_sqs_for_session', 'Queued %s sqs calendar items' % i)

    return i

#######
# RUN #
#######

if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0")

