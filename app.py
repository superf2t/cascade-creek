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
#import ast # used for converting strings to dict
from boto3.dynamodb.conditions import Key, Attr

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
    places = get_places()
    return render_template("home.html", places=places, count=len(places))

@app.route("/add_place")
def add_place():
    return render_template("add_place.html")

@app.route("/view_place/<place_id>")
def view_place(place_id):
    place = get_place(place_id)
    return render_template("view_place.html", place=place)

@app.route("/view_place/<place_id>/queue")
def initiate_place_scrape(place_id):
    place = get_place(place_id)
    session_id = get_session_id(place_id)
    insert_sqs_place_message(session_id, place)

    #redirect to get_listings_airbnb with session_id
    return redirect("/process_place_queue")

# get search listings from airbnb
@app.route("/process_place_queue")
def process_place_queue():
    return render_template("process_place_queue.html")

@app.route('/log')
def show_log(minutes=30):
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
    try:
        place = get_google_place(place)

        # see if this place already exists in the db
        table = vars.dynamodb.Table("Place")
        response = table.get_item(
                Key={
                    "place_id": place.place_id
                }
            )
        # if an Item was returned, this place is already in the db
        try:
            item = response["Item"]
            return jsonify("place already in database")

        # if a KeyError is thrown, this place is not in the database
        except KeyError:
            return jsonify(place.img_url())

    # google returned no results
    except:
        return jsonify("google returned no results")

# insert a place into database
@app.route("/_insert_place/", methods=["POST"])
def insert_place():

    str_place = request.form["hidden_place"]
    
    place = get_google_place(str_place)
    table = vars.dynamodb.Table("Place")
    table.put_item(
            Item={
                "place_id": place.place_id,
                "name": place.name,
                "lat": str(place.lat),
                "lng": str(place.lng),
                "ne_lat": str(place.ne_lat),
                "ne_lng": str(place.ne_lng),
                "sw_lat": str(place.sw_lat),
                "sw_lng": str(place.sw_lng),
                "insert_date": str(place.insert_date)
            }
        )

    return redirect("/")


@app.route("/_process_place_queue")
def _process_place_queue():
    #log
    insert_qbnb_log({'session_id':'n/a',
                    'function':'_process_place_queue',
                    'action': 'Preparing to process queue', 
                    'place_id': '0', 
                    'elapsed_time': '0',
                    'insert_date': str(datetime.datetime.now())})

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
        #log
        insert_qbnb_log({'session_id':'n/a',
                        'function':'_process_place_queue',
                        'action': 'No messages found in queue', 
                        'place_id': '0', 
                        'elapsed_time': '0',
                        'insert_date': str(datetime.datetime.now())})

        output = "No messages found in queue, exiting"

    else:
        m = message.message_attributes
        session_id = m['session_id']['StringValue']

        
        if m['type']['StringValue'] == 'place':

            place = Place(m['place_id']['StringValue'], m['name']['StringValue'], 0.0, 0.0, 
                          m['ne_lat']['StringValue'], m['ne_lng']['StringValue'],
                          m['sw_lat']['StringValue'], m['sw_lng']['StringValue'])

        
            # get results
            results = get_place_search(session_id, place.search_url)

            #are there more than 300 listings?
            # yes = break into 4 quadrants, queue each
            if results['results_json']['metadata']['listings_count'] >= 300:

                #log
                insert_qbnb_log({'function':'_process_place_queue',
                                'action': 'Found more than 300 results, preparing to create/save 4 new quadrants', 
                                'session_id':str(session_id),
                                'place_name': str(m['name']['StringValue']),
                                'place_id':str(m['place_id']['StringValue']),
                                'elapsed_time': '0',
                                'insert_date': str(datetime.datetime.now())})

                insert_four_new_place_quadrants(session_id, place)
                delete_sqs_place_message(message)
                output = "More than 300 results for place '%s'. Queued 4 new searches." % m['name']['StringValue']


            # no = queue listing pages
            elif results['results_json']['metadata']['listings_count'] < 300:

                #log
                insert_qbnb_log({'function':'_process_place_queue',
                                'action': 'Found fewer than 300 results, preparing to process/save listings', 
                                'session_id':str(session_id),
                                'place_name': str(m['name']['StringValue']),
                                'place_id':str(m['place_id']['StringValue']),
                                'elapsed_time': '0',
                                'insert_date': str(datetime.datetime.now())})

                #pdb.set_trace()
                #next_offset = results['results_json']['metadata']['pagination']['next_offset']

                listings_count = results['results_json']['metadata']['listings_count']

                pages_inserted = insert_sqs_listing_overview_pages(session_id, 
                                                    listings_count, 
                                                    place)

                delete_sqs_place_message(message)

                output = "less than 300, inserted %s pages into queue" % pages_inserted

            else:

                #log
                insert_qbnb_log({'function':'_process_place_queue',
                                'action': 'Unknown value for results[results_json][metadata][listings_count]', 
                                'session_id': str(session_id),
                                'elapsed_time': '0',
                                'insert_date': str(datetime.datetime.now())})

                output = "Unknown value for results[results_json][metadata][listings_count]"


        # if this is a listing over page
        #   save all the listings on this page, queue up a listing search
        elif m['type']['StringValue'] == 'listing overview':
        
            place = Place(m['place_id']['StringValue'], m['name']['StringValue'], 0.0, 0.0, 
                          m['ne_lat']['StringValue'], m['ne_lng']['StringValue'],
                          m['sw_lat']['StringValue'], m['sw_lng']['StringValue'])

            #log
            insert_qbnb_log({'function':'_process_place_queue',
                            'action': 'message[type] == listing overview, preparing to save listings on this page',
                            'session_id': str(session_id),
                            'elapsed_time': '0',
                            'insert_date': str(datetime.datetime.now())})

            time_start = time.time()

            # get results
            results = get_place_search(session_id, place.search_url)

            search_results = results['results_json']['search_results']

            # loop through results and save to db
            save_listings(session_id, search_results)

            # loop through results and queue a listing detail search
            insert_sqs_listing_detail_pages(session_id, search_results)
            insert_sqs_listing_calendar(session_id, search_results)

            # delete this listing overview sqs
            delete_sqs_place_message(message)

            #log
            time_end = time.time()
            insert_qbnb_log({'function':'_process_place_queue - listing overview',
                            'action': 'Saved listings and queued up listing detail searches',
                            'session_id': str(session_id),
                            'elapsed_time': str(time_end - time_start),
                            'insert_date': str(datetime.datetime.now())})

            output = 'Listing overview, saved places, queued individual listing pull'        

        # if this is a listing
        #   tbd
        elif m['type']['StringValue'] == 'listing detail':

            #log
            insert_qbnb_log({'function':'_process_place_queue',
                            'action': 'message[type] == listing detail, preparing to process a listing',  
                            'session_id': str(session_id),
                            'place_search_url': str(m['url']['StringValue']),
                            'elapsed_time': '0',
                            'insert_date': str(datetime.datetime.now())})

            listing = get_place_search(session_id, m['url']['StringValue'])
            save_listing_detail(session_id, listing)

            output = 'Listing details saved/updated'


        # if this is a calendar request
        #   tbd
        elif m['type']['StringValue'] == 'calendar':

            #log
            insert_qbnb_log({'function':'_process_place_queue',
                            'action': 'message[type] == calendar, preparing to pull listing calendar',
                            'session_id': str(session_id),
                            'place_search_url': str(m['url']['StringValue']),
                            'elapsed_time': '0',
                            'insert_date': str(datetime.datetime.now())})

            listing_id = m['listing_id']['StringValue']
            calendar = get_place_search(session_id, m['url']['StringValue'])
            save_calendar_detail(session_id, listing_id, calendar['calendar_months'])

            output = 'Processed a calendar request for listing id %s' % listing_id

        else:
            #log
            insert_qbnb_log({'function':'_process_place_queue',
                            'action': 'Unknown value for message.message_attributes[type][StringValue]', 
                            'elapsed_time': '0',
                            'insert_date': str(datetime.datetime.now())})

            output = '??? not sure what kind of sqs message this was ???'
        

    return jsonify(output)


#######################
# 3rd PARTY API CALLS #
#######################

# call google api and return a place
def get_google_place(place):
    time_start = time.time()

    # call google api and bring it on back
    google_result = requests.get("https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s" % (place, vars.google_api_key_geocode))
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
    time_end = time.time()
    insert_qbnb_log({'function':'get_google_place', 
                    'place_name': str(r["formatted_address"]),
                    'place_id':str(r["place_id"]), 
                    'elapsed_time': str(time_end - time_start),
                    'insert_date': str(datetime.datetime.now())})
    
    return place

# call airbnb search page and return listings
def get_place_search(session_id, place_search_url):    
    time_start = time.time()

    print '\nplace_search_url:\n%s\n' % place_search_url
    result = requests.get(place_search_url)

    #log
    time_end = time.time()
    insert_qbnb_log({'session_id':str(session_id), 
                    'function':'get_place_search', 
                    'place_search_url': place_search_url, 
                    'elapsed_time': str(time_end - time_start),
                    'insert_date':str(datetime.datetime.now())})

    return result.json()



###########
# HELPERS #
###########

# get one place from db
def get_place(place_id):
    p = Place()
    p.get_place_from_id(place_id)
    return p

# get all places from dynamodb, return an array of class Place
def get_places():
    places = []
    table = vars.dynamodb.Table("Place")
    r = table.scan()
    for key, place in enumerate(r["Items"]):
        p = Place(place["place_id"], place["name"], place["lat"], place["lng"], place["ne_lat"], place["ne_lng"], place["sw_lat"], place["sw_lng"])
        places.append(p)

    return places

# get log items
def get_log(num_items=500, time_delta=30):
    results_after = datetime.datetime.now() - datetime.timedelta(minutes=time_delta)
    table = vars.dynamodb.Table('QbnbLog')
    r = table.scan(Limit=num_items, FilterExpression=Attr('insert_date').gt(str(results_after)))

    logs_sorted = sorted(r['Items'], key=lambda k: k['insert_date'])
    
    return logs_sorted

# get most recent log items
# there is no way to just pull "most recent" from dynamodb, so I'll need to loop until I find results
def get_log_most_recent():
    time_delta = 15
    table = vars.dynamodb.Table('QbnbLog')
    logs_sorted = {}

    while True:
        results_after = datetime.datetime.now() - datetime.timedelta(minutes=time_delta)
        result = table.scan(Limit=500, FilterExpression=Attr('insert_date').gt(str(results_after)))

        # if results are returned, then exit
        if result['Count'] != 0:
            logs_sorted = sorted(result['Items'], key=lambda k: k['insert_date'])
            break
        else:
            # otherwise, increase the time limit and search again
            if time_delta < 2000:
                time_delta *= 2
            else:
                time_delta += 1440

    

    return logs_sorted




#if this function is called, we will insert a row into the database to log the session
def get_session_id(place_id):
    session_id = utils.get_random_string(5)
    insert_session(session_id, place_id)
    return session_id


#break a place up into 4 new quadrants, save each to the queue
def insert_four_new_place_quadrants(session_id, place):
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
    time_end = time.time()
    insert_qbnb_log({'session_id':str(session_id), 
                    'function':'insert_sqs_listing_overview_message', 
                    'action': 'Sent an sqs for listing overview page %s' % page,
                    'place_name': str(place.name),
                    'place_id':str(place.place_id), 
                    'elapsed_time': str(time_end - time_start),
                    'insert_date':str(datetime.datetime.now())})

    return True


def get_sqs_place_queue():
    return vars.sqs.Queue(url=vars.sqs_place)


# insert sqs for listing detail pages
def insert_sqs_listing_detail_pages(session_id, listings):

    #log
    insert_qbnb_log({'session_id':str(session_id), 
                    'function':'insert_sqs_listing_detail_pages', 
                    'action': 'Preparing to insert sqs for listing detail pages',
                    'elapsed_time': 0,
                    'insert_date':str(datetime.datetime.now())})

    sqs = get_sqs_place_queue()

    i = 0
    for listing in listings:    
        listing_id = listing['listing']['id']

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
                'StringValue': 'https://www.airbnb.com/api/v2/listings/%s?key=%s&_format=v1_legacy_for_p3' % (listing_id, vars.airbnb_key),
                'DataType': 'String'
            }
        }
        sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

        i += 1

    return i

# insert an sqs item to grab calendar information
def insert_sqs_listing_calendar(session_id, listings):
    
    #log
    insert_qbnb_log({'session_id':str(session_id), 
                    'function':'insert_sqs_listing_calendar', 
                    'action': 'Preparing to insert sqs for listing calendars',
                    'elapsed_time': 0,
                    'insert_date':str(datetime.datetime.now())})

    sqs = get_sqs_place_queue()
    _month = datetime.date.today().month
    _year = datetime.date.today().year

    i = 0
    for listing in listings:    
        listing_id = listing['listing']['id']

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
                'StringValue': 'https://www.airbnb.com/api/v2/calendar_months?key=%s&currency=USD&locale=en&listing_id=%s&month=%s&year=%s&count=3&_format=with_conditions' % (vars.airbnb_key, listing_id, _month, _year),
                'DataType': 'String'
            }
        }
        sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

        i += 1

    return i


def insert_sqs_place_message(session_id, place):
    time_start = time.time()

    sqs = get_sqs_place_queue()

    message_body = "place session %s, %s" % (session_id, place.name)
    message_attributes = build_sqs_place_attributes(session_id, place, 'place')
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    #log
    time_end = time.time()
    insert_qbnb_log({'session_id':str(session_id), 
                    'function':'insert_sqs_place_message', 
                    'action': 'Sent a new sqs message to Place queue',
                    'place_name': str(place.name),
                    'place_id':str(place.place_id), 
                    'elapsed_time': str(time_end - time_start),
                    'insert_date':str(datetime.datetime.now())})

    return True

def build_sqs_place_attributes(session_id, place, process_type, page=1):
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
        return messages[0]
    else:
        return ''
    
    #try:
        #return ast.literal_eval(messages[0].body)
    #except IndexError:
        #return {"type": "no results" }

def delete_sqs_place_message(message):

    m = message.message_attributes
    insert_qbnb_log({'session_id':str(m['session_id']['StringValue']), 
                    'function':'delete_sqs_place_message', 
                    'action': 'Preparing to delete an sqs message from Place queue',
                    'place_name': str(m['name']['StringValue']),
                    'place_id':str(m['place_id']['StringValue']),
                    'elapsed_time': '0',
                    'insert_date':str(datetime.datetime.now())})

    message.delete()

def insert_session(session_id, place_id):
    table = vars.dynamodb.Table("Session")
    table.put_item(Item={"session_id": str(session_id), 
                        "place_id": str(place_id), 
                        "insert_date": str(datetime.datetime.now())})

def insert_qbnb_log(item):
    table = vars.dynamodb.Table("QbnbLog")
    table.put_item(Item=item)


# loop through json from /search and save all listings to db
def save_listings(session_id, listings):
    table = vars.dynamodb.Table("Listing")


    update_expr = 'SET session_id = :session_id, ' \
                    'star_rating = :star_rating, ' \
                    'listing_name = :listing_name, ' \
                    'beds = :beds, ' \
                    'bedrooms = :bedrooms, ' \
                    'room_type = :room_type, ' \
                    'rate = :rate, ' \
                    'reviews_count = :reviews_count, ' \
                    'person_capacity = :person_capacity, ' \
                    'is_business_travel_ready = :is_business_travel_ready, ' \
                    'lat = :lat, ' \
                    'lng = :lng, ' \
                    'is_new_listing = :is_new_listing, ' \
                    'can_instant_book = :can_instant_book, ' \
                    'picture_url = :picture_url, ' \
                    'instant_bookable = :instant_bookable, ' \
                    'localized_city = :localized_city, ' \
                    'picture_count = :picture_count, ' \
                    'host_name = :host_name, ' \
                    'host_id = :host_id'

    for listing in listings:
        # build listing json item
        key = {'listing_id': int(listing['listing']['id'])}

        expr_attr_val = {
            ":session_id": str(session_id),
            ":star_rating": str(listing['listing']['star_rating']),
            ":listing_name": str(listing['listing']['name']),
            ":beds": str(listing['listing']['beds']),
            ":bedrooms": str(listing['listing']['bedrooms']),
            ":room_type": str(listing['listing']['room_type']),
            ":rate": str(listing['pricing_quote']['rate']['amount']),
            ":reviews_count": str(listing['listing']['reviews_count']),
            ":person_capacity": str(listing['listing']['person_capacity']),
            ":is_business_travel_ready": str(listing['listing']['is_business_travel_ready']),
            ":lat": str(listing['listing']['lat']),
            ":lng": str(listing['listing']['lng']),
            ":is_new_listing": str(listing['listing']['is_new_listing']),
            ":can_instant_book": str(listing['pricing_quote']['can_instant_book']),
            ":picture_url": str(listing['listing']['picture_url']),
            ":instant_bookable": str(listing['listing']['instant_bookable']),
            ":localized_city": str(listing['listing']['localized_city']),
            ":picture_count": str(listing['listing']['picture_count']),
            ":host_name": str(listing['listing']['primary_host']['first_name']),
            ":host_id": str(listing['listing']['primary_host']['id'])
        }

        
        # check to see if it already exists
          # if yes, update
          # if no, insert

        table.update_item(Key=key, 
                            UpdateExpression=update_expr, 
                            ExpressionAttributeValues=expr_attr_val)

        print '\nSuccessfully inserted:\n'
        print expr_attr_val
        print '\n'
        """
        print '\nid: %s' % listing['listing']['id']
        print 'rate: %s' % listing['pricing_quote']['rate']['amount']
        print 'reviews_count: %s\n' % listing['listing']['reviews_count']
        """

# save/update listing detail
def save_listing_detail(session_id, listing):
    table = vars.dynamodb.Table("Listing")

    # build listing json item
    key = {'listing_id': int(listing['listing']['id'])}

    update_expr = 'SET session_id = :session_id, ' \
                    'bathrooms = :bathrooms, ' \
                    'has_availability = :has_availability, ' \
                    'min_nights = :min_nights, ' \
                    'neighborhood = :neighborhood, ' \
                    'property_type = :property_type, ' \
                    'zipcode = :zipcode, ' \
                    'bed_type = :bed_type, ' \
                    'calendar_updated_at = :calendar_updated_at, ' \
                    'check_in_time = :check_in_time, ' \
                    'check_out_time = :check_out_time, ' \
                    'cleaning_fee_native = :cleaning_fee_native, ' \
                    'description = :description, ' \
                    'is_location_exact = :is_location_exact'

    expr_attr_val = {
        ":session_id": str(session_id),
        ":bathrooms": str(listing['listing']['bathrooms']),
        ":has_availability": str(listing['listing']['has_availability']),
        ":min_nights": str(listing['listing']['min_nights']),
        ":neighborhood": str(listing['listing']['neighborhood']),
        ":property_type": str(listing['listing']['property_type']),
        ":zipcode": str(listing['listing']['zipcode']),
        ":bed_type": str(listing['listing']['bed_type']),
        ":calendar_updated_at": str(listing['listing']['calendar_updated_at']),
        ":check_in_time": str(listing['listing']['check_in_time']),
        ":check_out_time": str(listing['listing']['check_out_time']),
        ":cleaning_fee_native": str(listing['listing']['cleaning_fee_native']),
        ":description": str(listing['listing']['description']),
        ":is_location_exact": str(listing['listing']['is_location_exact'])
    }

    
    # check to see if it already exists
      # if yes, update
      # if no, insert

    table.update_item(Key=key, 
                        UpdateExpression=update_expr, 
                        ExpressionAttributeValues=expr_attr_val)

    print '\nSuccessfully inserted:\n'
    print expr_attr_val
    print '\n'

    return True

#save/update calendar detail for a listing
def save_calendar_detail(session_id, listing_id, calendar_months):
    table = vars.dynamodb.Table("Calendar")

    # build listing json item
    

    update_expr = 'SET session_id = :session_id, ' \
                    'price = :price, ' \
                    'available = :available, ' \
                    'insert_date = :insert_date'

    for month in calendar_months:
        for day in month['days']:

            key = {'listing_id': int(listing_id),
                    'booking_date': str(day['date'])}

            expr_attr_val = {
                ":session_id": str(session_id),
                ":available": day['available'],
                ":price": day['price']['local_price'],
                ":insert_date": str(datetime.datetime.now())
            }

            table.update_item(Key=key, 
                                UpdateExpression=update_expr, 
                                ExpressionAttributeValues=expr_attr_val)

    return True


#######
# RUN #
#######

if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0")

