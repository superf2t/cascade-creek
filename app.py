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
    session_id = utils.get_random_string(5)
    send_sqs_place_message(session_id, place)

    #redirect to get_listings_airbnb with session_id
    return redirect("/process_place_queue")

# get search listings from airbnb
@app.route("/process_place_queue")
def process_place_queue():
    return render_template("process_place_queue.html")

@app.route('/log')
def show_log(minutes=30):
    if request.args.get('minutes') != None:
        minutes = int(request.args.get('minutes'))

    logs = get_log(time_delta=minutes)
    logs_sorted = sorted(logs['Items'], key=lambda k: k['insert_date'])
    return render_template("show_log.html", logs=logs_sorted, minutes=int(minutes))

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

    # is this a place?
    # yes, this is a geo coord place
    #   get results
    #   are there more than 300 reults?
    #     break out quadrants, re-queue
    #   are there less than 300 results?
    #     save/queue the listings
    # is this a listings page?
    # yes, this is a listings page
    #   get the listings page
    #   loop through every listing on this page and
    #     save/update property
    #     queue listing
    # yes, this is a listing
    #   get the listing
    #   update the listing with necessary info
    #   queue a calendar request
    # yes, this is a listing calendar request
    #   get the availability
    #   save/update the availability
    
    #try:
    # is
    
    #pdb.set_trace()

    if message == '':
        #log
        insert_qbnb_log({'session_id':'n/a',
                        'function':'_process_place_queue',
                        'action': 'No messages found in queue', 
                        'place_id': '0', 
                        'elapsed_time': '0',
                        'insert_date': str(datetime.datetime.now())})

        output = "No messages found in queue, exiting"

    elif message.message_attributes['type']['StringValue'] == 'place':
        m = message.message_attributes
        place = Place(m['place_id']['StringValue'], m['name']['StringValue'], 0.0, 0.0, 
                      m['ne_lat']['StringValue'], m['ne_lng']['StringValue'],
                      m['sw_lat']['StringValue'], m['sw_lng']['StringValue'])

        # get results
        results = get_place_search(m['session_id']['StringValue'], place.search_url)

        #are there more than 300 listings?
        # yes = break into 4 quadrants, queue each
        if results['results_json']['metadata']['listings_count'] >= 300:

            #log
            insert_qbnb_log({'function':'_process_place_queue',
                            'action': 'Found more than 300 results, preparing to create/save 4 new quadrants', 
                            'session_id':str(m['session_id']['StringValue']),
                            'place_name': str(m['name']['StringValue']),
                            'place_id':str(m['place_id']['StringValue']),
                            'elapsed_time': '0',
                            'insert_date': str(datetime.datetime.now())})

            save_four_new_quadrants(m['session_id']['StringValue'], place)
            delete_sqs_place_message(message)
            output = "More than 300 results for place '%s'. Queued 4 new searches." % m['name']['StringValue']


        # no = queue listing pages
        elif results['results_json']['metadata']['listings_count'] < 300:

            #log
            insert_qbnb_log({'function':'_process_place_queue',
                            'action': 'Found fewer than 300 results, preparing to process/save listings', 
                            'session_id':str(m['session_id']['StringValue']),
                            'place_name': str(m['name']['StringValue']),
                            'place_id':str(m['place_id']['StringValue']),
                            'elapsed_time': '0',
                            'insert_date': str(datetime.datetime.now())})

            output = "less than 300"

        else:

            #log
            insert_qbnb_log({'function':'_process_place_queue',
                            'action': 'Unknown value for results[results_json][metadata][listings_count]', 
                            'elapsed_time': '0',
                            'insert_date': str(datetime.datetime.now())})

            output = "Unknown value for results[results_json][metadata][listings_count]"


    # if this is a listing
    #   tbd
    elif message.message_attributes['type']['StringValue'] == 'listing':

        #log
        insert_qbnb_log({'function':'_process_place_queue',
                        'action': 'message[type] == listing, preparing to process a listing',  
                        'elapsed_time': '0',
                        'insert_date': str(datetime.datetime.now())})

        output = 'Listing'

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
    log = []
    table = vars.dynamodb.Table('QbnbLog')
    r = table.scan(Limit=num_items, FilterExpression=Attr('insert_date').gt(str(results_after)))
    
    return r

#break a place up into 4 new quadrants, save each to the queue
def save_four_new_quadrants(session_id, place):
    nw = place_quadrant(place, 'nw')
    ne = place_quadrant(place, 'ne')
    sw = place_quadrant(place, 'sw')
    se = place_quadrant(place, 'se')

    send_sqs_place_message(session_id, nw)
    send_sqs_place_message(session_id, ne)
    send_sqs_place_message(session_id, sw)
    send_sqs_place_message(session_id, se)

# create a new place which is a quadrant of a previous place
def place_quadrant(place, quadrant):
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

def get_sqs_place_queue():
    return vars.sqs.Queue(url=vars.sqs_place)

def send_sqs_place_message(session_id, place):
    time_start = time.time()

    sqs = get_sqs_place_queue()

    message_body = "place session %s, %s" % (session_id, place.name)
    message_attributes = {
        'type': {
            'StringValue': 'place',
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
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    #log
    time_end = time.time()
    insert_qbnb_log({'session_id':str(session_id), 
                    'function':'send_sqs_place_message', 
                    'action': 'Sent a new sqs message to Place queue',
                    'place_name': str(place.name),
                    'place_id':str(place.place_id), 
                    'elapsed_time': str(time_end - time_start),
                    'insert_date':str(datetime.datetime.now())})

    return True

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


def insert_qbnb_log(item):
    table = vars.dynamodb.Table("QbnbLog")
    table.put_item(Item=item)


#######
# RUN #
#######

if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0")

