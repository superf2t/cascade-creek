import vars
import utils
import time
import datetime
import json

#################
# SQS FUNCTIONS #
#################

def insert_sqs_batch(entries):
    sqs = get_sqs_place_queue()
    utils.log('insert_sqs_batch', 'Inserting batch sqs')
    sqs.send_messages(Entries=entries)
    
    return True

def insert_sqs_listing_overview_message(place, page):
    time_start = time.time()

    sqs = get_sqs_place_queue()

    message_body = "listing overview page %s, %s" % (page, place.name)
    message_attributes = build_sqs_place_attributes(place, 'listing overview', page)
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    #log
    elapsed_time = time.time() - time_start
    utils.log('insert_sqs_listing_overview_message', 'Sent an sqs for %s, listing overview page %s' % (place.name, page))

    return True


def get_sqs_place_queue():
    utils.log('get_sqs_place_queue', None)
    return vars.sqs.Queue(url=vars.sqs_place)


# insert sqs for listing detail pages
def insert_sqs_listing_detail_page(listing_id):

    sqs = get_sqs_place_queue()

    listing_url = 'https://www.airbnb.com/api/v2/listings/%s?key=%s&_format=v1_legacy_for_p3' % (listing_id, vars.airbnb_key)

    message_body = "listing id %s" % listing_id
    message_attributes = {
        'type': {
            'StringValue': 'listing detail',
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

    utils.log('insert_sqs_listing_detail_pages', 'Inserting listing detail sqs for listing_id %s' % listing_id, listing_url)
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    return True

# insert an sqs item to grab calendar information
def insert_sqs_listing_calendar(listing_id):

    entry = []
    entry.append(create_sqs_listing_calendar_entry(listing_id))
    utils.log('insert_sqs_listing_calendar', 'Inserting calendar sqs for listing_id %s' % listing_id)
    insert_sqs_batch(entry)

    return True

def create_sqs_listing_calendar_entry(listing_id):

    # get 2 months ago
    # ...used to be 3 months, but in some cases this causes airbnb to say "woah buddy, that's too far back, no soup for you"
    _month = (datetime.datetime.today() - datetime.timedelta(days=35)).month
    _year = (datetime.datetime.today() - datetime.timedelta(days=35)).year
    
    listing_url = 'https://www.airbnb.com/api/v2/calendar_months?key=%s&currency=USD&locale=en&listing_id=%s&month=%s&year=%s&count=3&_format=with_conditions' % (vars.airbnb_key, listing_id, _month, _year)

    entry = {
        "Id": str(listing_id),
        "MessageBody": "calendar, listing id %s" % listing_id,
        "MessageAttributes": {
                            "type": {
                                "StringValue": "calendar",
                                "DataType": "String"
                            },
                            "listing_id": {
                                "StringValue": str(listing_id),
                                "DataType": "String"
                            },
                            "url": {
                                "StringValue": listing_url,
                                "DataType": "String"
                            }
                        }
    }

    return entry

def insert_sqs_place_message(place):
    time_start = time.time()

    sqs = get_sqs_place_queue()

    message_body = "place %s" % place.name
    message_attributes = build_sqs_place_attributes(place, 'place')
    sqs.send_message(MessageBody=message_body, MessageAttributes=message_attributes)

    #log
    elapsed_time = time.time() - time_start
    utils.log('insert_sqs_place_message', 'Inserted sqs for %s' % place.name, None, elapsed_time)

    return True

def build_sqs_place_attributes(place, process_type, page=1):
    utils.log('build_sqs_place_attributes', 'place attributes page %s' % page)

    message_attributes = {
        'type': {
            'StringValue': str(process_type),
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
        utils.log('get_one_sqs_place_message', 'return type = %s' % messages[0].message_attributes['type']['StringValue'])
        return messages[0]
    else:
        utils.log('get_one_sqs_place_message', 'Nothing returned')
        return ''


def delete_sqs_place_message(message):

    m = message.message_attributes
    utils.log('delete_sqs_place_message', 'Preparing to delete an sqs message, %s' % message.body)

    message.delete()

def get_sqs_message_count():

    sqs = get_sqs_place_queue()
    return sqs.attributes['ApproximateNumberOfMessages']



