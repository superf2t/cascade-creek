"""
This file will call the process queue page once every 6 hours.
If there are no messages in the queue, it will wait another 6 hours.
If there are messages in the queue, it will wait 3 seconds and call it again.
"""

import time
import schedule
import urllib2
import json
import socket
# local 
import utils_sqs
import utils_db

def get_server():
    server = socket.gethostname()
    if server == 'precise64':
        return 'http://0.0.0.0:5000'
    else:
        return 'http://redata.giggy.com'

def queue_place():
    # call a page that will queue up a place if it needs to be queued
    server = get_server()
    response = urllib2.urlopen(server + '/queue/_auto_queue_place')
    data = json.loads(response.read())
    print 'queue_place()\nMessage: %s\n----' % data['message']

    return

def process_queue():
    # get queue page
    server = get_server()
    response = urllib2.urlopen(server + '/queue/_process_place_queue')
    data = json.loads(response.read())

    print 'Message: %s\nMessages in Queue: %s\n-----' % (data['message'], data['message_count'])

    if data['message_count'] != '0':
        time.sleep(3)
        process_queue()

###########################

# clear existing schedules
schedule.clear()
# check if a new place needs to be queued every 6 hours
schedule.every(6).hours.do(queue_place)
# kick off queue processing every 6 hours
schedule.every(6).hours.do(process_queue)

queue_place()
process_queue()

while True:
    #print '...tick...'
    schedule.run_pending()
    time.sleep(3600) # check every hour whether any jobs need to run

