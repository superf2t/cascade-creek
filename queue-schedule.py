"""
This file will call the process queue page once every 6 hours.
If there are no messages in the queue, it will wait another 6 hours.
If there are messages in the queue, it will wait 3 seconds and call it again.

To run this scheduled task:

    ssh redatavpn
    cd redata/cascade-creek
    screen
    [enter]
    python queue-schedule.py
    [ctrl-a] [d]
    
To reconnect:

    screen -r

"""

import sys
import time
import datetime
import schedule
import urllib2
import json
import socket
import traceback
# local 
import utils_sqs
import utils_db

def get_server():
    server = socket.gethostname()
    if server == 'gbox':
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

def process_queue(loop_count = 0):
    try:
        # get queue page
        server = get_server()
        response = urllib2.urlopen(server + '/queue/_process_place_queue')
        data = json.loads(response.read())

        print 'Message: %s\nMessages in Queue: %s\n-----' % (data['message'], data['message_count'])

        # this function kicks off every 10 minutes, so only call this recursive function
        #   (a) if there are still messages in the queue, and
        #   (b) less than the times it could execute in 10 minutes
        if data['message_count'] != '0' and loop_count < 190:
            time.sleep(3)
            process_queue(loop_count + 1)
    except:
        #e = sys.exc_info()[0]
        #print '--------- ERROR IN process_queue --------\n%s\n------------------' % e
        print '--------- ERROR IN process_queue --------\n%s\n------------------' % traceback.format_exc()


###########################

# clear existing schedules
#schedule.clear()
# check if a new place needs to be queued every 6 hours
#schedule.every(6).hours.do(queue_place)
# kick off queue processing every 10 minutes
#schedule.every(10).seconds.do(process_queue)

while True:
    print '...tick... %s' % datetime.datetime.now()
    try:
        #schedule.run_pending()

        queue_place()
        process_queue()
    except:
        e = sys.exc_info()[0]
        print '--------- ERROR IN SCHEDULE RUNNER --------\n%s\n------------------' % e

        process_queue()

    time.sleep(10) # check every 5 minutes whether any jobs need to run

