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

def get_server():
    server = socket.gethostname()
    if server == 'precise64':
        return 'http://0.0.0.0:5000'
    else:
        return 'http://redata.giggy.com'

def get_queue():
    # get queue page
    server = get_server()
    response = urllib2.urlopen(server + '/queue/_process_place_queue')
    data = json.loads(response.read())

    print 'Message: %s\nMessages in Queue: %s\n-----' % (data['message'], data['message_count'])

    if data['message_count'] != '0':
        time.sleep(3)
        get_queue()

schedule.clear()
schedule.every(10).seconds.do(get_queue)

get_queue()

while True:
    print '...tick...'
    schedule.run_pending()
    time.sleep(21600) # execute every 6 hours