from flask import Flask, render_template, 
                jsonify, request, redirect
import requests
import json
import datetime
import boto3
import vars


##########
# CONFIG #
##########

app = Flask(__name__)

###########
# CLASSES #
###########

class Place:
    def __init__(self, place_id, name, lat, lng, ne_lat, ne_lng, sw_lat, sw_lng):
        self.place_id = place_id
        self.name = name
        self.lat = lat
        self.lng = lng
        self.ne_lat = ne_lat
        self.ne_lng = ne_lng
        self.sw_lat = sw_lat
        self.sw_lng = sw_lng
        self.insert_date = datetime.datetime.now()

    def img_url(self):
        img_bounds = "%s,%s|%s,%s|%s,%s|%s,%s|%s,%s" % (self.ne_lat, self.ne_lng, self.sw_lat, self.ne_lng, self.sw_lat, self.sw_lng, self.ne_lat, self.sw_lng, self.ne_lat, self.ne_lng)
        return "https://maps.googleapis.com/maps/api/staticmap?center=%s,%s&path=color:red|weight:3|%s&size=500x500&key=%s" % (self.lat, self.lng, img_bounds, vars.google_api_key_static_map)



#########
# PAGES #
#########

@app.route('/')
def hello_world():
    return render_template('home.html')

@app.route('/add_place')
def add_place():
    return render_template('add_place.html')


#######
# API #
#######

# call google geocode service and get the name and coords for a location
@app.route('/_get_place/<place>')
def get_place(place):
    try:
        place = get_google_place(place)

        # see if this place already exists in the db
        table = vars.dynamodb.Table('Place')
        response = table.get_item(
                Key={
                    'place_id': place.place_id
                }
            )
        # if an Item was returned, this place is already in the db
        try:
            item = response['Item']
            return jsonify("place already in database")

        # if a KeyError is thrown, this place is not in the database
        except KeyError:
            return jsonify(place.img_url())

    # google returned no results
    except:
        return jsonify("google returned no results")

# insert a place into database
@app.route('/_insert_place/', methods=['POST'])
def insert_place():

    str_place = request.form['hidden_place']
    
    place = get_google_place(str_place)
    table = vars.dynamodb.Table('Place')
    table.put_item(
            Item={
                'place_id': place.place_id,
                'name': place.name,
                'lat': str(place.lat),
                'lng': str(place.lng),
                'ne_lat': str(place.ne_lat),
                'ne_lng': str(place.ne_lng),
                'sw_lat': str(place.sw_lat),
                'sw_lng': str(place.sw_lng),
                'insert_date': str(place.insert_date)
            }
        )

    return redirect("/")
    

###########
# HELPERS #
###########

def get_google_place(place):
    # call google api and bring it on back
    google_result = requests.get('https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=%s' % (place, vars.google_api_key_geocode))
    """
    r.status_code               200
    r.headers['content-type']   application/json; charset=UTF-8
    r.encoding                  UTF-8
    r.content                   json returned from api
    r.json()                    json returned from api
    """

    r = google_result.json()['results'][0]

    place = Place(r['place_id'], r['formatted_address'], 
                    r['geometry']['location']['lat'], r['geometry']['location']['lng'], 
                    r['geometry']['bounds']['northeast']['lat'], r['geometry']['bounds']['northeast']['lng'],
                    r['geometry']['bounds']['southwest']['lat'], r['geometry']['bounds']['southwest']['lng'])
    
    return place

#######
# RUN #
#######

if __name__ == '__main__':
    app.run(threaded=True, host='0.0.0.0')

