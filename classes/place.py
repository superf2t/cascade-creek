#########
# Place #
#########

import datetime
import vars
import utils

import pdb

class Place:
    def __init__(self, place_id = '', name = '', lat = '', lng = '', 
                ne_lat = '', ne_lng = '', sw_lat = '', sw_lng = ''):
        self.place_id = place_id
        self.name = name
        self.lat = lat
        self.lng = lng
        self.ne_lat = ne_lat
        self.ne_lng = ne_lng
        self.sw_lat = sw_lat
        self.sw_lng = sw_lng

        if ne_lat and ne_lng and sw_lat and sw_lng:
            self.set_img_url()
            self.set_search_url()

        self.insert_date = datetime.datetime.now()

    def set_img_url(self):
        img_path = "path=color:red|weight:3|%s,%s|%s,%s|%s,%s|%s,%s|%s,%s" % (self.ne_lat, self.ne_lng, self.sw_lat, self.ne_lng, self.sw_lat, self.sw_lng, self.ne_lat, self.sw_lng, self.ne_lat, self.ne_lng)
        
        # if this place has a parent, include a square for that
        try:
            img_path += "&path=color:blue|weight:1|%s,%s|%s,%s|%s,%s|%s,%s|%s,%s" % (self.parent.ne_lat, self.parent.ne_lng, self.parent.sw_lat, self.parent.ne_lng, self.parent.sw_lat, self.parent.sw_lng, self.parent.ne_lat, self.parent.sw_lng, self.parent.ne_lat, self.parent.ne_lng)
        except:
            #nothing
            pass

        self.img_url = "https://maps.googleapis.com/maps/api/staticmap?%s&size=500x500&key=%s" % (img_path, vars.google_api_key_static_map)

    def set_search_url(self):
        #return "http://localhost:5000/static/airbnb-results.json"
        self.search_url = "https://www.airbnb.com/search/search_results?page=1&source=map&ne_lat=%s&ne_lng=%s&sw_lat=%s&sw_lng=%s&search_by_map=true&guests=2&ss_id=%s" % (self.ne_lat, self.ne_lng, self.sw_lat, self.sw_lng, utils.get_random_string(8))


    def get_place_from_id(self, place_id):
        place = utils.pg_sql("select * from place where s_google_place_id = %s", (place_id,))
        p = place[0]
        self.place_id = p['s_google_place_id']
        self.name = p['s_name']
        self.lat = p['s_lat']
        self.lng = p['s_lng']
        self.ne_lat = p['s_ne_lat']
        self.ne_lng = p['s_ne_lng']
        self.sw_lat = p['s_sw_lat']
        self.sw_lng = p['s_sw_lng']
        self.set_img_url()

    def set_parent_place(self, parent_place):
        self.parent = Place(parent_place.place_id, parent_place.name, parent_place.lat, parent_place.lng,
                            parent_place.ne_lat, parent_place.ne_lng, parent_place.sw_lat, parent_place.sw_lng)
        # reset the img url, b/c it will show two squares now
        self.set_img_url()
