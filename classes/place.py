#########
# Place #
#########

import datetime
import vars
import utils

import pdb

class Place:
    def __init__(self, place_id = '', name = '', lat = '', lng = '', 
                ne_lat = '', ne_lng = '', sw_lat = '', sw_lng = '',
                count_listings = 0):
        self.place_id = place_id
        self.name = name
        self.lat = lat
        self.lng = lng
        self.ne_lat = ne_lat
        self.ne_lng = ne_lng
        self.sw_lat = sw_lat
        self.sw_lng = sw_lng
        self.count_listings = count_listings

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
        sql = "select *, " \
                "(select count(*) as listing_count from listing where s_google_place_id like %s) " \
              "from place where s_google_place_id = %s"
        params = (str(place_id + '%'), place_id)
        result = utils.pg_sql(sql, params)
        if len(result) > 0:
            self.place_id = result[0]['s_google_place_id']
            self.name = result[0]['s_name']
            self.lat = result[0]['s_lat']
            self.lng = result[0]['s_lng']
            self.ne_lat = result[0]['s_ne_lat']
            self.ne_lng = result[0]['s_ne_lng']
            self.sw_lat = result[0]['s_sw_lat']
            self.sw_lng = result[0]['s_sw_lng']
            self.listing_count = result[0]['listing_count']
            self.set_img_url()
            self.monthly_property_tax_per_100k = result[0]['monthly_property_tax_per_100k'] or 68
            self.monthly_mortgage_insurance_per_100k = result[0]['monthly_mortgage_insurance_per_100k'] or 29
            self.monthly_short_term_insurance_per_100k = result[0]['monthly_short_term_insurance_per_100k'] or 58
            self.monthly_utilities_per_sq_ft = result[0]['monthly_utilities_per_sq_ft'] or .22
            self.monthly_internet_fee = result[0]['monthly_internet_fee'] or 50
            self.monthly_management_fee = result[0]['monthly_management_fee'] or 20


    def set_parent_place(self, parent_place):
        self.parent = Place(parent_place.place_id, parent_place.name, parent_place.lat, parent_place.lng,
                            parent_place.ne_lat, parent_place.ne_lng, parent_place.sw_lat, parent_place.sw_lng)
        # reset the img url, b/c it will show two squares now
        self.set_img_url()
