import utils
from classes.place import Place
import datetime

#############################
# DATABASE HELPER FUNCTIONS #
#############################

def get_place(place_id):
    utils.log(None, 'get_place', 'get one place_id: %s' % place_id)

    p = Place()
    p.get_place_from_id(place_id)
    return p

# get all places from db, return an array of class Place
def get_places():
    utils.log(None, 'get_places', 'get all places from db')

    places = []

    results = utils.pg_sql("select *, " \
                        "(select count(*) from listing where listing.s_google_place_id = place.s_google_place_id) as count_listings " \
                        "from place")

    for place in results:
        p = Place(place["s_google_place_id"], place["s_name"], place["s_lat"], place["s_lng"], place["s_ne_lat"], place["s_ne_lng"], place["s_sw_lat"], place["s_sw_lng"], place["count_listings"])
        places.append(p)
    
    return places

# get a listing from the database
def get_listing(listing_id):

    sql = "select * from listing where i_listing_id = %s"
    params = (listing_id,)
    response = utils.pg_sql(sql, params)

    if len(response) == 0:
        utils.log(None, 'get_listing', 'Listing does not exist, listing_id = %s' % listing_id)
    else:
        utils.log(None, 'get_listing', 'Found listing, listing_id = %s' % listing_id)

    return response

# get all places from db, return an array of class Place
def get_listings(place_id):
    utils.log(None, 'get_listings', 'get all listings in db for place_id: %s' % place_id)

    listings = []
    place_id
    sql = "WITH t1 as ( " \
        "    select l.s_google_place_id, l.i_listing_id, l.s_listing_name, l.s_lat, l.s_lng, l.d_star_rating, l.d_rate, l.i_reviews_count, l.i_person_capacity, " \
        "        l.i_beds, l.i_bedrooms, l.d_bathrooms, l.s_picture_url, " \
        "        count(*) as count_nights_total " \
        "    from calendar c " \
        "        join listing l on c.i_listing_id = l.i_listing_id " \
        "    where l.s_google_place_id = %s " \
        "        and l.s_room_type = 'Entire home/apt' " \
        "        and l.s_room_type = 'Entire home/apt' " \
        "        and l.d_star_rating >= 4.0 " \
        "        and l.d_rate < 1000 " \
        "    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 " \
        "    order by i_listing_id " \
        "), t2 as (  " \
        "    select t1.i_listing_id, count(c.*) as count_nights_booked, sum(c.i_price) as total_bookings,  " \
        "        CAST(avg(c.i_price) as DECIMAL(6, 2)) as avg_nightly_price " \
        "    from t1 " \
        "        join calendar c on t1.i_listing_id = c.i_listing_id " \
        "    where t1.s_google_place_id = %s " \
        "        and c.b_available = FALSE " \
        "    group by 1 " \
        "    order by 1 " \
        "), t3 as ( " \
        "    select t2.i_listing_id, RANK() OVER (ORDER BY t2.count_nights_booked) as count_nights_rank, " \
        "        RANK() OVER (ORDER BY t2.total_bookings) as total_bookings_rank " \
        "    from t2 " \
        ") " \
        "select t1.i_listing_id, t1.s_lat, t1.s_lng, t1.s_listing_name, t1.d_star_rating, " \
        "    t1.i_reviews_count, t1.d_rate, t1.i_person_capacity, " \
        "    t1.i_beds, t1.i_bedrooms, t1.d_bathrooms, t1.s_picture_url, " \
        "    t1.count_nights_total, t2.count_nights_booked, t2.total_bookings,  " \
        "    CAST(t3.count_nights_rank / (select MAX(t3.count_nights_rank) * 1.0 from t3) * 100 AS DECIMAL(9, 0)) as count_nights_rank,  " \
        "    CAST(t3.total_bookings_rank / (select MAX(t3.total_bookings_rank) * 1.0 from t3) * 100 AS DECIMAL(9, 0)) as total_bookings_rank, " \
        "    CAST((t2.total_bookings / t1.count_nights_total) * 30 AS INT) as avg_monthly_bookings " \
        "from t1 " \
        "    join t2 on t1.i_listing_id = t2.i_listing_id " \
        "    join t3 on t2.i_listing_id = t3.i_listing_id " \
        "order by t2.total_bookings desc"
    params = (place_id, place_id)

    print sql % params

    results = utils.pg_sql(sql, params)

    for listing in results:
        listings.append({
                "listing_id": listing['i_listing_id'],
                "lat": listing['s_lat'],
                "lng": listing['s_lng'],
                "name": listing['s_listing_name'],
                "star_rating": str(listing['d_star_rating']),
                "count_reviews": str(listing['i_reviews_count']),
                "rate": str(listing['d_rate']),
                "person_capacity": str(listing['i_person_capacity']),
                "beds": str(listing['i_beds']),
                "bedrooms": str(listing['i_bedrooms']),
                "bathrooms": str(listing['d_bathrooms']),
                "picture_url": listing['s_picture_url'],
                "count_nights_total": str(listing['count_nights_total']),
                "count_nights_booked": str(listing['count_nights_booked']),
                "total_bookings": str(listing['total_bookings']),
                "count_nights_rank": str(listing['count_nights_rank']),
                "total_bookings_rank": str(listing['total_bookings_rank']),
                "avg_monthly_bookings": str(listing['avg_monthly_bookings'])
            });
    
    return listings


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

#if this function is called, we will insert a row into the database to log the session
def get_session_id(place_id, place_name):
    utils.log(None, 'get_session_id', 'get fresh session_id')
    session_id = utils.get_random_string(5)
    insert_session(session_id, place_id, place_name)
    return session_id


#get all existing sessions in the db
def get_sessions():
    utils.log(None, 'get_sessions', 'getting all sessions')


    response = utils.pg_sql("select s.s_session_id, s.s_google_place_id, p.s_name, s.dt_insert " \
                            "from session s " \
                                "join place p on s.s_google_place_id = p.s_google_place_id")
    return response


def insert_place(place):
    utils.log(None, 'insert_place', 'Inserting place_id: %s, name %s' % (place.place_id, place.name))

    sql = "insert into place (s_google_place_id, s_name, s_lat, s_lng, s_ne_lat, s_ne_lng, s_sw_lat, s_sw_lng, dt_insert) " \
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    params = (place.place_id, place.name, place.lat, place.lng, place.ne_lat, place.ne_lng, place.sw_lat, place.sw_lng, place.insert_date)
    utils.pg_sql(sql, params)

    return True

def insert_session(session_id, place_id, place_name):
    utils.log(session_id, 'insert_session', 'Insert row into Session table')

    sql = "insert into session (s_session_id, s_google_place_id, dt_insert) values (%s, %s, %s)"
    params = (session_id, place_id, str(datetime.datetime.now()))

    utils.pg_sql(sql, params)


def upsert_listing(place_id, session_id, listing):
    
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
                            "i_person_capacity, b_is_business_travel_ready, s_lat, s_lng, b_is_new_listing, " \
                            "b_can_instant_book, s_picture_url, s_localized_city, i_picture_count, " \
                            "i_host_id, s_host_name, i_beds, i_bedrooms, dt_insert) " \
                            " = " \
                            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

    params = (listing_id, place_id, session_id, listing_name, 
                star_rating, room_type, rate, reviews_count, person_capacity,
                is_business_travel_ready, lat, lng, is_new_listing, 
                can_instant_book, picture_url, localized_city, picture_count,
                host_id, host_name, beds, bedrooms, str(datetime.datetime.now()),
                session_id, listing_name, star_rating, room_type, rate, reviews_count, 
                person_capacity, is_business_travel_ready, lat, lng, is_new_listing, 
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
    s_description = listing['listing']['description'][:9999],
    b_is_location_exact = listing['listing']['is_location_exact']

    sql = "update listing " \
            "set (d_bathrooms, s_bed_type, b_has_availability, i_min_nights, s_neighborhood, " \
                "s_property_type, s_zipcode, s_calendar_updated_at, s_check_in_time, " \
                "s_check_out_time, i_cleaning_fee, s_description, b_is_location_exact) " \
            " = " \
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
            "where i_listing_id = %s"

    params = (d_bathrooms, s_bed_type, b_has_availability, i_min_nights, s_neighborhood,
                s_property_type, s_zipcode, s_calendar_updated_at, s_check_in_time,
                s_check_out_time, i_cleaning_fee, s_description, b_is_location_exact, listing_id)

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

            utils.pg_sql(sql, params)

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


# get the average bookings per month by number of bedrooms for a place
def get_avg_bookings_by_bedrooms(place_id):
    utils.log(None, 'get_avg_bookings_by_bedrooms', 'Getting average bookings by bedroom for place_id %s' % place_id)

    sql = "WITH t1 as ( " \
            "select i_bedrooms, count(distinct l.i_listing_id) as count_homes, count(*) as count_nights_total, sum(i_price) as price_total " \
            "from calendar c " \
                "join listing l on c.i_listing_id = l.i_listing_id " \
            "where l.s_google_place_id = %s " \
                "and l.s_room_type = 'Entire home/apt' " \
                "and l.d_star_rating > 3 " \
            "group by i_bedrooms " \
        "), t2 as ( " \
            "select i_bedrooms, count(distinct l.i_listing_id) as count_homes, count(*) as count_nights_total, sum(i_price) as price_total " \
            "from calendar c " \
               "join listing l on c.i_listing_id = l.i_listing_id " \
            "where l.s_google_place_id = %s " \
                "and c.b_available = False " \
                "and l.s_room_type = 'Entire home/apt' " \
                "and l.d_star_rating > 3 " \
            "group by i_bedrooms " \
            "order by i_bedrooms " \
        ") " \
        "select t1.i_bedrooms, t1.count_homes, (t2.price_total / t1.count_nights_total) * 30 as avg_bookings " \
        "from t1 " \
            "join t2 on t1.i_bedrooms = t2.i_bedrooms " \
        "where t1.count_homes > 5"
    params = (place_id, place_id)
    results = utils.pg_sql(sql, params)
    return results

