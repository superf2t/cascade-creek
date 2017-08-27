import utils
import utils_sqs
from classes.place import Place
import datetime

import pdb

#############################
# DATABASE HELPER FUNCTIONS #
#############################

def get_place(place_id):
    utils.log('get_place', 'get one place_id: %s' % place_id)

    p = Place()
    p.get_place_from_id(place_id)
    return p

# get all places from db, return an array of class Place
def get_places():
    utils.log('get_places', 'get all places from db')

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
        utils.log('get_listing', 'Listing does not exist, listing_id = %s' % listing_id)
    else:
        utils.log('get_listing', 'Found listing, listing_id = %s' % listing_id)

    return response

# get all places from db, return an array of class Place
def get_listings(place_id, ne_lat, ne_lng, sw_lat, sw_lng):
    utils.log('get_listings', 'get all listings in db for place_id: %s' % place_id)

    # if lat/lng was not passed in, just make them very big values that will return all results
    if ne_lat == 0 and ne_lng == 0:
        ne_lat = 999
        ne_lng = 999
        sw_lat = -999
        sw_lng = -999

    listings = []
    place_id
    sql = """
        WITH t1 as ( 
            select l.s_google_place_id, l.i_listing_id, l.s_listing_name, l.s_lat, l.s_lng, l.d_star_rating, l.d_rate, l.i_reviews_count, l.i_person_capacity, 
                l.i_beds, l.i_bedrooms, l.d_bathrooms, l.s_picture_url, 
                count(*) as count_nights_total 
            from calendar c 
                join listing l on c.i_listing_id = l.i_listing_id 
            where l.s_google_place_id = %s 
                and l.s_room_type = 'Entire home/apt' 
                and l.s_room_type = 'Entire home/apt' 
                and l.d_star_rating >= 4.0 
                and l.d_rate < 1000 
                and c.dt_booking_date < now() 
                and CAST(s_lat AS NUMERIC) BETWEEN %s AND %s 
                and CAST(s_lng AS NUMERIC) BETWEEN %s AND %s 
            group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 
            order by i_listing_id 
        ), t2 as (  
            select t1.i_listing_id, count(c.*) as count_nights_booked, sum(c.i_price) as total_bookings,  
                CAST(avg(c.i_price) as DECIMAL(6, 2)) as avg_nightly_price 
            from t1 
                join calendar c on t1.i_listing_id = c.i_listing_id 
            where c.b_available = FALSE 
                and c.dt_booking_date < now() 
            group by 1 
            order by 1 
        ), t3 as ( 
            select t2.i_listing_id, 
               RANK() OVER (ORDER BY (t2.count_nights_booked / (t1.count_nights_total * 1.0))) as count_nights_rank, 
               RANK() OVER (ORDER BY (t2.total_bookings / t1.count_nights_total)) as total_bookings_rank 
            from t2 
               join t1 on t2.i_listing_id = t1.i_listing_id 
        ) 
        select t1.i_listing_id, t1.s_lat, t1.s_lng, t1.s_listing_name, t1.d_star_rating, 
            t1.i_reviews_count, t1.d_rate, t1.i_person_capacity, 
            t1.i_beds, t1.i_bedrooms, t1.d_bathrooms, t1.s_picture_url, 
            t1.count_nights_total, t2.count_nights_booked, t2.total_bookings,  
            CAST(t3.count_nights_rank / (select MAX(t3.count_nights_rank) * 1.0 from t3) * 100 AS DECIMAL(9, 0)) as count_nights_rank,  
            CAST(t3.total_bookings_rank / (select MAX(t3.total_bookings_rank) * 1.0 from t3) * 100 AS DECIMAL(9, 0)) as total_bookings_rank, 
            CAST((t2.count_nights_booked / (t1.count_nights_total * 1.0)) * 30 AS INT) as avg_num_nights,
            CAST((t2.total_bookings / t1.count_nights_total) * 30 AS INT) as avg_monthly_bookings 
        from t1 
            join t2 on t1.i_listing_id = t2.i_listing_id 
            join t3 on t2.i_listing_id = t3.i_listing_id 
        order by t2.total_bookings desc
    """
    params = (place_id, sw_lat, ne_lat, sw_lng, ne_lng)

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
                "avg_num_nights": str(listing['avg_num_nights']),
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
    utils.log('get_log_most_recent', 'get recent log items')

    response = utils.pg_sql("select * from (select * from qbnb_log order by dt_insert desc limit 500) t1 order by dt_insert")

    return response

#get all place history
def get_places_history():
    utils.log('get_places_history', 'getting all place history')


    response = utils.pg_sql("""
                                WITH c AS (
                                    select 
                                        l.s_google_place_id,
                                        max(c.dt_booking_date) as dt_booking_date 
                                    from calendar c
                                        join listing l on c.i_listing_id = l.i_listing_id 
                                    group by 1
                                ),
                                l AS (
                                    select s_google_place_id, count(distinct i_listing_id) as count_i_listing_id from listing group by 1
                                )
                                select
                                    p.s_google_place_id as place_id, 
                                    p.s_name, 
                                    p.dt_insert,
                                    c.dt_booking_date as max_booking_date,
                                    l.count_i_listing_id as count_listings
                                from place p
                                    join l on p.s_google_place_id = l.s_google_place_id
                                    join c on l.s_google_place_id = c.s_google_place_id
                            """)
    return response


def insert_place(place):
    utils.log('insert_place', 'Inserting place_id: %s, name %s' % (place.place_id, place.name))

    sql = "insert into place (s_google_place_id, s_name, s_lat, s_lng, s_ne_lat, s_ne_lng, s_sw_lat, s_sw_lng, dt_insert) " \
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    params = (place.place_id, place.name, place.lat, place.lng, place.ne_lat, place.ne_lng, place.sw_lat, place.sw_lng, place.insert_date)
    utils.pg_sql(sql, params)

    return True

def save_place_monthly_costs(place_id, monthly_property_tax_per_100k, 
                                monthly_mortgage_insurance_per_100k, 
                                monthly_short_term_insurance_per_100k, 
                                monthly_utilities_per_sq_ft, 
                                monthly_internet_fee,
                                monthly_management_fee):
    utils.log('save_place_monthly_costs', 'Updating place table with new monthly costs for place_id: %s' % place_id)

    sql = "update place " \
          "set monthly_property_tax_per_100k = %s, " \
            "monthly_mortgage_insurance_per_100k = %s, " \
            "monthly_short_term_insurance_per_100k = %s, " \
            "monthly_utilities_per_sq_ft = %s, " \
            "monthly_internet_fee = %s, " \
            "monthly_management_fee = %s " \
          "where s_google_place_id = %s"
    params = (monthly_property_tax_per_100k, monthly_mortgage_insurance_per_100k, monthly_short_term_insurance_per_100k, monthly_utilities_per_sq_ft, monthly_internet_fee, monthly_management_fee, place_id)
    print sql % params
    utils.pg_sql(sql, params)
    return "Successfully updated monthly costs"


def upsert_listing(place_id, listing):
    
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
    sql = "insert into listing (i_listing_id, s_google_place_id, s_listing_name, " \
                                "d_star_rating, s_room_type, d_rate, i_reviews_count, i_person_capacity, " \
                                "b_is_business_travel_ready, s_lat, s_lng, b_is_new_listing, " \
                                "b_can_instant_book, s_picture_url, s_localized_city, i_picture_count, " \
                                "i_host_id, s_host_name, i_beds, i_bedrooms, dt_insert) " \
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
            "on conflict (i_listing_id, s_google_place_id) " \
            "do update set (s_listing_name, d_star_rating, s_room_type, d_rate, i_reviews_count, " \
                            "i_person_capacity, b_is_business_travel_ready, s_lat, s_lng, b_is_new_listing, " \
                            "b_can_instant_book, s_picture_url, s_localized_city, i_picture_count, " \
                            "i_host_id, s_host_name, i_beds, i_bedrooms, dt_insert) " \
                            " = " \
                            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "

    params = (listing_id, place_id, listing_name, 
                star_rating, room_type, rate, reviews_count, person_capacity,
                is_business_travel_ready, lat, lng, is_new_listing, 
                can_instant_book, picture_url, localized_city, picture_count,
                host_id, host_name, beds, bedrooms, str(datetime.datetime.now()),
                listing_name, star_rating, room_type, rate, reviews_count, 
                person_capacity, is_business_travel_ready, lat, lng, is_new_listing, 
                can_instant_book, picture_url, localized_city, picture_count,
                host_id, host_name, beds, bedrooms, str(datetime.datetime.now()))

    utils.pg_sql(sql, params)
    
# save/update listing detail
def save_listing_detail(listing):
    listing_id = listing['listing']['id']

    utils.log('save_listing_detail', 'Saving listing detail for listing_id %s' % listing_id)

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
def save_calendar_detail(listing_id, calendar_months):
    utils.log('save_calendar_detail', 'Saving calendar detail for listing_id %s' % listing_id)

    i = 0
    for month in calendar_months:
        for day in month['days']:

            booking_date = day['date']
            available = day['available']
            price = day['price']['local_price']

            # call the postgres function created to check to see if something already exists before inserting
            sql = "INSERT INTO calendar(i_listing_id, dt_booking_date, b_available, i_price, dt_insert) " \
                "VALUES (%s, %s, %s, %s, now()) " \
                "ON CONFLICT (i_listing_id, dt_booking_date) " \
                "DO UPDATE SET (b_available, i_price, dt_insert) = (%s, %s, now());"
            params = (listing_id, booking_date, available, price, available, price)

            utils.pg_sql(sql, params)

            i += 1

    utils.log('save_calendar_detail', 'Inserted %s calendar items into db' % i)

    return True

# queue a calendar request for every listing for a place
def queue_calendar_sqs_for_place(place_id):
    # get all listings with this place id
    
    listings = utils.pg_sql("select * from listing where s_google_place_id = %s", (place_id, ))
    if len(listings) == 0:
        utils.log('queue_calendar_sqs_for_place', 'No listings returned for place_id = %s' % place_id)
    else:

        # loop through listings and queue a calendar sqs
        # batch 10 sqs inserts at a time, when loop breaks, send leftovers if they exist
        i = 0
        entries = []
        for listing in listings:

            entries.append(utils_sqs.create_sqs_listing_calendar_entry(listing['i_listing_id']))
            i += 1

            # if we've created 10 entries, then send 'em'
            if i % 10 == 0:
                utils_sqs.insert_sqs_batch(entries)
                entries = []

        #send what is left over
        if len(entries) > 0:
            utils_sqs.insert_sqs_batch(entries)
            
        utils.log('queue_calendar_sqs_for_place', 'Queued %s sqs calendar items' % i)

    return i


# get the average bookings per month by number of bedrooms for a place
def get_avg_bookings_by_bedrooms(place_id, ne_lat, ne_lng, sw_lat, sw_lng):
    utils.log('get_avg_bookings_by_bedrooms', 'Getting average bookings by bedroom for place_id %s' % place_id)

    sql = """
        WITH t1 as (
            select i_bedrooms, l.i_listing_id, count(*) as count_nights_total, sum(i_price) as price_total
            from calendar c
                join listing l on c.i_listing_id = l.i_listing_id
            where l.s_google_place_id = %s
                and l.s_room_type = 'Entire home/apt'
                and l.d_star_rating > 3
                and c.dt_booking_date < now()
                and CAST(l.s_lat AS NUMERIC) BETWEEN %s AND %s
                and CAST(l.s_lng AS NUMERIC) BETWEEN %s AND %s
            group by 1, 2
        ), t2 as (
            select t1.i_bedrooms, t1.i_listing_id, t1.count_nights_total, t1.price_total, 
                count(*) as count_nights_booked,
                sum(c.i_price) as price_nights_booked,
                (sum(c.i_price) / t1.count_nights_total) * 30 as avg_monthly_bookings
            from t1 
                join calendar c on t1.i_listing_id = c.i_listing_id
            where c.b_available = False
                and c.dt_booking_date < now()
            group by 1, 2, 3, 4
        )
        select i_bedrooms, count(*) as count_homes, 
            CAST(sum(avg_monthly_bookings) / count(*) AS INT) as avg_bookings,
            CAST((sum(count_nights_booked) / sum(count_nights_total)) * 30 AS INT) as avg_num_nights,
            CAST(sum(price_nights_booked) / sum(count_nights_booked) AS INT) as avg_price_per_night,
            percentile_disc(0.8) WITHIN GROUP (ORDER BY avg_monthly_bookings) as eighty_pct
        from t2
        group by 1
        UNION ALL
        select 99, count(*) as count_homes,
            CAST(sum(avg_monthly_bookings) / count(*) AS INT) as avg_bookings,
            CAST((sum(count_nights_booked) / sum(count_nights_total)) * 30 AS INT) as avg_num_nights,
            CAST(sum(price_nights_booked) / sum(count_nights_booked) AS INT) as avg_price_per_night,
            percentile_disc(0.8) WITHIN GROUP (ORDER BY avg_monthly_bookings) as eighty_pct
        from t2
        order by 1
    """
    params = (place_id, sw_lat, ne_lat, sw_lng, ne_lng)
    results = utils.pg_sql(sql, params)

    return results

def get_place_for_auto_queue():
    # get places that:
    #   * have not been queued up in over a month
    #   * are in reverse descending order of size
    #   * are modulo in rank equal to today's day of month
    # this logic will assure that places will be equally spaced and places with
    #   big listing counts will only potentially be paired with smaller ones
    sql = """
            WITH l AS (
                select s_google_place_id, count(*) as listing_count 
                from listing 
                group by 1
            ), c AS (
                select 
                    l.s_google_place_id,
                    max(c.dt_booking_date) as max_dt_booking_date 
                from calendar c
                    join listing l on c.i_listing_id = l.i_listing_id 
                group by 1
            ), p AS (
                select ROW_NUMBER() OVER (order by l.listing_count desc) as row_number, p.s_google_place_id, p.s_name, 
                    l.listing_count, c.max_dt_booking_date
                from place p
                    join l on p.s_google_place_id = l.s_google_place_id
                    join c on p.s_google_place_id = c.s_google_place_id
                where b_active = True
            )
            select * 
            from p 
            where row_number / CAST(date_part('day', current_date) as FLOAT) = 1.0
                and max_dt_booking_date < current_date
        """

    results = utils.pg_sql(sql)
    return results

