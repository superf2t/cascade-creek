################
# Random utils #
################
import vars
import uuid
import datetime
import psycopg2
import psycopg2.extras


def pg_sql(sql):
    conn = psycopg2.connect(vars.pg_conn_string)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql)
    return cursor.fetchall()

# get a random string to use for the airbnb ss_id
#   (not sure if this is necessary, but it looks important and is easy)
def get_random_string(string_length):
    random = str(uuid.uuid4()) # Convert UUID format to a Python string.
    random = random.upper() # Make all characters uppercase.
    random = random.replace("-","") # Remove the UUID "-".
    return random[0:string_length].lower() # Return the random string.

def log(session_id, function, action, url = None, place_id = None, listing_id = None, elapsed_time = 0):
    item = {
        'session_id': str(session_id),
        'function': str(function),
        'action': str(action),
        'url': str(url),
        'place_id': str(place_id),
        'listing_id': str(listing_id),
        'elapsed_time': str(elapsed_time),
        'insert_date': str(datetime.datetime.now())
    }

    table = vars.dynamodb.Table("QbnbLog")
    table.put_item(Item=item)