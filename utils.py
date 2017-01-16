################
# Random utils #
################
import vars
import uuid
import datetime
import psycopg2
import psycopg2.extras


def pg_sql(sql, params = ()):
    conn = psycopg2.connect(vars.pg_conn_string)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cursor.execute(sql, params)
    conn.commit()

    data = []
    try:
        data = cursor.fetchall()
    except psycopg2.ProgrammingError:
        pass
    finally:
        cursor.close()
        conn.close()

    return data
    

# get a random string to use for the airbnb ss_id
#   (not sure if this is necessary, but it looks important and is easy)
def get_random_string(string_length):
    random = str(uuid.uuid4()) # Convert UUID format to a Python string.
    random = random.upper() # Make all characters uppercase.
    random = random.replace("-","") # Remove the UUID "-".
    return random[0:string_length].lower() # Return the random string.

def log(session_id, function, action, url = None, elapsed_time = 0):

    sql = "insert into qbnb_log (s_session_id, d_elapsed_time, s_function, s_action, s_url, dt_insert) " \
            "values (%s, %s, %s, %s, %s, %s)"
    params = (session_id, elapsed_time, function, action, url, str(datetime.datetime.now()))

    pg_sql(sql, params)
