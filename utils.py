################
# Random utils #
################
import uuid

# get a random string to use for the airbnb ss_id
#   (not sure if this is necessary, but it looks important and is easy)
def get_random_string(string_length):
    random = str(uuid.uuid4()) # Convert UUID format to a Python string.
    random = random.upper() # Make all characters uppercase.
    random = random.replace("-","") # Remove the UUID "-".
    return random[0:string_length].lower() # Return the random string.
