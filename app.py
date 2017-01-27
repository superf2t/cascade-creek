from flask import Flask, render_template, request
import vars
import utils
import utils_db

from pages.place import place
from pages.queue import queue

##########
# CONFIG #
##########

app = Flask(__name__)
app.register_blueprint(place, url_prefix='/place')
app.register_blueprint(queue, url_prefix='/queue')

#########
# PAGES #
#########

@app.route("/")
def hello_world_page():
    utils.log(None, 'hello_world_page', 'page load')
    places = utils_db.get_places()
    return render_template("home.html", places=places, count=len(places), google_api_key_js_map=vars.google_api_key_js_map)


@app.route('/log')
def show_log_page(minutes=30):
    utils.log(None, 'show_log_page', None)

    if request.args.get('minutes') == 'recent':
        minutes = 9999
        logs = get_log_most_recent()

    else:
        if request.args.get('minutes') != None:
            minutes = int(request.args.get('minutes'))

        logs = get_log(time_delta=minutes)

    return render_template("show_log.html", logs=logs, minutes=int(minutes))

##################
# Non-page URL"s #
##################

#none

####################
# HELPER FUNCTIONS #
####################

#none

#######
# RUN #
#######

if __name__ == "__main__":
    app.run(threaded=True, host="0.0.0.0")

