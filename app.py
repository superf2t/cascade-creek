from flask import Flask, render_template, request, redirect
import vars
import utils
import utils_db
from classes.user import User
import flask_login

from pages.place import place
from pages.queue import queue

##########
# CONFIG #
##########

app = Flask(__name__)
app.secret_key = vars.app_secret_key
app.register_blueprint(place, url_prefix='/place')
app.register_blueprint(queue, url_prefix='/queue')

login_manager = flask_login.LoginManager()
login_manager.init_app(app)

#########
# PAGES #
#########

@app.route("/")
@flask_login.login_required
def hello_world_page():
    utils.log(None, 'hello_world_page', 'page load')
    places = utils_db.get_places()
    return render_template("home.html", places=places, count=len(places), google_api_key_js_map=vars.google_api_key_js_map)


@app.route("/login", methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template("login.html")

    un = request.form['un']
    try:
        if request.form['pw'] == vars.users[un]['pw']:
            user = User()
            user.id = un
            flask_login.login_user(user)
            return redirect('/')
    except:
        pass

    return render_template("login.html", error="No workie, try again")

@app.route('/logout')
def logout():
    flask_login.logout_user()
    return redirect("/login")

@app.route('/log')
@flask_login.login_required
def show_log_page(minutes=30):
    utils.log(None, 'show_log_page', None)

    if request.args.get('minutes') == 'recent':
        minutes = 9999
        logs = utils_db.get_log_most_recent()

    else:
        if request.args.get('minutes') != None:
            minutes = int(request.args.get('minutes'))
        else:
            minutes = 30

        logs = utils_db.get_log(time_delta=minutes)

    return render_template("show_log.html", logs=logs, minutes=int(minutes))

#########
# LOGIN #
#########

@login_manager.user_loader
def user_loader(un):
    if un not in vars.users:
        return

    user = User()
    user.id = un
    return user


@login_manager.request_loader
def request_loader(request):
    un = request.form.get('un')
    if un not in vars.users:
        return

    user = User()
    user.id = un

    # DO NOT ever store passwords in plaintext and always compare password
    # hashes using constant-time comparison!
    user.is_authenticated = request.form['pw'] == vars.users[un]['pw']

    return user

@login_manager.unauthorized_handler
def unauthorized_handler():
    return redirect("/login")


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

