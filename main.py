import os

import flask
import scraping_telegramchats

app = flask.Flask(__name__)

@app.route('/')
@app.route('/home')
def home():
    return 'connected'

@app.route('/listen')
def start():
    return 'Added'


if __name__ == "__main__":
    app.run(debug=True)
