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
    return 'hello world'  # craping_telegramchats.main()

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))





# from flask import Flask, request
# import os
# import telethon
# import travel_visa_bot_polling
#
#
# server = Flask(__name__)
# travel_visa_bot_polling.main()
#
#
# @server.route('/' + bot.token, methods=['POST'])
# def get_message():
#     json_string = request.get_data().decode('utf-8')
#     update = telebot.types.Update.de_json(json_string)
#     bot.process_new_updates([update])
#     return '!', 200
#
#
# @server.route('/')
# def webhook():
#     bot.remove_webhook()
#     bot.set_webhook(url='https://travel-visa-bot.herokuapp.com/' + bot.token)
#     return 'Hello from bot', 200
#
#
# @server.route('/admin')
# def helloadmin():
#     return 'Hello admin', 200
#
#
# @server.route('/rwh')
# def remove_webhook():
#     bot.remove_webhook()
#     return 'webhook delete - ok', 200
#
#
# if __name__ == '__main__':
#     server.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))