import asyncio

import requests
from flask import Flask
import configparser
from scraping_telegramchats2 import main
import os

# gunicorn --bind=0.0.0.0 --timeout 600 application:app

config = configparser.ConfigParser()
config.read("./settings/config.ini")
server = Flask(__name__)

@server.route('/')
async def hello():
    return 'Hello guys', 200

@server.route('/', methods=['POST'])
def get_message():
    return '!', 200

@server.route('/scrape')
async def start_parsing():

    await main()
    print('sleep 10 minutes')
    await asyncio.sleep(60*10)
    print('sleeping is finish')
    return f'Hello admin', 200


if __name__ == '__main__':
    # server.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
    server.run()

