from flask import Flask
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

server = Flask(__name__)
api_id = int(config['TelegramRuslan']['api_id'])
api_hash = config['TelegramRuslan']['api_hash']
username = config['TelegramRuslan']['username']
phone = '+375296449690'

# client = TelegramClient('username2', api_id, api_hash)
# client.connect()

@server.route('/')
async def hello():

    return 'Hello guys', 200

@server.route('/', methods=['POST'])
def get_message():
    return '!', 200


@server.route('/scrape')
async def start_parsing():

    # await main()

    t=0
    return f'Hello admin {t}', 200


if __name__ == '__main__':
    # server.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
    server.run()

