import configparser
import os

# api_id = os.getenv('api_id')
# api_hash = os.getenv('api_hash')
# username = os.getenv('username')
# token = os.getenv('token')
#
config_keys = configparser.ConfigParser()
config_keys.read("./settings/config_keys.ini")
api_id = config_keys['Telegram']['api_id']
api_hash = config_keys['Telegram']['api_hash']
username = config_keys['Telegram']['username']
token = config_keys['Token']['token']
