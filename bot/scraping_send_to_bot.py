import asyncio
import configparser
import datetime
import random
from telethon.sync import TelegramClient
from telethon import client


config = configparser.ConfigParser()
config.read("config.ini")


api_id = int(config['TelegramRuslan']['api_id'])
api_hash = config['TelegramRuslan']['api_hash']
username = config['TelegramRuslan']['username']
phone = '+375296449690'
bot = config['My_channels']['bot']


client = TelegramClient(username, api_id, api_hash)
client.start()

class PushToDB:

    def start_push_to_bot(self, length, prof, message):

        with client:
            client.loop.run_until_complete(self.push_to_bot(length, prof, message))

    async def push_to_bot(self, length, prof, message):

        await client.send_message(entity=bot, message=f"{length}/{prof}{message}")
        print(f"{datetime.datetime.now().strftime('%H:%M')} message to bot was sent: {prof}")
        print('message = ', message)
        print('wait 5-7 sec')
        print('--------------------------------------------')

        message_for_print = message.replace('\n', '')[0:40]
        # with open('d:\Python\scrapping_telethon\log.txt', 'a') as file:
        #     file.write(f"scrapper\nmessage = {message_for_print}\nlength = {length}\nchannels = {prof}\n\n")
        await asyncio.sleep(random.randrange(20, 25))
        pass