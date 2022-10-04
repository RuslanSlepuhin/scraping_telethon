import asyncio
import configparser
from telethon import TelegramClient
from telethon.tl.types import PeerChannel, InputPeerChannel

config = configparser.ConfigParser()
config.read("config.ini")
api_id = int(config['TelegramRuslan']['api_id'])
api_hash = config['TelegramRuslan']['api_hash']
username = config['TelegramRuslan']['username']
client = TelegramClient(username, api_id, api_hash)
client.start()

link_channel = 'https://t.me/it_jobs_middle_and_senior'

async def get_ch():
    id_channel = await client.get_entity(link_channel)
    print(id_channel.id)


with client:
    client.loop.run_until_complete(get_ch())