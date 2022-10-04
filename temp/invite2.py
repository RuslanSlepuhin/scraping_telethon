import asyncio
import configparser
import os
import time

import pandas
from telethon import events
from telethon.sync import TelegramClient
from telethon.tl import functions
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.messages import AddChatUserRequest
from telethon.tl.types import User, UserStatusRecently, InputPeerUser, InputPeerChannel

config = configparser.ConfigParser()
# config.read("config.ini")
# api_id = int(config['Ruslan']['api_id'])
# api_hash = config['Ruslan']['api_hash']
# username = config['Ruslan']['username']
phone = '+375296449690'  #телеграм Руслан
# client = TelegramClient(username, api_id, api_hash)
# client.connect()

class Invite:

    async def invite(self, all_participants, client):

        channel = await client.get_entity('https://t.me/openchannelruslan')
        channel_to_send = InputPeerChannel(channel.id, channel.access_hash)

        for user in all_participants:
            print(user.username)
            print(user.id)
            print(user.access_hash)
            print(user.first_name)
            print(user.last_name)
            try:
                try:
                    user_to_add = InputPeerUser(user.id, user.access_hash)
                    # user_to_add = await client.get_input_entity(user)
                except:
                    user_to_add = InputPeerUser(user.id, user.access_hash)
                    # user_to_add = await client.get_input_entity(user.username)
                await client(functions.channels.InviteToChannelRequest(channel_to_send, [user_to_add,]))
                print(f'success\n')
                time.sleep(6)
            except Exception as e:
                print(f'{e}\n')
                time.sleep(7)

    # def start(self, channel_to_send, all_participants):
    #     with client:
    #         client.loop.run_until_complete(self.invite(channel_to_send, all_participants))

# asyncio.run(Invite().start(-1001665966449, [137336064, 758905227, 97129286, 556128576]))

# Invite().start(-1001665966449, [137336064, 758905227, 97129286, 556128576])

# loop = asyncio.new_event_loop()
# asyncio.set_event_loop(loop)
# loop.run_until_complete(Invite().invite(-1001665966449, [137336064, 758905227, 97129286, 556128576]))