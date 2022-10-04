import asyncio
import configparser
import os
import random
import time
import traceback

import pandas
from telethon import events
from telethon.errors import PeerFloodError, UserPrivacyRestrictedError
from telethon.sync import TelegramClient
from telethon.tl import functions
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.messages import AddChatUserRequest
from telethon.tl.types import User, UserStatusRecently, InputPeerUser

config = configparser.ConfigParser()
config.read("config.ini")
api_id = int(config['Telegram']['api_id'])
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']
phone = '+375296449690'  #телеграм Руслан
client = TelegramClient(username, api_id, api_hash)
client.connect()

class Invite:

    async def invite(self, users):
        target_group_entity =-1001665966449
        n=0
        # mode = int(input(gr + "Input : " + re))
        for user in users:
            n += 1
            if 1 == 1:
                time.sleep(1)
                try:
                    # print("Adding {}".format(user['id']))
                    # if mode == 1:
                    if user['username'] == "":

                        user_to_add = client.get_input_entity(user['username'])
                    # elif mode == 2:
                    else:
                        user_to_add = InputPeerUser(user['id'], user['access_hash']) #####################
                    # else:
                    #     sys.exit(re + "[!] Invalid Mode Selected. Please Try Again.")
                    await client(InviteToChannelRequest(target_group_entity, [user_to_add,]))
                    print("[+] Waiting for 10-30 Seconds...")
                    time.sleep(random.randrange(10, 30))
                except PeerFloodError:
                    print(
                        "[!] Getting Flood Error from telegram. \n[!] Script is stopping now. \n[!] Please try again after some time.")
                except UserPrivacyRestrictedError:
                    print("[!] The user's privacy settings do not allow you to do this. Skipping.")
                except:
                    traceback.print_exc()
                    print("[!] Unexpected Error")
                    continue
