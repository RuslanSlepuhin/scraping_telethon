import random
import pandas as pd
import configparser
import time
from datetime import datetime, timedelta
import psycopg2
from scraping_db import DataBaseOperations
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch, Message, PeerChannel
from links import list_links
from telethon.sync import TelegramClient
from telethon import events, client
from telethon.tl.functions.messages import GetHistoryRequest, ImportChatInviteRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser, InputUser, PeerUser, InputChannel, InputPeerEmpty

config = configparser.ConfigParser()
config.read("config.ini")

#--------------------------- забираем значения из config.ini-------------------------------
alexandr_channel = config['My_channels']['alexandr_channel']
bot = config['My_channels']['bot']

api_id = int(config['TelegramRuslan']['api_id'])
api_hash = config['TelegramRuslan']['api_hash']
username = config['TelegramRuslan']['username']
phone = '+375296449690'

client = TelegramClient(username, api_id, api_hash)
client.start()

quant = 1  # счетчик вывода количества запушенных в базу сообщений (для контроля в консоли)

database = config['DB3']['database']
user = config['DB3']['user']
password = config['DB3']['password']
host = config['DB3']['host']
port = config['DB3']['port']
try:
    con = psycopg2.connect(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )
except:
    print('No connect with db')


class ListenChat:

    @client.on(events.NewMessage(chats=(list_links)))
    async def normal_handler(event):

        print('I,m listening chats ....')

        info = event.message.to_dict()
        title = info['message'].partition(f'\n')[0]
        body = info['message'].replace(title, '').replace(f'\n\n', f'\n')
        date = (info['date'] + timedelta(hours=3))

        if event.chat.username:
            chat_name = f'@{event.chat.username} | {event.chat.title}'
        else:
            chat_name = event.chat.title

        results_dict = {
            'chat_name': chat_name,
            'title': title,
            'body': body,
            'time_of_public': date
        }
        db = DataBaseOperations()
        dict_bool = db.push_to_bd(results_dict)  # из push_to_db возвращается bool or_exists

        if dict_bool['not_exists']:
            send_message = f'{chat_name}\n\n' + info['message']
            await client.send_message(entity=bot, message=send_message)
        print(results_dict)

class WriteToDbMessages():

    async def dump_all_participants(self, channel):
        """Записывает json-файл с информацией о всех участниках канала/чата"""
        offset_user = 0  # номер участника, с которого начинается считывание
        limit_user = 100  # максимальное число записей, передаваемых за один раз

        all_participants = []  # список всех участников канала
        filter_user = ChannelParticipantsSearch('')

        print(f'Start scraping participants from {channel}\n\n')

        try:
            while True:
                participants = await client(GetParticipantsRequest(channel,
                                                               filter_user, offset_user, limit_user, hash=0))
                if not participants.users:
                    break
                all_participants.extend(participants.users)
                offset_user += len(participants.users)

            all_users_details = []  # список словарей с интересующими параметрами участников канала
            channel_name = f'@{channel.username} | {channel.title}'
            for participant in all_participants:

                print(f'\n{participant.id}\n{participant.access_hash}')

                first_name = str(participant.first_name).replace('\'', '')
                last_name = str(participant.last_name).replace('\'', '')

                all_users_details.append({'id': participant.id,
                                          'access_hash': participant.access_hash,
                                          'first_name': first_name,
                                          'last_name': last_name,
                                          'user': participant.username,
                                          'phone': participant.phone,
                                          'is_bot': participant.bot})

            print('Numbers of followers = ', len(all_users_details))

            #--------------запись в файл------------
            file_name = channel.username

            for i in all_users_details:
                print(i)
                print(i['id'], i['access_hash'])
            j1 = [str(i['id']) for i in all_users_details]
            j2 = [str(i['access_hash']) for i in all_users_details]
            j3 = [str(i['user']) for i in all_users_details]
            j4 = [str(i['first_name']) for i in all_users_details]
            j5 = [str(i['last_name']) for i in all_users_details]


            df = pd.DataFrame(
                {
                'from channel': channel_name,
                'id_participant': j1,
                'access_hash': j2,
                'username': j3,
                'first_name': j4,
                'last_name': j5
                 }
            )

            df.to_excel(f'./participants_from_{file_name}.xlsx', sheet_name='Sheet1')

            #------------- конец записи в файл ------------

            # await Invite().invite(all_participants, client) #####################
            # DataBaseOperations().push_to_bd_participants(participant, all_users_details, channel_name, channel.username) ################################################

            print(f'\nPause 40-60 sec...')
            time.sleep(random.randrange(40, 60))
            print('...Continue')

        except Exception as e:
            print(f'Error для канала {channel}: {e}')

    async def dump_all_messages(self, channel, limit_msg):

        block = False

        offset_msg = 0  # номер записи, с которой начинается считывание
        # limit_msg = 1   # максимальное число записей, передаваемых за один раз

        all_messages = []  # список всех сообщений
        total_messages = 0
        total_count_limit = limit_msg  # значение 0 = все сообщения

        while True:
            history = await client(GetHistoryRequest(
                peer=channel,
                offset_id=offset_msg,
                offset_date=None, add_offset=0,
                limit=limit_msg, max_id=0, min_id=0,
                hash=0))
            if not history.messages:
                break
            messages = history.messages

            channel_name = f'@{channel.username} | {channel.title}'

            for message in messages:

                if not message.message:  # если сообщение пустое, например "Александр теперь в группе"
                    pass
                else:
                    # all_messages.append(message.to_dict())
                    # await client.send_message(bot, message) ############
                    # time.sleep(random.randrange(5, 15)) ###########

                    title = message.message.partition(f'\n')[0] ##############3
                    body = message.message.replace(title, '').replace(f'\n\n', f'\n')##############
                    date = (message.date + timedelta(hours=3))###########3
                    results_dict = {
                        'chat_name': channel_name,
                        'title': title,
                        'body': body,
                        'time_of_public': date
                    }##############
                    db = DataBaseOperations(con)#############3

                    response_dict = db.push_to_bd(results_dict)  ##############3
                    channels = response_dict.keys()##############

                    if 'block' in channels:#################
                        block = response_dict['block']#########3

                    channel_list = []#############
                    if not block:
                        message_text = ''################
                        length = 0###############
                        for chann in channels:##########3
                            if not response_dict[chann]:###########3
                                message_text = message_text + f'{chann}/'###########3
                                channel_list.append(chann)###############
                                length += 1###############3

                        if message_text:##########3
                            message.message = f"{length}/{message_text}{message.message}"
                            # await client.send_message(entity=bot, message=f"{length}/{message}{i['message']}")

                            await client.send_message(entity=bot, message=message)###########

                            # channel_from = PeerChannel(channel)
                            # await client.forward_messages(bot, message, channel_from)  ###########

                            for i in channel_list:############
                                print(f"pushed to channel = {i}")#############

                    else:############
                        pass#############

                time.sleep(random.randrange(5, 7))##############


            offset_msg = messages[len(messages) - 1].id
            total_messages = len(all_messages)
            if total_count_limit != 0 and total_messages >= total_count_limit:
                break

        # channel_name = f'@{channel.username} | {channel.title}'

        # for i in all_messages:
            # title = i['message'].partition(f'\n')[0]
            # body = i['message'].replace(title, '').replace(f'\n\n', f'\n')
            # date = (i['date'] + timedelta(hours=3))
            # results_dict = {
            #     'chat_name': channel_name,
            #     'title': title,
            #     'body': body,
            #     'time_of_public': date
            # }
            # db = DataBaseOperations(con)
            #
            # response_dict = db.push_to_bd(results_dict)  # из push_to_db возвращается bool or_exists
            # channels = response_dict.keys()
            #
            # if 'block' in channels:
            #     block = response_dict['block']
            #
            # channel_list = []
            # if not block:
            #     message = ''
            #     length = 0
            #     for channel in channels:
            #         if not response_dict[channel]:
            #             message = message + f'{channel}/'
            #             channel_list.append(channel)
            #             length += 1
            #
            #     if message:
            #         # await client.send_message(entity=bot, message=f"{length}/{message}{i['message']}")
            #         await client.send_message(entity=bot, message=i)
            #
            #         for i in channel_list:
            #             print(f"pushed to channel = {i}")
            #
            # else:
            #     pass
            #
            # time.sleep(random.randrange(25, 40))

            time.sleep(random.randrange(25, 40))


    async def main_start(self, list_links, limit_msg, action):

        for url in list_links:
            bool_index = True
            channel = None

            try:
                channel = await client.get_entity(url)
            except Exception as e:
                if e.args[0] == 'Cannot get entity from a channel (or group) that you are not part of. Join the group and retry':
                    private_url = url.split('/')[-1]
                    try:
                        await client(ImportChatInviteRequest(private_url))  # если канал закрытый, подписаться на него
                        channel = await client.get_entity(url)  # и забрать из него историю сообщений
                    except Exception as e:
                        print(f'Error: Цикл прошел с ошибкой в месте, где нужна подписка: {e}')

                else:
                    print(f'ValueError for url {url}: {e}')
                    bool_index = False

            if bool_index:
                match action:
                    case 'get_message':
                        await self.dump_all_messages(channel, limit_msg)
                    case 'get_participants':
                        await self.dump_all_participants(channel)

    def start(self, limit_msg, action):
        with client:
            client.loop.run_until_complete(self.main_start(list_links, limit_msg, action))


def main():
    get_messages = WriteToDbMessages()
    get_messages.start(limit_msg=20, action='get_message')  #get_participants

    # print("Listening chats...")
    # client.start()
    # ListenChat()
    # client.run_until_disconnected()

main()
