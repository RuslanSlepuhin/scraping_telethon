
import configparser
import time
from datetime import datetime, timedelta
from scraping_db import DataBaseOperations

from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.functions.contacts import ResolveUsernameRequest
from telethon.tl.types import ChannelParticipantsSearch

from links import list_links

from telethon.sync import TelegramClient
from telethon import events, client
import psycopg2
from telethon.tl.functions.messages import GetHistoryRequest, ImportChatInviteRequest

config = configparser.ConfigParser()
config.read("config.ini")

#--------------------------- забираем значения из config.ini-------------------------------
# private_channel = config['My_channels']['private_channel']
# ad_channel = config['My_channels']['ad_channel']
# backend_channel = config['My_channels']['backend_channel']
# frontend_channel =config['My_channels']['frontend_channel']
# devops_channel = config['My_channels']['devops_channel']
# fullstack_channel = config['My_channels']['fullstack_channel']
# pm_channel = config['My_channels']['pm_channel']
# product_channel = config['My_channels']['product_channel']
# designer_channel = config['My_channels']['designer_channel']
# analyst_channel = config['My_channels']['analyst_channel']
# qa_channel = config['My_channels']['qa_channel']
# hr_channel = config['My_channels']['hr_channel']
alexandr_channel = config['My_channels']['alexandr_channel']
bot = config['My_channels']['bot']

api_id = int(config['Telegram']['api_id'])
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']
phone = '+375296449690'

client = TelegramClient(username, api_id, api_hash)
client.start()

quant = 1  # счетчик вывода количества запушенных в базу сообщений (для контроля в консоли)

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
        if dict_bool['or_exists']:
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

                first_name = str(participant.first_name).replace('\'', '')
                last_name = str(participant.last_name).replace('\'', '')

                all_users_details.append({"id": participant.id,
                                          "first_name": first_name,
                                          "last_name": last_name,
                                          "user": participant.username,
                                          "phone": participant.phone,
                                          "is_bot": participant.bot})

            print('Numbers of followers = ', len(all_users_details))
            DataBaseOperations().push_to_bd_participants(all_users_details, channel_name, channel.username)
            time.sleep(10)

        except Exception as e:
            print(e)

    async def dump_all_messages(self, channel, limit_msg):
        offset_msg = 0  # номер записи, с которой начинается считывание
        # limit_msg = 1   # максимальное число записей, передаваемых за один раз

        all_messages = []  # список всех сообщений
        total_messages = 0
        total_count_limit = limit_msg  # значение 0 = все сообщения

        dict_bool = {
            'or_exists': bool,
            'time_index': bool
        }

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
            for message in messages:
                if not message.message:  # если сообщение пустое, например "Александр теперь в группе"
                    break
                all_messages.append(message.to_dict())

            offset_msg = messages[len(messages) - 1].id
            total_messages = len(all_messages)
            if total_count_limit != 0 and total_messages >= total_count_limit:
                break

        channel_name = f'@{channel.username} | {channel.title}'

        for i in all_messages:
            title = i['message'].partition(f'\n')[0]
            body = i['message'].replace(title, '').replace(f'\n\n', f'\n')
            date = (i['date'] + timedelta(hours=3))
            results_dict = {
                'chat_name': channel_name,
                'title': title,
                'body': body,
                'time_of_public': date
            }
            db = DataBaseOperations()

            #----------------only for test without db--------------------

            # print(title)
            # print(body)
            # s = Professions()
            # profession = s.sort_by_profession2(title, body)
            # print('profession = ', profession)
            #
            # for pro in profession:
            #===========================================================
            dict_bool = db.push_to_bd(results_dict)  # из push_to_db возвращается bool or_exists

            if dict_bool['or_exists']:
                pro = dict_bool['profession']
                await client.send_message(entity=bot, message=f"{pro}/{i['message']}")
                print(f"\npushed to chat {pro}")
                time.sleep(15)
        # if not dict_bool['time_index']:
        time.sleep(10)


    async def main_start(self, list_links, limit_msg):

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
                await self.dump_all_messages(channel, limit_msg)
                # await self.dump_all_participants(channel)

    def start(self, limit_msg):
        with client:
            client.loop.run_until_complete(self.main_start(list_links, limit_msg))


def main():
    get_messages = WriteToDbMessages()
    get_messages.start(limit_msg=20)

    # print("Listening chats...")
    # client.start()
    # ListenChat()
    # client.run_until_disconnected()

main()
