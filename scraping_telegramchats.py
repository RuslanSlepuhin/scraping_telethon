import configparser
import time
from asyncio import Task
from datetime import datetime, timedelta

import flask

from links import list_links
from telethon.sync import TelegramClient
from telethon import functions, types, events, client
from asgiref.sync import sync_to_async, async_to_sync
import psycopg2

# In the same way, you can also leave such channel
from telethon.tl.functions.channels import LeaveChannelRequest
from telethon import connection, utils

# класс для работы с сообщениями
from telethon.tl.functions.messages import GetHistoryRequest, ImportChatInviteRequest

# app = flask.Flask(__name__)

config = configparser.ConfigParser()
config.read("config.ini")

api_id = int(config['Telegram']['api_id'])
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']
phone = '+375296449690'

client = TelegramClient(username, api_id, api_hash)
client.start()

quant = 1

class ListenChat:

    @client.on(events.NewMessage(chats=(list_links)))
    async def normal_handler(event):

        print('have a new message')

        info = event.message.to_dict()
        title = info['message'].partition(f'\n')[0]
        body = info['message'].replace(title, '').replace(f'\n\n', f'\n')
        date = info['date'].strftime('%d.%m.%y %H:%M')

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
        db.push_to_bd(results_dict)
        print(results_dict)


class WriteToDbMessages():
    async def dump_all_messages(self, channel, limit_msg):
        """Записывает json-файл с информацией о всех сообщениях канала/чата"""
        offset_msg = 0  # номер записи, с которой начинается считывание
        # limit_msg = 1   # максимальное число записей, передаваемых за один раз

        all_messages = []  # список всех сообщений
        total_messages = 0
        total_count_limit = limit_msg  # поменяйте это значение, если вам нужны не все сообщения

        message = []
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
                # здесь можно организовать формирование словаря

            offset_msg = messages[len(messages) - 1].id
            total_messages = len(all_messages)
            if total_count_limit != 0 and total_messages >= total_count_limit:
                break

        channel_name = f'@{channel.username} | {channel.title}'

        # results_list = []
        for i in all_messages:
            title = i['message'].partition(f'\n')[0]
            body = i['message'].replace(title, '').replace(f'\n\n', f'\n')
            date = (i['date'] + timedelta(hours=3)).strftime('%d.%m.%y %H:%M')
            results_dict = {
                'chat_name': channel_name,
                'title': title,
                'body': body,
                'time_of_public': date
            }
            db = DataBaseOperations()
            db.push_to_bd(results_dict)
        time.sleep(30)


    async def main_start(self, list_links, limit_msg):
        results_list = []

        for url in list_links:
            bool_index = True
            channel = None

            try:
                channel = await client.get_entity(url)
            except Exception as e:
                if e.args[0] == 'Cannot get entity from a channel (or group) that you are not part of. Join the group and retry':
                    private_url = url.split('/')[-1]
                    try:
                        await client(ImportChatInviteRequest(private_url))
                        channel = await client.get_entity(url)
                    except Exception as e:
                        print(f'Error: Цикл прошел с ошибкой в месте, где нужна подписка: {e}')

                else:
                    print(f'ValueError for url {url}: {e}')
                    bool_index = False
                    # self.mute_wrong_url(url)

            if bool_index:
                await self.dump_all_messages(channel, limit_msg)

    def start(self, limit_msg):
        with client:
            client.loop.run_until_complete(self.main_start(list_links, limit_msg))


    # def mute_wrong_url(self, url):
    #     with open('links_test.py', 'r') as f:
    #         old_data = f.read()
    #
    #     new_data = old_data.replace(url, 'https://t.me/ruslanchannel3')
    #
    #     with open('links-test.py', 'w') as f:
    #         f.write(new_data)

# ---------------------DB operations ----------------------
class DataBaseOperations:

    def push_to_bd(self, results_dict):
        global quant
        con = None

        try:
            con = psycopg2.connect(
                database="tele",
                user="ruslan",
                password="12345",
                host="127.0.0.1",
                port="5432"
            )
        except:
            print('No connect with db')

        cur = con.cursor()

        with con:
            cur.execute("""CREATE TABLE IF NOT EXISTS telegram (
                id SERIAL PRIMARY KEY,
                chat_name VARCHAR(150),
                title VARCHAR(700),
                body VARCHAR (6000),
                time_of_public VARCHAR(20),
                created_at TIMESTAMP
                );"""
                        )
            con.commit()

        print(f'\n****************************************\nINPUT IN DB FUNC = \n', results_dict)

        chat_name = results_dict['chat_name']
        print('len chat_name = ', len(chat_name))
        title = results_dict['title']
        print('len title = ', len(title))
        body = str(results_dict['body']).replace(f'\'', '"')
        print('len body = ', len(body))
        time_of_public = results_dict['time_of_public']
        print('len time_of_public = ', len(time_of_public))
        created_at = datetime.now()

        new_post = f"""INSERT INTO telegram (chat_name, title, body, time_of_public, created_at) 
            VALUES ('{chat_name}', '{title}', '{body}', '{time_of_public}', '{created_at}');"""

        with con:
            try:
                query = f"""SELECT * FROM telegram WHERE title='{title}' AND body='{body}'"""
                cur.execute(query)
                r = cur.fetchall()

                if not r:
                    cur.execute(new_post)
                    con.commit()
                    print(quant, f'= Added to DB = ', results_dict)
                    quant += 1
                else:
                    print(quant, f'This message exists already = ', results_dict)
            except Exception as e:
                print('Dont push in db, error = ', e)


    def get_all_from_db(self):
        con = None

        try:
            con = psycopg2.connect(
                database="tg",
                user="ruslan",
                password="12345",
                host="127.0.0.1",
                port="5432"
            )

        except:
            print('No connect with db')

        cur = con.cursor()

        query = """SELECT * FROM telegram"""
        with con:
            cur.execute(query)
            r = cur.fetchall()
            for i in r:
                print(i)


def main():
    get_messages = WriteToDbMessages()
    get_messages.start(limit_msg=1)

    print("I'm listening chats...")
    client.start()
    ListenChat()
    client.run_until_disconnected()

main()

# db = DataBaseOperations()
# db.get_all_from_db()
