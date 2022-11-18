
import configparser
import time
from datetime import datetime, timedelta
from tg_channels.links import list_links
from settings.database_settings import database, user, password, host, port
from telethon.sync import TelegramClient
from telethon import events, client
import psycopg2
from telethon.tl.functions.messages import GetHistoryRequest, ImportChatInviteRequest
import re

config = configparser.ConfigParser()
config.read("config.ini")

# забираем значения из config.ini
# private_channel = config['My_channels']['private_channel']
backend_channel = config['My_channels']['backend_channel']  #+
frontend_channel =config['My_channels']['frontend_channel']  #+
devops_channel = config['My_channels']['devops_channel']  #+
developer_channel = config['My_channels']['developer_channel']  #+
pm_channel = config['My_channels']['pm_channel']  #+
# designer_channel = config['My_channels']['designer_channel']
# analyst_channel = config['My_channels']['analyst_channel']
# qa_channel = config['My_channels']['qa_channel']
# hr_channel = config['My_channels']['hr_channel']
# others_channel = config['My_channels']['others_channel']

api_id = 11495582
api_hash = '07bab8cc1546be63992d349fb5fc590c'
username = 'ruslanslepuhin'
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
        # if dict_bool['or_exists']:
        #     send_message = f'{chat_name}\n\n' + info['message']
        #     await client.send_message(entity=private_channel, message=send_message)
        print(results_dict)

class WriteToDbMessages():
    async def dump_all_messages(self, channel, limit_msg):
        offset_msg = 0  # номер записи, с которой начинается считывание
        # limit_msg = 1   # максимальное число записей, передаваемых за один раз

        all_messages = []  # список всех сообщений
        total_messages = 0
        total_count_limit = limit_msg  # значение 0 = все сообщения

        channel_to_send = None

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
            date = (i['date'] + timedelta(hours=3))#.strftime('%d.%m.%y %H:%M')
            results_dict = {
                'chat_name': channel_name,
                'title': title,
                'body': body,
                'time_of_public': date
            }
            db = DataBaseOperations()
            dict_bool = db.push_to_bd(results_dict)  # из push_to_db возвращается bool or_exists
            if dict_bool['or_exists']:
                send_message = i['message']

                match dict_bool['profession']:
                    case 'Backend':
                        channel_to_send = backend_channel
                    # case 'Frontend':
                    #     channel_to_send = frontend_channel
                    # case 'DevOps':
                    #     channel_to_send = devops_channel
                    # case 'Developer':
                    #      channel_to_send = developer_channel
                    # case 'PM':
                    #     channel_to_send = pm_channel
                    # case 'Designer':
                    #     channel_to_send = designer_channel
                    # case 'Analyst':
                    #     channel_to_send = analyst_channel
                    # case 'QA':
                    #     channel_to_send = qa_channel
                    # case 'HR':
                    #     channel_to_send = hr_channel
                    # case 'Others':
                    #     channel_to_send = others_channels
                    case 'Backend|Frontend':
                        await client.send_message(entity=backend_channel, message=send_message)
                        time.sleep(10)
                        await client.send_message(entity=frontend_channel, message=send_message)
                        time.sleep(10)

                if channel_to_send:
                    await client.send_message(entity=channel_to_send, message=send_message)
                    print(f'\npushed to chat {channel_to_send}')

        if not dict_bool['time_index']:
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
                        await client(ImportChatInviteRequest(private_url))
                        channel = await client.get_entity(url)
                    except Exception as e:
                        print(f'Error: Цикл прошел с ошибкой в месте, где нужна подписка: {e}')

                else:
                    print(f'ValueError for url {url}: {e}')
                    bool_index = False

            if bool_index:
                await self.dump_all_messages(channel, limit_msg)


    def start(self, limit_msg):
        with client:
            client.loop.run_until_complete(self.main_start(list_links, limit_msg))

# ---------------------DB operations ----------------------
class DataBaseOperations:

    def push_to_bd(self, results_dict):
        or_exists = False
        time_index = True
        global quant
        con = None

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

        cur = con.cursor()

        with con:
            cur.execute("""CREATE TABLE IF NOT EXISTS telegram_channels_professions (
                id SERIAL PRIMARY KEY,
                chat_name VARCHAR(150),
                title VARCHAR(1000),
                body VARCHAR (6000),
                profession VARCHAR (30),
                time_of_public TIMESTAMP,
                created_at TIMESTAMP
                );"""
                        )
            con.commit()

        print(f'\n****************************************\n'
              f'INPUT IN DB FUNC = \n', results_dict)

        chat_name = results_dict['chat_name']
        title = results_dict['title'].replace(f'\'', '"')
        body = str(results_dict['body']).replace(f'\'', '"')
        profession = self.sort_by_profession(title, body)
        time_of_public = results_dict['time_of_public']
        created_at = datetime.now()

        new_post = f"""INSERT INTO telegram_channels_professions (chat_name, title, body, profession, time_of_public, created_at) 
            VALUES ('{chat_name}', '{title}', '{body}', '{profession}', '{time_of_public}', '{created_at}');"""

        with con:
            try:
                query = f"""SELECT * FROM telegram_channels_professions WHERE title='{title}' AND body='{body}'"""
                cur.execute(query)
                r = cur.fetchall()

                if not r:
                    cur.execute(new_post)
                    con.commit()
                    print(quant, f'= Added to DB = {profession}', results_dict)
                    or_exists = True
                    quant += 1
                    time_index = True
                    time.sleep(15)
                else:
                    print(quant, f'This message exists already = ', results_dict)
                    time_index = False
            except Exception as e:
                print('Dont push in db, error = ', e)

            return {'or_exists': or_exists, 'time_index': time_index, 'profession': profession}


    def sort_by_profession(self, title, body):
        pattern_backend_frontend = r'backend.*frontend'
        pattern_backend = r'back\s*end|б[е,э]к\s*[е,э]нд'
        pattern_frontend = r'front\s*end|фронт\s*[е,э]нд'
        pattern_devops = r'dev\s*ops'
        pattern_developer = r'developer|разработчик|site\s*reliability'
        pattern_backend_languages = r'python|scala|java|linux|haskell|php|server|сервер|ios|android'
        pattern_frontend_languages = r'javascript|html|css|react\s*js'
        pattern_pm = r'product\s*manager|прод[а,у]кт\s*м[е,а]н[е,а]джер|project\s*manager'
        pattern_designer = r'дизайн|design|ui|ux'
        pattern_analyst = r'analyst|аналитик'
        pattern_qa = r'qa|тестировщ'
        pattern_hr = r'hr|рекрутер'

        line = f'{title.lower()} {body.lower()}'

        if re.search(pattern_backend_frontend, line):
            return 'Backend|Frontend'

        if re.search(pattern_backend, line):
            return 'Backend'

        elif re.search(pattern_frontend, line):
            return 'Frontend'

        elif re.search(pattern_devops, line):
            return 'DevOps'

        elif re.search(pattern_backend_languages, line):
            return 'Backend'

        elif re.search(pattern_frontend_languages, line):
            return 'Frontend'

        elif re.search(pattern_developer, line):
            return 'Developer'

        elif re.search(pattern_pm, line):
            return 'PM'

        elif re.search(pattern_designer, line):
            return 'Designer'

        elif re.search(pattern_analyst, line):
            return 'Analyst'

        elif re.search(pattern_qa, line):
            return 'QA'

        elif re.search(pattern_hr, line):
            return 'HR'
        else:
            return 'Others'


    def get_all_from_db(self):
        con = None

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

        cur = con.cursor()

        query = """SELECT * FROM telegramchannels"""
        with con:
            cur.execute(query)
            r = cur.fetchall()
            for i in r:
                print(i)


def main():
    get_messages = WriteToDbMessages()
    get_messages.start(limit_msg=10)

    print("Listening chats...")
    client.start()
    ListenChat()
    client.run_until_disconnected()

main()


# db = DataBaseOperations()
# db.get_all_from_db()