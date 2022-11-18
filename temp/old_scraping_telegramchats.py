import configparser
import time
from datetime import datetime, timedelta

from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

from tg_channels.links import list_links
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
fullstack_channel = config['My_channels']['developer_channel']  #+developer_channel
pm_channel = config['My_channels']['pm_channel']  #+
designer_channel = config['My_channels']['designer_channel']
# analyst_channel = config['My_channels']['analyst_channel']
# qa_channel = config['My_channels']['qa_channel']
# hr_channel = config['My_channels']['hr_channel']
# others_channel = config['My_channels']['others_channel']
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
        # if dict_bool['or_exists']:
        #     send_message = f'{chat_name}\n\n' + info['message']
        #     await client.send_message(entity=private_channel, message=send_message)
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
                                          "is_bot": participant.bot_aiogram})

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
                channel_to_send = bot ####################################
                # match dict_bool['profession']:
                    # case 'backend':
                    #     channel_to_send = backend_channel
                    # case 'frontend':
                    #     channel_to_send = frontend_channel
                    # case 'devops':
                    #     channel_to_send = devops_channel
                    # # case 'developer':
                    # #      channel_to_send = developer_channel
                    # case 'pm':
                    #     channel_to_send = pm_channel
                    # case 'designer':
                    #     channel_to_send = designer_channel
                    # case 'Analyst':
                    #     channel_to_send = analyst_channel
                    # case 'QA':
                    #     channel_to_send = qa_channel
                    # case 'HR':
                    #     channel_to_send = hr_channel
                    # case 'Others':
                    #     channel_to_send = others_channels
                    # case 'fullstack':
                    #     channel_to_send = fullstack_channel

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

# ---------------------DB operations ----------------------
class DataBaseOperations:

    def connect_db(self):
        global database, user, password, host, port
        con = None
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        try:
            con = psycopg2.connect(
                database=self.database,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
        except:
            print('No connect with db')
        return con

    #-------------participants-------------------------
    def push_to_bd_participants(self, all_user_dictionary, channel_name, channel_username):
        con = self.connect_db()
        cur = con.cursor()

        with con:
            cur.execute("""CREATE TABLE IF NOT EXISTS channels (
                        id SERIAL PRIMARY KEY,
                        channel VARCHAR(100),
                        link VARCHAR (100)
                        );"""
            )
            con.commit()
            cur.execute("""CREATE TABLE IF NOT EXISTS participant (
                        id SERIAL PRIMARY KEY,
                        id_participant VARCHAR(40),
                        first_name VARCHAR(150),
                        last_name VARCHAR (150),
                        user_name VARCHAR (40),
                        phone VARCHAR (40),
                        is_bot BOOLEAN,
                        channel VARCHAR (150)
                        );"""
                                )
            con.commit()

        with con:
            channel_link = f'https://t.me/{channel_username}'
            query = f"""SELECT * FROM channels WHERE channel='{channel_name}'"""
            cur.execute(query)
            response = cur.fetchall()

            if not response:
                new_channel = f"""INSERT INTO channels (channel, link) 
                                            VALUES ('{channel_name}', '{channel_link}');"""
                try:
                    cur.execute(new_channel)
                    con.commit()
                    print('*********add to channels ', channel_name, channel_link)
                except Exception as e:
                    print(e)
            else:
                print('This CHANNEL exist already')

        with con:

            channel = channel_name
            print('all user len = ', len(all_user_dictionary))
            for i in all_user_dictionary:

                id_participant = i['id']
                first_name = i['first_name']
                last_name = i['last_name']
                user_name = i['user']
                phone = i['phone']
                is_bot = i['is_bot']


                query = f"""SELECT * FROM participant WHERE id_participant='{id_participant}'"""
                cur.execute(query)
                response = cur.fetchall()

                if not response:
                    new_post = f"""INSERT INTO participant (id_participant, first_name, last_name, user_name, phone, is_bot, channel) 
                                            VALUES ('{id_participant}', '{first_name}', 
                                            '{last_name}', '{user_name}', '{phone}', '{is_bot}', '{channel}');"""
                    try:
                        cur.execute(new_post)
                        con.commit()
                        print('!!!!!!!!!!!!!add to users ', i)
                    except Exception as e:
                        print(e)
                else:
                    print('This user exist already', i)

    #--------------------------------------------------

    def push_to_bd(self, results_dict):

        or_exists = False
        time_index = True
        global quant

        con = self.connect_db()
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

        with con:
            try:
                query = f"""SELECT * FROM telegram_channels_professions WHERE title='{title}' AND body='{body}'"""
                cur.execute(query)
                r = cur.fetchall()

                if not r:
                    new_post = f"""INSERT INTO telegram_channels_professions (chat_name, title, body, profession, time_of_public, created_at) 
                                VALUES ('{chat_name}', '{title}', '{body}', '{profession}', '{time_of_public}', '{created_at}');"""
                    cur.execute(new_post)
                    con.commit()
                    print(quant, f'= Added to DB = {profession}', results_dict)
                    or_exists = True
                    quant += 1
                    time_index = True
                    time.sleep(15)
                else:
                    print(quant, f'This message exists already = ', results_dict)

                    #######################
                    or_exists = True
                    time_index = True
                    time.sleep(15)
                    #######################

            except Exception as e:
                print('Dont push in db, error = ', e)

            return {'or_exists': or_exists, 'time_index': time_index, 'profession': profession}

    def sort_by_profession(self, title, body):
        self.check_dictionary = {
            'title': {
                'backend': 0,
                'frontend': 0,
                'devops': 0,
                'developer': 0,
                'fullstack': 0,
                'mobile': 0,
                'pm': 0,
                'ba': 0,
                'designer': 0,
                'qa': 0,
                'analyst': 0,
                'mobile_developer': 0,
                'hr': 0,
                'ad': 0,
            },
            'body': {
                'backend': 0,
                'frontend': 0,
                'devops': 0,
                'developer': 0,
                'fullstack': 0,
                'mobile': 0,
                'pm': 0,
                'ba': 0,
                'designer': 0,
                'qa': 0,
                'analyst': 0,
                'mobile_developer': 0,
                'hr': 0,
                'ad': 0,
            }
        }

        counter = 1
        counter2 = 1
        pattern_ad = r'ищу\s{0,1}работу|opentowork|\bsmm\b|\bcopyright\w{0,3}\b|\btarget\w{0,3}\b|фильм на вечер|' \
                     r'хотим рассказать о новых каналах|#резюме|кадровое\s{0,1}агентство|skillbox|' \
                     r'зарабатывать на крипте|\bсекретар\w{0,2}|делопроизводител\w{0,2}'
        pattern_backend = r'back\s{0,1}end|б[е,э]к\s{0,1}[е,э]нд[а-я]{0,2}|backend.{0,1}developer|datascientist|datascience'
        pattern_frontend = r'front.*end|фронт.*[е,э]нд[а-я]{0,2}\B|vue\.{0,1}js\b|\bangular\b'
        pattern_devops = r'dev\s*ops'
        pattern_developer = r'^(frontend|backend)\s{0,1}developer|разработчик[а-я]{0,2}\B|site\s*reliability|typescript'
        pattern_backend_languages = r'python[\s,#]|scala[\s,#]|java[\s,#]|linux[\s,#]|haskell[\s,#]|php[\s,#]|server|' \
                                    r'\bсервер\w{0,3}\b|c\+\+|\bml\b|\bnode.{0,1}js\b|docker|java\/{0,1}|scala\/{0,1}'
        pattern_frontend_languages = r'javascript|html|css|react\s*js|\.net|firebase|\bnode.{0,1}js\b|vue\.{0,1}js'
        pattern_backend_mobile = r'android|ios'
        pattern_fullstack = r'full.{0,1}stack'
        pattern_pm = r'product\s*manager|прод[а,у]кт\s*м[е,а]н[е,а]джер|project\s*manager|marketing\s*manager|marketing'
        pattern_designer = r'дизайнер[а-я]{0,2}\B|designer|\bui\s'
        pattern_analitic = r'analyst|аналитик[а-я]{0,2}\B'
        pattern_qa = r'qa\b|тестировщик[а-я]{0,2}\B|qaauto\b|тестирован[а-я]{0,2}\B'
        pattern_hr = r'\bhr\b|рекрутер[а-я]{0,2}\B'

        text = [title.lower(), body.lower()]
        text_field = ['title', 'body']

        k = 0
        for item in text:
            looking_for = re.findall(pattern_ad, item)
            if looking_for:
                self.check_dictionary[text_field[k]]['ad'] += len(looking_for)

            looking_for = re.findall(pattern_backend, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['backend'] += len(looking_for)

            looking_for = re.findall(pattern_frontend, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['frontend'] += len(looking_for)

            looking_for = re.findall(pattern_devops, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['devops'] += len(looking_for)

            looking_for = re.findall(pattern_backend_mobile, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['mobile'] += len(looking_for)

            looking_for = re.findall(pattern_fullstack, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['fullstack'] += len(looking_for)

            looking_for = re.findall(pattern_backend_languages, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['backend'] += len(looking_for)

            looking_for = re.findall(pattern_frontend_languages, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['frontend'] += len(looking_for)

            looking_for = re.findall(pattern_developer, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['developer'] += len(looking_for)

            looking_for = re.findall(pattern_pm, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['pm'] += len(looking_for)

            looking_for = re.findall(pattern_designer, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['designer'] += len(looking_for)

            looking_for = re.findall(pattern_analitic, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['analyst'] += len(looking_for)

            looking_for = re.findall(pattern_qa, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['qa'] += len(looking_for)

            looking_for = re.findall(pattern_hr, item)
            if looking_for:
                counter += 1
                self.check_dictionary[text_field[k]]['hr'] += len(looking_for)
            else:
                counter2 += 1

            k += 1

        profession = self.analys_profession()

        return profession

    def analys_profession(self):
        max_title_value = 0
        max_title_key = ''

        for key in self.check_dictionary['title']:
            if self.check_dictionary['title'][key] > max_title_value:
                max_title_value = self.check_dictionary['title'][key]
                max_title_key = key

        max_body_value = 0
        max_body_key = ''
        for key in self.check_dictionary['body']:
            if self.check_dictionary['body'][key] > max_body_value:
                max_body_value = self.check_dictionary['body'][key]
                max_body_key = key

        print(self.check_dictionary)

        print('title', max_title_key, max_title_value)
        print('body', max_body_key, max_body_value)

        profession = ''

        if max_title_value == 0 and max_body_value == 0:
            profession = 'ad'

        elif max_title_value == 0 and max_body_value != 0:
            profession = max_body_key

        elif max_title_key in ['frontend', 'backend'] and max_body_key == 'developer':
            profession = max_title_key

        elif max_title_key != max_body_key and max_body_value != 0:
            profession = max_body_key

        elif max_title_key and not max_body_key and max_body_value == 0:
            profession = max_title_key

        elif max_title_key == max_body_key:
            profession = max_title_key

        elif self.check_dictionary['title']['fullstack'] or self.check_dictionary['body']['fullstack']:
            profession = 'fullstack'

        if max_title_key == 'qa' and max_body_key == 'backend':
            profession = max_title_key

        if self.check_dictionary['title']['qa'] or self.check_dictionary['body']['qa']:
            profession = 'qa'

        if self.check_dictionary['title']['fullstack'] or self.check_dictionary['body']['fullstack']:
            profession = 'fullstack'

        if max_title_key == 'devops' and (max_body_key == 'backend' or max_body_key == 'frontend'):
            profession = max_title_key

        if self.check_dictionary['title']['mobile'] or self.check_dictionary['body']['mobile']:
            profession = 'mobile'

        if self.check_dictionary['title']['ad']:
            profession = 'ad'

        print(profession.upper())

        return profession

    def get_all_from_db(self):
        con = self.connect_db()

        cur = con.cursor()

        query = """SELECT * FROM telegramchannels"""
        with con:
            cur.execute(query)
            r = cur.fetchall()
            for i in r:
                print(i)


def main():
    get_messages = WriteToDbMessages()
    get_messages.start(limit_msg=20)

    # print("Listening chats...")
    # client.start()
    # ListenChat()
    # client.run_until_disconnected()

main()
