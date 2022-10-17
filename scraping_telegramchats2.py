import asyncio
import random
from types import NoneType
import pandas as pd
import configparser
import time
from datetime import timedelta, datetime
import psycopg2
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from db_operations.scraping_db import DataBaseOperations
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch, PeerChannel, InputPeerChannel
from links import list_links
from telethon.sync import TelegramClient
from telethon import client, events
from telethon.tl.functions.messages import GetHistoryRequest, ImportChatInviteRequest
from sites.scraping_geekjob import GeekJobGetInformation
from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
from aiogram.utils.markdown import hlink

config = configparser.ConfigParser()
config.read("./settings/config.ini")
#--------------------------- забираем значения из config.ini-------------------------------
api_id = config['Ruslan']['api_id']
api_hash = config['Ruslan']['api_hash']

quant = 1  # счетчик вывода количества запушенных в базу сообщений (для контроля в консоли)

database = config['DB3']['database']
user = config['DB3']['user']
password = config['DB3']['password']
host = config['DB3']['host']
port = config['DB3']['port']

con = psycopg2.connect(
    database=database,
    user=user,
    password=password,
    host=host,
    port=port
)

# client = TelegramClient('137336064', int(api_id), api_hash)
# client.connect()
# companies = DataBaseOperations(con=con).get_all_from_db('companies')

class PushChannels:  # I bring that class from scarping_push_to_channel.py

    def __init__(self, client, bot_dict):
        # if not client:
        #     client=TelegramClient('137336064', int(api_id), api_hash)
        #     client.start()
        self.client = client
        self.msg = []
        self.bot_dict = bot_dict
        self.bot = self.bot_dict['bot']
        self.accept_channel_list = ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst',
                                    'fullstack', 'mobile', 'qa', 'hr', 'game', 'ba', 'marketing', 'junior',
                                    'sales_manager']  # channel limiter
        self.last_id_agregator = 0

    async def compose_and_send_messages(self):
        companies = DataBaseOperations(con=con).get_all_from_db(table_name='companies', without_sort=True)
        for table in self.accept_channel_list:
            vacancies = DataBaseOperations(con=con).get_all_from_db(table, param="WHERE DATE(created_at)>'2022-10-09'")

            count_messages = 1
            short_message = f"Вакансии для {table.capitalize()}\n\n"
            for item in vacancies:
                title = item[2]
                body = item[3]
                link = item[6]
                prof = AlexSort2809()
                response = prof.sort_by_profession_by_Alex(title, body, companies)
                params = response[1]

                if params['company_hiring']:
                    short_message += f"Компания {params['company_hiring']}\n"
                if params['english_level']:
                    short_message += f"{params['english_level']}\n"
                if params['jobs_type']:
                    short_message += f"{params['jobs_type']}\n"
                if params['city']:
                    short_message += f"{params['city']}\n"
                if params['relocation']:
                    short_message += f"params['relocation']\n"
                short_message += f"[Подробнее >>>] {link}"

                if count_messages > 4:
                    await self.bot.send_message(config['My_channels'][f'{table}_channel'], short_message, parse_mode="Markdown")
                    count_message = 1
                    short_message = f"Вакансии для {table.capitalize()}\n\n"
                else:
                    short_message += f"\n\n"
                    count_messages += 1

    async def push_new_hub(self, results_dict, forwarded_message, last_id_agregator):

        # ONLY FOR PUSH TO PROF DB ALL MESSAGES AND PUSH TO AGREGATOR CHANNEL MESSAGES FOR WRITE
        # TO PROF DB THE AGREGATOR'S LINKS WITH MESSAGES

        companies = DataBaseOperations(con=con).get_all_from_db(table_name='companies', without_sort=True)
        prof = AlexSort2809()
        response = prof.sort_by_profession_by_Alex(results_dict['title'], results_dict['body'], companies)
        profession_list = response[0]  # dict with professions and others
        params = response[1]  # dict with param as country, english, relocation and others

        db = DataBaseOperations(con=con)
        response_dict = db.push_to_bd(results_dict=results_dict,
                                      profession_list=profession_list,
                                      agregator_link=f"{config['My_channels']['agregator_link']}/{last_id_agregator}")

        # check does this dict have 'no_sort'
        message = ''
        if 'no_sort' not in response_dict:
            for key in response_dict:
                if not response_dict[key]:
                    message += f"\n\nБольше {key.capitalize()} вакансий в канале {config['Links'][key]}"
            if message:
                await self.try_to_send_message_to_channel(config['My_channels']['agregator_channel'], forwarded_message['message'], message)
                last_id_agregator += 1
        elif not response_dict['no_sort']:
            await self.try_to_send_message_to_channel(config['My_channels']['no_sort_channel'], f'n0_sort\n\n' + forwarded_message['message'])

        # ALL TASKS WERE COMPETE. NEXT STEP WILL GET FROM EACH PROFESSION MESSAGES FOR COMPOSE IT TO SEVERAL LONG MESSAGES

        return last_id_agregator

    async def push(self, results_dict, forwarded_message, last_id_agregator):
        block = False
        channels = None
        channels_for_new_bot = []
        self.last_id_agregator = last_id_agregator

        companies = DataBaseOperations(con=con).get_all_from_db(table_name='companies', without_sort=True)
        prof = AlexSort2809()
        response = prof.sort_by_profession_by_Alex(results_dict['title'], results_dict['body'], companies)
        profession_list = response[0]  # dict with professions and others
        params = response[1]  # dict with param as country, english, relocation and others

# ---------------- send to DB full messages -------------------
        response_dict = DataBaseOperations(con=con).push_to_bd(
            results_dict=results_dict,
            profession_list=profession_list,
            agregator_link = f"{config['My_channels']['agregator_link']}/{self.last_id_agregator}"
        )

# --------------- get short messages -----------------------
        short_message = await self.get_short_message(results_dict['title'], results_dict['body'], profession_list, params)
        print(short_message)

        pass


# ------------- new code -------------------
        values = response_dict.values()  # are there any False?
        # short_message = forwarded_message['message'].replace('\n', '').strip()[0:40]
        if False in values:
            print(f'message: {short_message}\n')
            # self.msg.append(await bot.send_message(bot_dict['chat_id'], f"message: {short_message}"))
        else:
            print(f'message: {short_message}\n')
            # self.msg.append(await bot.send_message(bot_dict['chat_id'], f"message: {short_message}\n<b>exists in DB</b>", parse_mode='html'))

        if 'no_sort' not in response_dict:  # if there is the professions than add 'agregator' to the dict
            if False in values:
                response_dict['agregator'] = False
# ------------------------- end ----------------
        if type(response_dict) is NoneType:  # is it really needs?
            print('&&&&&&&&& NONeTYPE')
        else:
            channels = response_dict.keys()
            message = ''
            markup = None
            for channel in reversed(channels):
                if not response_dict[channel]:
#------------------ the new code for don't use forward bot and send message from this code ------------------------
                    channel_id = config['My_channels'][f'{channel}_channel']

                    if channel != 'agregator' and channel in self.accept_channel_list:  # channel limiter
                        await self.try_to_send_message_to_channel(channel_id, short_message, message, markup)
                        print(f'have pushed to channel = {channel}')

                    elif channel == 'agregator':
                        for i in channels:  # compose message More vacancies in <link>
                            if i != 'agregator':
                                message += f"Больше вакансий <b>{i.capitalize()}</b> в канале {config['Links'][i]}\n\n"

                        await self.try_to_send_message_to_channel(channel_id, forwarded_message['message'], message, markup)
                        self.last_id_agregator += 1
                        print(f'have pushed to channel = {channel}')
                        # compose the keyboard
                        markup = InlineKeyboardMarkup(row_width=1)
                        markup.add(InlineKeyboardButton('Подробнее >>>', f"{config['My_channels']['agregator_link']}/{int(self.last_id_agregator)}"))
                        message = ''
        await self.delete_messages()
        return self.last_id_agregator

    async def delete_messages(self):
        for i in self.msg:
            await i.delete()

    async def try_to_send_message_to_channel(self, channel_id, message_to_send, message=None, markup=None):
        try:
            await self.bot.send_message(int(channel_id), message_to_send + f'\n\n{message}', parse_mode='html', reply_markup=markup)
            time.sleep(1)
        except Exception as e:
            await self.bot.send_message(self.bot_dict['chat_id'], f'Error = {str(e)}\nЭто сообщение автоматически переслано Руслану')
            time.sleep(1)
            await self.bot.send_message(137336064, f'Error = {str(e)}\n\nchannel = {channel_id}')
            time.sleep(1)
            await self.bot.send_message(137336064, message_to_send + f'\n\n{message}', parse_mode='html')
            time.sleep(1)

        time.sleep(1)

    async def get_short_message(self, title, body, profession_list, params):

        short_message = 'Вакансия: '
        for i in profession_list['profession']:
            short_message += f'{i}, '

        if params['company_hiring']:
            short_message += f"\nКомпания {params['company_hiring']}"
        if params['english_level']:
            short_message += f"\nEnglish: {params['english_level']}"
        if params['jobs_type']:
            short_message += f"\nТип работы: {params['jobs_type']}"
        if params['city']:
            short_message += f"\nГород: {params['city']}"
        if params['relocation']:
            short_message += f"\nРелокация: {params['relocation']}"

        return short_message

class WriteToDbMessages():

    def __init__(self, client, bot_dict):

        #if client is empty
        if not client:
            client = TelegramClient('137336064', int(api_id), api_hash)
            self.client = client
            self.client.start()
        else:
            self.client = client
            if not self.client.is_connected():
                self.client.start()
                print('client needed to connect')
            else:
                print('client is on connection')
        self.client = client
        self.bot_dict = bot_dict
        self.last_id_agregator = 0
        self.valid_profession_list = ['marketing', 'ba', 'game', 'product', 'mobile',
                                      'pm', 'sales_manager', 'analyst', 'frontend',
                                      'designer', 'devops', 'hr', 'backend', 'qa', 'junior']
        self.start_date_time = None
        self.companies = []

    async def dump_all_participants(self, channel):
        """Записывает json-файл с информацией о всех участниках канала/чата"""
        offset_user = 0  # номер участника, с которого начинается считывание
        limit_user = 100  # максимальное число записей, передаваемых за один раз

        all_participants = []  # список всех участников канала
        filter_user = ChannelParticipantsSearch('')

        print(f'Start scraping participants from {channel}\n\n')

        try:
            while True:
                participants = await self.client(GetParticipantsRequest(channel,
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

            print(f'\nPause 40-60 sec...')
            time.sleep(random.randrange(40, 60))
            print('...Continue')

        except Exception as e:
            print(f'Error для канала {channel}: {e}')

    async def dump_all_messages(self, channel, limit_msg):

        print('dump')
        self.count_message_in_one_channel = 1
        block = False
        offset_msg = 0  # номер записи, с которой начинается считывание
        # limit_msg = 1   # максимальное число записей, передаваемых за один раз
        all_messages = []  # список всех сообщений
        total_messages = 0
        total_count_limit = limit_msg  # значение 0 = все сообщения
        history = None

        while True:
            try:
                history = await self.client(GetHistoryRequest(
                    peer=channel,
                    offset_id=offset_msg,
                    offset_date=None, add_offset=0,
                    limit=limit_msg, max_id=0, min_id=0,
                    hash=0))
            except Exception as e:
                await self.bot_dict['bot'].send_message(
                                        self.bot_dict['chat_id'],
                                        f"Getting history:\n{str(e)}: {channel}\npause 25-30 seconds...",
                                        parse_mode="HTML",
                                        disable_web_page_preview = True)
                time.sleep(2)

            # if not history.messages:
            if not history:
                await self.bot_dict['bot'].send_message(self.bot_dict['chat_id'], f'Not history for channel {channel}')
                break
            messages = history.messages
            for message in messages:
                if not message.message:  # если сообщение пустое, например "Александр теперь в группе"
                    pass
                else:
                    all_messages.append(message.to_dict())

            offset_msg = messages[len(messages) - 1].id
            total_messages = len(all_messages)
            if total_count_limit != 0 and total_messages >= total_count_limit:
                break

        await self.process_messages(channel, all_messages)
        print('pause 25-35 sec.')
        time.sleep(random.randrange(25, 35))

    async def process_messages(self, channel, all_messages):
        # channel_name = f'@{channel.username} | {channel.title}'
        # channel_name = channel
        for one_message in reversed(all_messages):
            await self.operations_with_each_message(channel, one_message)


    async def operations_with_each_message(self, channel, one_message):

        title = one_message['message'].partition(f'\n')[0]
        body = one_message['message'].replace(title, '').replace(f'\n\n', f'\n')
        date = (one_message['date'] + timedelta(hours=3))
        results_dict = {
            'chat_name': channel,
            'title': title,
            'body': body,
            'profession': '',
            'vacancy': '',
            'vacancy_url': '',
            'company': '',
            'english': '',
            'relocation': '',
            'job_type': '',
            'city': '',
            'salary': '',
            'experience': '',
            'contacts': '',
            'time_of_public': date,
            'created_at': ''
        }

        print(f"channel = {channel}")
# =============================== scheme next steps =======================================
        # we are in the messages loop, it takes them by one
        # -----------------------LOOP---------------------------------
        # STEP0/ I have to get id for last message in agregator_channel
        #          I did it previous step (look at up)

        # STEP NEXT/ Get the profession/ previous it needs to get companies list from table companies
        #           I have got the companies previous. Look at up
        self.companies = DataBaseOperations(con=con).get_all_from_db(table_name='companies', without_sort=True)  # check!!!

        response = AlexSort2809().sort_by_profession_by_Alex(title, body, self.companies)
        profession = response['profession']
        params = response['params']

        # STEP1/ we need to write to DB by professions that message with last message's id in agregator_channel
        #       I can get this with DBOperations()
        if 'no_sort' not in profession['profession']:
            profession = await self.clear_not_valid_professions(profession)  # delete not valid keys (middle, senior and others)
            print('valid professions ', profession['profession'])
            if profession['profession']:
                # write to profession's tables. Returns dict with professions as a key and False, if it was written and True if existed
                response_dict = DataBaseOperations(con=con).push_to_bd(results_dict, profession, self.last_id_agregator) #check!!!
                print('from db professions ', response_dict)

        # STEP2/ send they to agregator channel if they have the professions
        #       We need to control id message and send it to field on profession table
        #       Increase number last message's id += 1
                if False in response_dict.values():

                    # send to agregator channel one time
                    try:
                        await self.bot_dict['bot'].send_message(
                            config['My_channels']['agregator_channel'], one_message['message'])
                        self.last_id_agregator += 1
                        print('\nit was sended in agregator channel')
                        print('time_sleep 3 sec\n')
                        time.sleep(3)
                    except Exception as e:
                        await self.bot_dict['bot'].send_message(
                            self.bot_dict['chat_id'],
                            f'Error to send to channel {e}')
        else:
            try:
                await self.bot_dict['bot'].send_message(
                    config['My_channels']['no_sort_channel'], one_message['message'])
                await asyncio.sleep(2)
            except Exception as e:
                await self.bot_dict['bot'].send_message(
                    self.bot_dict['chat_id'], f"Error in channel no_sort: {e}")
                await asyncio.sleep(2)

#---------------------- END OF LOOP ---------------------------------

        # STEP3/ we have to get from each table last messages and compose the shorts with 5 short messages with links

    async def get_last_and_tgpublic_shorts(self, time_start, shorts=False):
        """
        It gets last messages from profession's tables,
        composes shorts from them
        and send to profession's tg channels
        Here user decide short or full sending

        """
        self.companies = DataBaseOperations(con=con).get_all_from_db(table_name='companies', without_sort=True)  # check!!!

        if shorts:
            await self.send_sorts(time_start=time_start)  # 1. for send shorts
        else:
            await self.send_fulls(time_start=time_start)  # 2. for send last full messages from db

        await self.bot_dict['bot'].send_message(self.bot_dict['chat_id'], 'DONE')

    async def send_sorts(self, time_start):
        messages_counter = 1
        short_message = ''
        for pro in self.valid_profession_list:
            print(f"It gets from profession's tables = {pro}")
            # get last records from table with profession PRO
            response_messages = DataBaseOperations(con=con).get_all_from_db(pro,
                                                                            param=f"WHERE created_at > "
                                                                                  f"'{time_start['year']}-{time_start['month']}-{time_start['day']} {time_start['hour']}:{time_start['minute']}:{time_start['sec']}'")  # check!!!
            for response in response_messages:
                title = response[2]
                body = response[3]
                agregator_id = response[7]
                response_params = AlexSort2809().sort_by_profession_by_Alex(title, body, self.companies)
                params = response_params['params']
                short_message += f"Вакансия: \n"
                short_message += f"Компания: {params['company_hiring']}\n" if params['company_hiring'] else ''
                short_message += f"English: {params['english_level']}\n" if params['english_level'] else ''
                short_message += f"Тип работы: {params['jobs_type']}\n" if params['jobs_type'] else ''
                short_message += f"Релокация: {params['relocation']}\n" if params['relocation'] else ''
                short_message += f"Город: {params['city']}\n" if params['city'] else ''
                short_message += hlink('Подробнее >>>\n\n', f"{config['My_channels']['agregator_link']}/{agregator_id}")

                # STEP4/ send this shorts in channels
                if messages_counter > 5 or messages_counter == len(response_messages):
                    short_message = f"Вакансии для {pro}:\n\n" + short_message
                    await self.bot_dict['bot'].send_message(
                        config['My_channels'][f"{pro}_channel"],
                        short_message, parse_mode='HTML',
                        disable_web_page_preview = True)

                    print(f'\nprinted short in channel {pro}\n')
                    time.sleep(2)
                    short_message = ''
                    messages_counter = 1
                else:
                    messages_counter += 1

    async def send_fulls(self, time_start):
        for pro in self.valid_profession_list:
            print(f"It gets from profession's tables = {pro}")
            # get last records from table with profession PRO
            response_messages = DataBaseOperations(con=con).get_all_from_db(pro,
                                                                            param=f"WHERE created_at > "
                                                                                  f"'{time_start['year']}-{time_start['month']}-{time_start['day']} {time_start['hour']}:{time_start['minute']}:{time_start['sec']}'")  # check!!!
            for response in response_messages:
                title = response[2]
                body = response[3]
                text = title+body

                try:
                    await self.bot_dict['bot'].send_message(config['My_channels'][f"{pro}_channel"], text)
                    time.sleep(2)
                except Exception as e:
                    await self.bot_dict['bot'].send_message(self.bot_dict['chat_id'], f'There is the error {e} (send_fulls')
                    time.sleep(2)

    async def clear_not_valid_professions(self, profession):
        exclude_list = []
        values_list = profession['profession']
        for value in values_list:
            if value not in self.valid_profession_list:
                exclude_list.append(value)
        for exclude in exclude_list:
            profession['profession'].remove(exclude)
        return profession

    async def get_last_id_agregator(self):
        history_argegator = await self.client(GetHistoryRequest(
            peer=config['My_channels']['agregator_link'],
            offset_id=0,
            offset_date=None, add_offset=0,
            limit=1, max_id=0, min_id=0,
            hash=0))
        last_id_agregator = history_argegator.messages[0].id
        print('last id in agregator = ', last_id_agregator)
        time.sleep(random.randrange(15, 20))
        return last_id_agregator

    async def main_start(self, list_links, limit_msg, action):

        print('main_start')
        channel = ''

        self.last_id_agregator = await self.get_last_id_agregator()+1
        pass
        for url in list_links:

            await self.dump_all_messages(url, limit_msg)  # creative resolve the problem of a wait seconds

    #         bool_index = True
    #
    #         try:
    #             channel = await self.client.get_entity(url)                # channel = await self.client.get_entity(url)
    #         except Exception as e:
    #             if e.args[0] == 'Cannot get entity from a channel (or group) that you are not part of. Join the group and retry':
    #                 private_url = url.split('/')[-1]
    #                 try:
    #                     await self.client(ImportChatInviteRequest(private_url))  # если канал закрытый, подписаться на него
    #                     channel = await self.client.get_entity(url)  # и забрать из него историю сообщений
    #                 except Exception as e:
    #                     print(f'Error: Цикл прошел с ошибкой в месте, где нужна подписка: {e}')
    #
    #                     await self.bot_dict['bot'].send_message(
    #                         self.bot_dict['chat_id'],
    #                         f"{str(e)}: {url}\npause 25-30 seconds...",
    #                         parse_mode="HTML",
    #                         disable_web_page_preview = True)
    #                     bool_index = False
    #
    #             else:
    #                 print(f'ValueError for url {url}: {e}')
    #
    #                 await self.bot_dict['bot'].send_message(
    #                     self.bot_dict['chat_id'],
    #                     f"{str(e)}: {url}\npause 25-30 seconds...", parse_mode="HTML",
    #                     disable_web_page_preview = True)
    #                 bool_index = False
    #
    #         if bool_index:
    #             self.count_message_in_one_channel = 1
    #             match action:
    #                 case 'get_message':
    #                     self.start_date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #                     print(f'Was started at {self.start_date_time}')
    #                     await self.dump_all_messages(channel, limit_msg)
    #                     print('//////////////////////////////////////////////////////////////////////////////////////////////////')
    #                 case 'get_participants':
    #                     await self.dump_all_participants(channel)
    #         else:
    #             print('Wait 25-30 seconds...')
    #             time.sleep(random.randrange(25, 30))

    async def start(self, limit_msg, action):
        print('start')
        # async with self.client:
        #     self.client.loop.run_until_complete(self.main_start(list_links, limit_msg, action))
        # async with self.client:
        await self.main_start(list_links, limit_msg, action)
        # return self.client

async def main(client, bot_dict):
    get_messages = WriteToDbMessages(client, bot_dict)
    await get_messages.start(limit_msg=5, action='get_message')  #get_participants get_message

    # print("Listening chats...")
    # client.start()
    # ListenChat()
    # client.run_until_disconnected()
    # return client

# main()
