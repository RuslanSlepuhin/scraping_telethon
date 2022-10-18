import asyncio
import time

import psycopg2
import os
import random
import re
import urllib
from datetime import datetime, timedelta
import pandas
from aiogram import Bot, Dispatcher, executor, types
import logging
import configparser
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.dispatcher.filters.state import StatesGroup, State
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telethon import events
from telethon.sync import TelegramClient
from telethon.tl import functions
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser, InputUser, PeerUser, InputChannel, MessageService

from db_operations.scraping_db import DataBaseOperations
from links import list_links
from scraping_telegramchats2 import WriteToDbMessages, main
from sites.parsing_sites_runner import ParseSites

config = configparser.ConfigParser()
config.read("./settings/config.ini")
# token = config['Token']['token']
token = config['Token']['token']
api_id = config['Ruslan']['api_id']
api_hash = config['Ruslan']['api_hash']

logging.basicConfig(level=logging.INFO)
bot_aiogram = Bot(token=token)
storage = MemoryStorage()
dp = Dispatcher(bot_aiogram, storage=storage)

all_participant = []
marker = False
file_name = ''
marker_code = False
password = 0
con = None

print(f'Bot started at {datetime.now()}')

client = TelegramClient('ruslanslepuhin', int(api_id), api_hash)
client.start()

class InviteBot:

    def __init__(self):
        self.chat_id = None
        self.start_time_listen_channels = datetime.now()
        self.start_time_scraping_channels = None
        self.valid_profession_list = ['marketing', 'ba', 'game', 'product', 'mobile',
                                      'pm', 'sales_manager', 'analyst', 'frontend',
                                      'designer', 'devops', 'hr', 'backend', 'qa', 'junior']
        self.markup = None
        self.api_id = config['Ruslan']['api_id']
        self.api_hash = config['Ruslan']['api_hash']

    def main_invitebot(self):
        async def connect_with_client(message, api_id, api_hash, id_user, phone_number, password):

            global client, hash_phone
            e=None

            api_id = int(api_id)
            id_user = str(id_user)
            client = TelegramClient(id_user, api_id, api_hash)

            await client.connect()

            if not await client.is_user_authorized():
                try:
                    phone_code_hash = await client.send_code_request(str(phone_number))
                    hash_phone = phone_code_hash.phone_code_hash

                except Exception as e:
                    await bot_aiogram.send_message(message.chat.id, str(e))

                if not e:
                    await get_code(message)
            else:
                await bot_aiogram.send_message(message.chat.id, 'Connect - ok')

        async def or_connect(message):
            try:
                is_connect = client.is_connected()
            except Exception as e:
                return await bot_aiogram.send_message(message.chat.id, str(e))

            if is_connect:
                return True
            else:
                return False

        class Form(StatesGroup):
            api_id = State()
            api_hash = State()
            password = State()
            phone_number = State()
            code = State()

        @dp.message_handler(commands=['start', 'help'])
        async def send_welcome(message: types.Message):

            global phone_number, password, con
            self.chat_id = message.chat.id

        # -------- make an parse keyboard for admin ---------------
            parsing_kb = ReplyKeyboardMarkup(resize_keyboard=True)
            parsing_button1 = KeyboardButton('Get news from channels')
            parsing_button2 = KeyboardButton('Listen to channels')
            parsing_button3 = KeyboardButton('Digest')
            parsing_button4 = KeyboardButton('Invite people')

            parsing_kb.row(parsing_button1, parsing_button2)
            parsing_kb.row(parsing_button3, parsing_button4)


            # con = db_connect()
            await bot_aiogram.send_message(message.chat.id, f'Привет, {message.from_user.first_name}!', reply_markup=parsing_kb)
            await bot_aiogram.send_message(137336064, f'Start user {message.from_user.id}')
            pass
            #
            # id_customer = message.from_user.id
            # customer = await check_customer(message, id_customer)
            #
            # if customer:
            #         get_customer_from_db = get_db(id_customer)
            #         api_id = get_customer_from_db[0][2]
            #         api_hash = get_customer_from_db[0][3]
            #         phone_number = get_customer_from_db[0][4]
            #         try:
            #             if client.is_connected():
            #                 await client.disconnect()
            #         except:
            #             pass
            #         await connect_with_client(message, api_id, api_hash, id_customer, phone_number, password)

        # Возможность отмены, если пользователь передумал заполнять
        @dp.message_handler(state='*', commands=['cancel', 'start'])
        @dp.message_handler(Text(equals='отмена', ignore_case=True), state='*')
        async def cancel_handler(message: types.Message, state: FSMContext):
            current_state = await state.get_state()
            if current_state is None:
                return

            await state.finish()
            await message.reply('ОК')

        #------------------------ api id----------------------------------
        # api_id
        @dp.message_handler(state=Form.api_id)
        async def process_api_id(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['api_id'] = message.text

            await Form.next()
            await bot_aiogram.send_message(message.chat.id, "Введите api_hash (отменить /cancel)")

        #-------------------------- api_hash ------------------------------
        # api_hash
        @dp.message_handler(state=Form.api_hash)
        async def process_api_hash(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['api_hash'] = message.text

            await Form.next()
            await bot_aiogram.send_message(message.chat.id, "Введите password, если есть двухэтапная верификация (отменить /cancel)\n"
                                                    "Введите '0', если нет двухэтапной верификации")

        #-------------------------- password ------------------------------
        # password
        @dp.message_handler(state=Form.password)
        async def process_api_hash(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['password'] = message.text

            await Form.next()
            await bot_aiogram.send_message(message.chat.id, "Введите номер телефона (отменить /cancel)")

        #-------------------------- phone number ------------------------------
        # phone_number
        @dp.message_handler(state=Form.phone_number)
        async def process_phone_number(message: types.Message, state: FSMContext):

            global phone_number

            async with state.proxy() as data:
                data['phone_number'] = message.text

                await bot_aiogram.send_message(
                    message.chat.id,
                    f"Your api_id: {data['api_id']}\nYour api_hash: {data['api_hash']}\nYour phone number: {data['phone_number']}")

                api_id = data['api_id']
                api_hash = data['api_hash']
                password = data['password']
                phone_number = data['phone_number']

            send_to_db(message.from_user.id, int(api_id), api_hash, phone_number)  # записать в базу

            await connect_with_client(message, api_id=api_id, api_hash=api_hash, id_user=message.from_user.id, phone_number=phone_number, password=password)

        #-------------------------- code ------------------------------
        # code
        async def get_code(message):
            await Form.code.set()
            await bot_aiogram.send_message(message.chat.id, 'Введите код в формате 12345XXXXX6789, где ХХХХХ - цифры телеграм кода (отмена* /cancel)')

        @dp.message_handler(state=Form.code)
        async def process_phone_number(message: types.Message, state: FSMContext):

            global client, hash_phone, phone_number

            async with state.proxy() as data:
                data['code'] = message.text
                code = data['code'][5:10]
                try:
                    password = data['password']
                except:
                    password = '0'

                # if data['phone_number']:
                #     phone_number = data['phone_number']

                phone = phone_number

                try:
                    if password == '0':
                        await client.sign_in(phone=phone, code=code, phone_code_hash=hash_phone)
                    else:
                        await client.sign_in(phone=phone, password=password, code=code, phone_code_hash=hash_phone)

                    await bot_aiogram.send_message(message.chat.id, 'Connect - ok')
                except Exception as e:
                    await bot_aiogram.send_message(message.chat.id, str(e))

                await state.finish()

        @dp.callback_query_handler()
        async def catch_callback(callback: types.CallbackQuery):
            short_digest = ''
            response = []

            msg = await bot_aiogram.send_message(callback.message.chat.id, 'Please wait a few seconds ...')
            if callback.data == 'show_info_last_records':
                time_start = await get_time_start()
                for pro in self.valid_profession_list:
                    try:
                        response = DataBaseOperations(con=None).get_all_from_db(
                            pro,
                            param=f"WHERE created_at > '{time_start['year']}-{time_start['month']}-{time_start['day']} "
                                  f"{time_start['hour']}:{time_start['minute']}:{time_start['sec']}'", order=' ')
                        short_digest += f"<b>{pro}</b>: {len(response)} новых записей\n"

                    except Exception as e:
                        print(e)
                await msg.delete()
                await bot_aiogram.send_message(callback.message.chat.id, short_digest, parse_mode='html', reply_markup=self.markup)
                pass

            if callback.data == 'download_excel':
                pass

            if callback.data == 'send_digest_full':
                # ----------------------- send the messages to tg channels as digest or full --------------------------
                time_start = await get_time_start()
                await WriteToDbMessages(
                    client,
                    bot_dict={'bot': bot_aiogram, 'chat_id': callback.message.chat.id}).get_last_and_tgpublic_shorts(time_start, shorts=False)  # get from profession's tables and put to tg channels
            if callback.data == 'send_digest_shorts':
                # ----------------------- send the messages to tg channels as digest or full --------------------------
                time_start = await get_time_start()
                await WriteToDbMessages(
                    client,
                    bot_dict={'bot': bot_aiogram, 'chat_id': callback.message.chat.id}).get_last_and_tgpublic_shorts(time_start, shorts=True)

        @dp.message_handler(content_types=['text'])
        async def messages(message):

            global all_participant, marker, file_name, marker_code, client
            channel_to_send = None
            user_to_send = []
            msg = None

            if marker:
                channel = message.text
                channel_short_name = f"@{channel.split('/')[-1]}"
                try:
                    channel = await client.get_entity(channel)
                    channel_to_send = InputChannel(channel.id, channel.access_hash)  # был InputPeerChannel
                except Exception as e:
                    await bot_aiogram.send_message(message.chat.id, f'{e}\nУкажате канал в формате https//t.me/<имя канала> (без @)\n'
                                                            f'Обратите внимание на то, что <b>и Вы и этот бот</b> в этом канале должны быть <b>администраторами</b>', parse_mode='html')

                if channel_to_send:
                    try:
                        await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: Инвайт запущен', parse_mode='html')

                        n=0
                        numbers_invite = 0
                        numbers_failure = 0
                        was_subscribe = 0


                        # ---------------------------- отправлять по одному инвайту---------------------------------
                        for user in all_participant:
                            print('id: ', user[0], 'hash', user[1], 'username', user[2])

                            id_user = int(user[0])
                            access_hash_user = int(user[1])
                            username = user[2]
                            print(type(username))

                            try:
                                user_channel_status = await bot_aiogram.get_chat_member(chat_id=channel_short_name, user_id=id_user)
                                if user_channel_status.status != types.ChatMemberStatus.LEFT:
                                    if msg:
                                        await msg.delete()
                                        msg = None
                                    msg = await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: пользователь с id={id_user} уже подписан', parse_mode='html')
                                    await asyncio.sleep(1)
                                    was_subscribe += 1
                                    user_exists = True
                                else:
                                    user_exists = False

                            except Exception as e:
                                user_exists = False
                                if msg:
                                    await msg.delete()
                                    msg = None
                                await bot_aiogram.send_message(message.chat.id, str(e))

                            if not user_exists:
                                if username != 'None':
                                    user_to_send1 = await client.get_input_entity(username)
                                    user_to_send = [user_to_send1]
                                else:
                                    user_to_send = [InputUser(id_user, access_hash_user)]  # (PeerUser(id_user))


                                try:
                                    if msg:
                                        await msg.delete()
                                        msg = None

                                    # client.invoke(InviteToChannelRequest(channel_to_send,  [user_to_send]))
                                    # await client(InviteToChannelRequest(channel_to_send, user_to_send))  #work!!!!!

                                    await client(functions.channels.InviteToChannelRequest(channel_to_send, user_to_send))

                                    msg = await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}:</b> {user[0]} заинвайлся успешно\n'
                                                                                  f'({numbers_invite+1} инвайтов)', parse_mode='html')
                                    numbers_invite += 1


                                except Exception as e:
                                    if re.findall(r'seconds is required (caused by InviteToChannelRequest)', str(e)):
                                        break
                                    else:
                                        if msg:
                                            await msg.delete()
                                            msg = None
                                        await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: Для пользователя id={user[0]}\n{str(e)}', parse_mode='html')
                                        numbers_failure += 1
                                        msg = None


                                n += 1
                                await asyncio.sleep(random.randrange(5, 10))
                                if n >=198:
                                    if msg:
                                        await msg.delete()
                                        msg = None
                                    msg = await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: инвайт продолжится через 24 часа из-за ограничений Телеграм.\nНе завершайте сессию с ботом.\n'
                                                                                  f'Пока запущено ожидание по каналу {channel_short_name}, Вы можете отправить еще один файл (с другим названием) для инвайта в <b>ДРУГОЙ канал</b>', parse_mode='html')
                                    await asyncio.sleep(60*24+15)
                                    n=0
        # ---------------------------- end отправлять по одному инвайту---------------------------------

                        if msg:
                            await msg.delete()
                            msg = None
                        await bot_aiogram.send_message(message.chat.id,
                                           f'<b>{channel_short_name}</b>: {numbers_invite} пользователей заинвайтились, проверьте в канале\n'
                                           f'{numbers_failure} не заинватились в канал\n'
                                           f'{was_subscribe} были уже подписаны на канал', parse_mode='html')
                        all_participant = []
                        marker = False
                        os.remove(f'{file_name}')
                    except Exception as e:
                        if msg:
                            await msg.delete()
                            msg = None
                        await bot_aiogram.send_message(message.chat.id, f'{e}')

                #pass

            else:
                if message.text == 'Get news from channels':

                    # self.client = TelegramClient('ruslanslepuhin', int(self.api_id), self.api_hash)
                    # self.client.start()

                    if not client.is_connected():  # run client if it was working in invite
                        client.start()

                    self.start_time_scraping_channels = datetime.now()
                    print('time_start = ', self.start_time_scraping_channels)

                    # await bot.delete_message(message.chat.id, message.message_id)
                    await bot_aiogram.send_message(message.chat.id, 'Scraping is starting')


        # -----------------------parsing telegram channels -------------------------------------
                    await main(client, bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id})  # run parser tg channels and write to profession's tables
                    await bot_aiogram.send_message(
                        message.chat.id,
                        'Парсинг ТГ каналов прошел успешно, всё записано в базу',
                        parse_mode='HTML')
                    time.sleep(2)

        # ---------------------- parsing the sites. List of them will grow ------------------------
        #             if not client.is_connected():
        #                 client.disconnect()
        #                 client.connect()
                    await bot_aiogram.send_message(message.chat.id, 'Парсятся сайты...')
                    psites = ParseSites(client=client)
                    await psites.call_sites()  # paes
                    await bot_aiogram.send_message(message.chat.id, 'Парсинг сайтов прошел успешно, всё записано в базу. Можно выгрузить кнопкой <b>Digest</b>', parse_mode='html')


                #----------------------- Listening channels at last --------------------------------------

                if message.text == 'Invite people':
                    if client.is_connected():
                        client.disconnect()

                    id_customer = message.from_user.id
                    customer = await check_customer(message, id_customer)
                    con = db_connect()
                    if customer:
                        get_customer_from_db = get_db(id_customer)
                        api_id = get_customer_from_db[0][2]
                        api_hash = get_customer_from_db[0][3]
                        phone_number = get_customer_from_db[0][4]
                        try:
                            if client.is_connected():
                                await client.disconnect()
                        except:
                            pass
                        await connect_with_client(message, api_id, api_hash, id_customer, phone_number, password)


                    # await bot.delete_message(message.chat.id, message.message_id)
                    # response = requests.get(url='https://tg-channel-parse.herokuapp.com/scrape')
                    # await bot.send_message(message.chat.id, response.status_code)
                if message.text == 'Listen to channels':
                    # await bot.delete_message(message.chat.id, message.message_id)
                    # await bot.send_message(message.chat.id, "Bot is listening TG channels and it will send notifications here")
                    # ListenChats()
                    # await client.run_until_disconnected()
                    pass

                if message.text == 'Digest':
                    self.markup = InlineKeyboardMarkup(row_width=1)
                    but_show = InlineKeyboardButton('Показать сводку по собранным сообщениям', callback_data='show_info_last_records')
                    but_download_excel = InlineKeyboardButton('Выгрузить excel для внесения правок', callback_data='download_excel')
                    but_send_digest_full = InlineKeyboardButton('Разлить по каналам fulls', callback_data='send_digest_full')
                    but_send_digest_shorts = InlineKeyboardButton('Разлить по каналам shorts', callback_data='send_digest_shorts')
                    self.markup.add(but_show, but_download_excel, but_send_digest_full, but_send_digest_shorts)
                    time_start = await get_time_start()
                    await bot_aiogram.send_message(
                        message.chat.id,
                        f"В базу записаны последние сообщения, начиная с "
                        f"{time_start}. Выберите действие", reply_markup=self.markup)
                    # show inline menu:
                    # - show numbers of last records from each table
                    # - download excel with last records, rewrite all changes and put messages in the channels
                    # - send digest to the channels without change
                    pass

                else:
                    pass
                    # await bot.send_message(message.chat.id, 'Отправьте файл')

        async def get_separate_time(time_in):
            start_time = {}
            start_time['year'] = time_in.strftime('%Y')
            start_time['month'] = time_in.strftime('%m')
            start_time['day'] = time_in.strftime('%d')
            start_time['hour'] = time_in.strftime('%H')
            start_time['minute'] = time_in.strftime('%M')
            start_time['sec'] = time_in.strftime('%S')
            return start_time

        @dp.message_handler(content_types=['document'])
        async def download_doc(message: types.Message):

            global all_participant, marker, file_name


            if client.is_connected():

                all_participant = []
                excel_data_df = None

                document_id = message.document.file_id
                file_info = await bot_aiogram.get_file(document_id)
                fi = file_info.file_path
                file_name = message.document.file_name
                urllib.request.urlretrieve(f'https://api.telegram.org/file/bot{token}/{fi}', f'./{file_name}')

                try:
                    excel_data_df = pandas.read_excel(f'{file_name}', sheet_name='Sheet1')
                except Exception as e:
                    await bot_aiogram.send_message(message.chat.id, f'{e}')

                if 'id_participant' in excel_data_df.columns and 'access_hash' in excel_data_df.columns:

                    excel_dict = {
                        'id_participant': excel_data_df['id_participant'].tolist(),
                        'access_hash': excel_data_df['access_hash'].tolist(),
                        'user': excel_data_df['username'].tolist(),
                    }
                    print(excel_dict)

                    n = 0
                    while n<len(excel_dict['id_participant']):
                        all_participant.append([int(excel_dict['id_participant'][n]), int(excel_dict['access_hash'][n]), excel_dict['user'][n]])
                        n += 1

                    for iii in all_participant:
                        for jjj in iii:
                            print(jjj, type(jjj))

                    print('all_participant = ', all_participant)

                    await bot_aiogram.send_message(
                        message.chat.id,
                        f'Получен файл с {len(all_participant)} пользователями\n'
                        f'Введите url канала в формате https//t.me/<имя канала> без @:\n'
                    )

                    marker = True
                else:
                    await bot_aiogram.send_message(message.chat.id, 'В файле нет id_participant или access_hash')

            else:
                await bot_aiogram.send_message(message.chat.id, 'Для авторизации нажмите /start')

        async def check_customer(message, id_customer):
            files = os.listdir('./')
            sessions = filter(lambda x: x.endswith('.session'), files)

            for session in sessions:
                print(session)
                if session == f'{id_customer}.session':
                    print('session exists')
                    return True

            await Form.api_id.set()
            await bot_aiogram.send_message(message.chat.id, "Введите api_id (отменить /cancel)")

        def send_to_db(id_user, api_id, api_hash, phone_number):

            global con

            if not con:
                con = db_connect()

            cur = con.cursor()
            with con:
                cur.execute(f"""CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    id_user INTEGER,
                    api_id INTEGER,
                    api_hash VARCHAR (50),
                    phone_number VARCHAR (25)
                    );"""
                            )
                con.commit()

            with con:
                cur.execute(f"""SELECT * FROM users WHERE id_user={id_user}""")
                r = cur.fetchall()

            if not r:
                with con:
                    new_post = f"""INSERT INTO users (id_user, api_id, api_hash, phone_number) 
                                                    VALUES ({id_user}, {api_id}, '{api_hash}', '{phone_number}');"""
                    cur.execute(new_post)
                    con.commit()
                    print(f'Пользователь {id_user} добавлен в базу')
                    pass

        def get_db(id_customer):

            global con

            if not con:
                con = db_connect()

            cur = con.cursor()

            query = f"""SELECT * FROM users WHERE id_user={id_customer}"""
            with con:
                cur.execute(query)
                r = cur.fetchall()
                print(r)
            return r

        def db_connect():

            con = None

            database = config['DB5new']['database']
            user = config['DB5new']['user']
            password = config['DB5new']['password']
            host = config['DB5new']['host']
            port = config['DB5new']['port']

            try:
                # DATABASE_URL = os.environ['https://data.heroku.com/datastores/762076fd-4f27-4e85-a78f-e3d1973c8ac6#administration']
                # con = psycopg2.connect(DATABASE_URL, sslmode='require')
                con = psycopg2.connect(
                    database=database,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )
            except:
                print('No connect with db')
            return con

        async def clear_invite_history(channel):

            """
            Clear the history in choose channel
            """

            history = await client(GetHistoryRequest(
                peer=channel,
                offset_id=0,
                offset_date=None, add_offset=0,
                limit=3, max_id=0, min_id=0,
                hash=0))
            # if not history.messages:


            for message in history.messages:
                print(f'\n\n{message}\n\n')
                if message.action:
                    print(f'Message_service\n\n')
                    await client.delete_messages(message.peer_id.channel_id, message.id)
                    await client.delete_messages(channel, message.id)
                else:
                    await client.delete_messages(channel, message.id)
            await asyncio.sleep(10)

        async def get_time_start():
            time_start = None
            if self.start_time_scraping_channels:
                if self.start_time_scraping_channels <= self.start_time_listen_channels:
                    time_start = await get_separate_time(self.start_time_scraping_channels)
                else:
                    time_start = await get_separate_time(self.start_time_listen_channels)
            else:
                time_start = await get_separate_time(self.start_time_listen_channels)
            return time_start

        # class ListenChats:

        # @client.on(events.NewMessage(chats=(list_links)))
        # async def normal_handler(event):
        #     print('I,m listening chats ....')
        #     one_message = event.message.to_dict()
        #     print(one_message)
        #
        #     await WriteToDbMessages(client=client, bot_dict={'bot': bot_aiogram, 'chat_id': self.chat_id}).operations_with_each_message(channel=event.chat.title, one_message=one_message)
        #
        #     await client.send_message(int(config['My_channels']['bot_test']), one_message['message'][0:40])
        #     client.run_until_disconnected

        executor.start_polling(dp, skip_updates=True)


InviteBot().main_invitebot()