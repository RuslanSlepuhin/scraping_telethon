import asyncio
import time
import pandas as pd
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
from telethon.tl.functions.channels import GetParticipantRequest, GetParticipantsRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser, InputUser, PeerUser, InputChannel, MessageService, \
    ChannelParticipantsSearch
from db_operations.scraping_db import DataBaseOperations
from links import list_links
from scraping_telegramchats2 import WriteToDbMessages, main
from sites.parsing_sites_runner import ParseSites
from logs.logs import Logs
logs = Logs()

config = configparser.ConfigParser()
config.read("./settings/config.ini")
api_id = config['Ruslan']['api_id']
api_hash = config['Ruslan']['api_hash']
username = '137336064'
username_test = 'test_ruslan'
token = config['Token']['token']
token_test = config['Test2Token']['token']

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

client = TelegramClient(username, int(api_id), api_hash)
client.start()
logs.write_log(f'\n------------------ restart --------------------')

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
        self.current_session = ''
        self.current_customer = None
        self.api_id: int
        self.api_hash: str
        self.phone_number: str
        self.hash_phone: str
        self.code: str
        self.password: ''

    def main_invitebot(self):
        async def connect_with_client(message, id_user):

            global client, hash_phone
            e=None

            client = TelegramClient(str(id_user), int(self.api_id), self.api_hash)

            await client.connect()
            print('Client_is_on_connection')

            if not await client.is_user_authorized():
                try:
                    print('But it is not authorized')
                    phone_code_hash = await client.send_code_request(str(self.phone_number))
                    self.hash_phone = phone_code_hash.phone_code_hash

                except Exception as e:
                    await bot_aiogram.send_message(message.chat.id, str(e))

                if not e:
                    await get_code(message)
            else:
                await bot_aiogram.send_message(message.chat.id, 'Connection is ok')

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
            phone_number = State()
            code = State()
            password = State()

        @dp.message_handler(commands=['start', 'help'])
        async def send_welcome(message: types.Message):

            global phone_number, password, con
            self.chat_id = message.chat.id

            logs.write_log(f'\n------------------ start --------------------')
            # -------- make a parse keyboard for admin ---------------
            parsing_kb = ReplyKeyboardMarkup(resize_keyboard=True)
            parsing_button1 = KeyboardButton('Get news from channels')
            parsing_button2 = KeyboardButton('Subscr.statistics')
            parsing_button3 = KeyboardButton('Digest')
            parsing_button4 = KeyboardButton('Invite people')
            parsing_button5 = KeyboardButton('Get participants')

            parsing_kb.row(parsing_button1, parsing_button2)
            parsing_kb.row(parsing_button3, parsing_button4)
            parsing_kb.add(parsing_button5)

            await bot_aiogram.send_message(message.chat.id, f'Привет, {message.from_user.first_name}!', reply_markup=parsing_kb)
            await bot_aiogram.send_message(137336064, f'Start user {message.from_user.id}')

        @dp.message_handler(commands=['logs', 'log'])
        async def get_logs(message: types.Message):
            path = './logs/logs.txt'
            await send_file_to_user(message, path)


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
            await bot_aiogram.send_message(message.chat.id, "Type your phone number +XXXXXXXXXX (11 numbers with + and country code)\nor cancel for exit")

        #-------------------------- phone number ------------------------------
        # phone_number
        @dp.message_handler(state=Form.phone_number)
        async def process_phone_number(message: types.Message, state: FSMContext):

            global phone_number

            logs.write_log(f"invite_bot_2: Form.phone number")

            async with state.proxy() as data:
                data['phone_number'] = message.text

                logs.write_log(f"invite_bot_2: phone number: {data['phone_number']}")

                await bot_aiogram.send_message(
                    message.chat.id,
                    f"Your api_id: {data['api_id']}\nYour api_hash: {data['api_hash']}\nYour phone number: {data['phone_number']}")

                self.api_id = data['api_id']
                self.api_hash = data['api_hash']
                self.phone_number = data['phone_number']

            DataBaseOperations(None).write_user_without_password(
                id_user=message.from_user.id,
                api_id=int(self.api_id),
                api_hash=self.api_hash,
                phone_number=self.phone_number
            )
            self.password = None

            await connect_with_client(message, id_user=message.from_user.id)


        #-------------------------- code ------------------------------
        # code
        async def get_code(message):

            logs.write_log(f"invite_bot_2: function get_code")

            await Form.code.set()
            await bot_aiogram.send_message(message.chat.id, 'Введите код в формате 12345XXXXX6789, где ХХХХХ - цифры телеграм кода (отмена* /cancel)')

        @dp.message_handler(state=Form.code)
        async def process_phone_number(message: types.Message, state: FSMContext):

            global client, hash_phone, phone_number

            logs.write_log(f"invite_bot_2: Form.code")

            async with state.proxy() as data:
                data['code'] = message.text
                self.code = data['code'][5:10]

                logs.write_log(f"invite_bot_2: Form.code: {data['code']}")

                # ask to get password (always)
                if not self.password:
                    await Form.password.set()
                    await bot_aiogram.send_message(message.chat.id,
                                               "Please type your password 2 step verify if you have\n"
                                               "Type 0 if you don't\n(type /cancel for exit)")
                else:
                    await state.finish()
                    await client_sign_in(message)

        # -------------------------- password ------------------------------
        # password
        @dp.message_handler(state=Form.password)
        async def process_api_hash(message: types.Message, state: FSMContext):
            logs.write_log('invite_bot_2: Form.password')

            async with state.proxy() as data:
                data['password'] = message.text
            self.password = data['password']
            logs.write_log(f"invite_bot_2: Form.password: {data['password']}")
            # DataBaseOperations(None).add_password_to_user(id=self.current_customer[0], password=self.password)

            await state.finish()
            await client_sign_in(message)

            # await Form.next()
            # await bot_aiogram.send_message(message.chat.id, "Введите номер телефона (отменить /cancel)")

        async def client_sign_in(message):
            try:

                if self.password == '0':
                    await client.sign_in(phone=self.phone_number, code=self.code, phone_code_hash=self.hash_phone)
                    await bot_aiogram.send_message(message.chat.id, 'Connection is ok')

                else:
                    await client.sign_in(phone=self.phone_number, code=self.code, password=self.password, phone_code_hash=self.hash_phone)
                    await bot_aiogram.send_message(message.chat.id, 'Connection is ok')

            except Exception as e:
                await bot_aiogram.send_message(message.chat.id, str(e))


        @dp.callback_query_handler()
        async def catch_callback(callback: types.CallbackQuery):
            short_digest = ''
            response = []

            msg = await bot_aiogram.send_message(callback.message.chat.id, 'Please wait a few seconds ...')


            if callback.data == 'show_info_last_records':
                result_dict = {}

                logs.write_log(f"invite_bot_2: Callback: show_info_last_records")

                if not self.current_session:
                    self.current_session = await get_last_session()

                param = f"WHERE session='{self.current_session}'"
                messages = DataBaseOperations(None).get_all_from_db('admin_last_session', param=param)

                for value in self.valid_profession_list:
                    result_dict[value] = 0

                for message in messages:
                    for value in self.valid_profession_list:
                        if value in message[4]:
                            result_dict[value] += 1

                message_to_send = f'<b>Statistics:</b> on session {self.current_session}\n'
                for profession in result_dict:
                    message_to_send += f'{profession}: {result_dict[profession]}\n'

                await bot_aiogram.send_message(callback.message.chat.id, message_to_send, parse_mode='html', reply_markup=self.markup)

                pass

            if callback.data == 'download_excel':

                logs.write_log(f"invite_bot_2: Callback: download_excel")

                pass

            if callback.data == 'send_digest_full':

                logs.write_log(f"invite_bot_2: Callback: send_digest_full")

                # ----------------------- send the messages to tg channels as digest or full --------------------------
                # if not self.current_session:
                #     self.current_session = DataBaseOperations(None).get_all_from_db('current_session', without_sort=True)
                if not self.current_session:
                    self.current_session = await get_last_session()

                await WriteToDbMessages(
                    client,
                    bot_dict={'bot': bot_aiogram, 'chat_id': callback.message.chat.id}).get_last_and_tgpublic_shorts(current_session=self.current_session, shorts=False)  # get from profession's tables and put to tg channels

            if callback.data == 'send_digest_shorts':

                logs.write_log(f"invite_bot_2: Callback: send_digest_shorts")

                # ----------------------- send the messages to tg channels as digest or full --------------------------
                # if not self.current_getting_session:
                #     self.current_getting_session = DataBaseOperations(None).get_all_from_db('current_session', without_sort=True)

                time_start = await get_time_start()
                await WriteToDbMessages(
                    client,
                    bot_dict={'bot': bot_aiogram, 'chat_id': callback.message.chat.id}).get_last_and_tgpublic_shorts(time_start, current_session=self.current_session, shorts=True)

        @dp.message_handler(content_types=['text'])
        async def messages(message):

            global all_participant, marker, file_name, marker_code, client
            channel_to_send = None
            user_to_send = []
            msg = None

            if marker:

                logs.write_log(f"invite_bot_2: content_types: if marker")

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
                if message.text == 'Get participants':

                    logs.write_log(f"invite_bot_2: content_types: Get participants")

                    await bot_aiogram.send_message(
                        message.chat.id,
                        'it is parsing subscribers...',
                        parse_mode='HTML')
                    await main(client, bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id},
                               action='get_participants')

                if message.text == 'Get news from channels':

                    logs.write_log(f"invite_bot_2: content_types: Get news from channels")

                    # if not client.is_connected():  # run client if it was working in invite
                    #     client.start()

# ----------------- make the current session and write it in DB ----------------------
                    self.current_session = datetime.now().strftime("%Y%m%d%H%M%S")
                    DataBaseOperations(None).write_current_session(self.current_session)
                    await bot_aiogram.send_message(message.chat.id, f'Current scraping session {self.current_session}')
                    await asyncio.sleep(1)
                    self.start_time_scraping_channels = datetime.now()
                    print('time_start = ', self.start_time_scraping_channels)
                    await bot_aiogram.send_message(message.chat.id, 'Scraping is starting')
                    await asyncio.sleep(1)

        # -----------------------parsing telegram channels -------------------------------------
                    await bot_aiogram.send_message(
                        message.chat.id,
                        'Парсит телеграм каналы...',
                        parse_mode='HTML')
                    await main(client, bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id})  # run parser tg channels and write to profession's tables
                    await bot_aiogram.send_message(
                        message.chat.id,
                        '...прошло успешно, записано в базу',
                        parse_mode='HTML')
                    await asyncio.sleep(2)

        # ---------------------- parsing the sites. List of them will grow ------------------------
                    await bot_aiogram.send_message(message.chat.id, 'Парсятся сайты...')
                    psites = ParseSites(client=client, bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id})
                    await psites.call_sites()  # paes
                    await bot_aiogram.send_message(message.chat.id, '...прошло успешно, записано в базу. Можно выгрузить кнопкой <b>Digest</b>', parse_mode='html')


                #----------------------- Listening channels at last --------------------------------------

                if message.text == 'Invite people':
                    # if client.is_connected():
                    #     client.disconnect()

                    logs.write_log(f"invite_bot_2: content_types: Invite people")

                    id_customer = message.from_user.id
                    customer = await check_customer(message, id_customer)
                    # con = db_connect()
                    if customer:
                        # get_customer_from_db = get_db(id_customer)
                        get_customer_from_db = DataBaseOperations(None).get_all_from_db(table_name='users', param=f"WHERE id_user={id_customer}", without_sort=True)
                        self.current_customer = get_customer_from_db[0]

                        self.api_id = self.current_customer[2]
                        self.api_hash = self.current_customer[3]
                        self.phone_number = self.current_customer[4]
                        self.password = self.current_customer[5]
                        try:
                            if client.is_connected():
                                await client.disconnect()
                        except:
                            pass
                        await connect_with_client(message, id_customer)


                    # await bot.delete_message(message.chat.id, message.message_id)
                    # response = requests.get(url='https://tg-channel-parse.herokuapp.com/scrape')
                    # await bot.send_message(message.chat.id, response.status_code)
                if message.text == 'Listen to channels':

                    logs.write_log(f"invite_bot_2: content_types: Listen to channels")

                    # await bot.delete_message(message.chat.id, message.message_id)
                    # await bot.send_message(message.chat.id, "Bot is listening TG channels and it will send notifications here")
                    # ListenChats()
                    # await client.run_until_disconnected()
                    await get_subscribers_statistic(message)
                    pass

                if message.text == 'Digest':

                    logs.write_log(f"invite_bot_2: content_types: Digest")

                    self.markup = InlineKeyboardMarkup(row_width=1)
                    but_show = InlineKeyboardButton('Показать сводку по собранным сообщениям', callback_data='show_info_last_records')
                    # but_download_excel = InlineKeyboardButton('Выгрузить excel для внесения правок', callback_data='download_excel')
                    but_send_digest_full = InlineKeyboardButton('Разлить по каналам fulls', callback_data='send_digest_full')
                    # but_send_digest_shorts = InlineKeyboardButton('Разлить по каналам shorts', callback_data='send_digest_shorts')
                    self.markup.add(but_show, but_send_digest_full)
                    # self.markup.add(but_download_excel, but_send_digest_shorts)

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

                if message.text == 'Subscr.statistics':

                    logs.write_log(f"invite_bot_2: content_types: Subscr.statistics")

                    await get_subscribers_statistic(message)
                    # await send_excel(message)
                else:
                    pass
                    # await bot.send_message(message.chat.id, 'Отправьте файл')

        async def get_separate_time(time_in):

            logs.write_log(f"invite_bot_2: function: get_separate_time")

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

            logs.write_log(f"invite_bot_2: function: content_type['document']")

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

            logs.write_log(f"invite_bot_2: unction: check_customer")

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

            logs.write_log(f"invite_bot_2: function: send_to_db")

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
                    phone_number VARCHAR (25),
                    password VARCHAR (100)
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

            logs.write_log(f"invite_bot_2: function: get_db")

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

            logs.write_log(f"invite_bot_2: function: db_connect")

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
            logs.write_log(f"invite_bot_2: function: clear_invite_history")

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

            logs.write_log(f"invite_bot_2: function: get_time_start")

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
        # await logs.write_log(f"invite_bot_2: class: ListenChats")

        #     print('I,m listening chats ....')
        #     one_message = event.message.to_dict()
        #     print(one_message)
        #
        #     await WriteToDbMessages(client=client, bot_dict={'bot': bot_aiogram, 'chat_id': self.chat_id}).operations_with_each_message(channel=event.chat.title, one_message=one_message)
        #
        #     await client.send_message(int(config['My_channels']['bot_test']), one_message['message'][0:40])
        #     client.run_until_disconnected

        async def get_subscribers_statistic(message):

            logs.write_log(f"invite_bot_2: function: get_subscribers_statistic")

            id_user_list = []
            access_hash_list = []
            username_list = []
            first_name_list = []
            last_name_list = []
            join_time_list = []
            is_bot_list = []
            mutual_contact_list = []
            is_admin_list = []
            channel_list = []

            msg = await bot_aiogram.send_message(message.chat.id, f'Followers statistics')

            for channel in ['backend', 'designer', 'frontend', 'devops', 'pm', 'analyst', 'mobile',
                            'qa', 'hr', 'game', 'ba', 'marketing', 'junior', 'sales_manager', 'no_sort',
                            'agregator']:

                channel_name = channel
                channel = config['My_channels'][f'{channel}_channel']

                offset_user = 0  # номер участника, с которого начинается считывание
                limit_user = 100  # максимальное число записей, передаваемых за один раз

                all_participants = []  # список всех участников канала
                filter_user = ChannelParticipantsSearch('')

                # channel = channel[4:]
                try:
                    channel = await client.get_input_entity(int(channel))
                except:
                    try:
                        channel = channel[4:]
                        channel = await client.get_input_entity(int(channel))
                    except Exception as e:
                        await bot_aiogram.send_message(message.chat.id, 'Have the error ', str(e))

                participants = await client(GetParticipantsRequest(
                    channel, filter_user, offset_user, limit_user, hash=0))

                # for participant in participants.users:
                #     print(participant)
                users = {}
                users['users'] = [i for i in participants.users]
                users['date'] = [i for i in participants.participants]


                for i in range(0, len(users['users'])):
                    id_user = users['users'][i].id
                    access_hash = users['users'][i].access_hash
                    username = users['users'][i].username
                    first_name = users['users'][i].first_name
                    last_name = users['users'][i].last_name
                    try:
                        join_time = users['date'][i].date
                    except Exception as e:
                        join_time = None

                    try:
                        is_bot = users['users'][i].bot
                    except Exception:
                        is_bot = None

                    try:
                        mutual_contact = users['users'][i].mutual_contact
                    except Exception:
                        mutual_contact = None

                    is_admin = False
                    try:
                        if users['date'][i].admin_rigths:
                            is_admin = True
                    except Exception:
                        pass

                    print(f"\n{i}")
                    print('id = ', id_user)
                    print('access_hash = ', access_hash)
                    print('username = ', username)
                    print('first_name = ', first_name)
                    print('last_name = ', last_name)
                    print('join_time = ', join_time)
                    print('is_bot = ', is_bot)
                    print('mutual_contact = ', mutual_contact)
                    print('is_admin = ', is_admin)

                    channel_list.append(channel_name)
                    id_user_list.append(id_user)
                    access_hash_list.append(access_hash)
                    username_list.append(username)
                    first_name_list.append(first_name)
                    last_name_list.append(last_name)
                    if join_time:
                        join_time = join_time.strftime('%d-%m-%Y %H:%M:%S')
                    join_time_list.append(join_time)
                    is_bot_list.append(is_bot)
                    mutual_contact_list.append(mutual_contact)
                    is_admin_list.append(is_admin)



                msg = await bot_aiogram.edit_message_text(f'{msg.text}\nThere are <b>{i}</b> subscribers in <b>{channel_name}</b>...\n', msg.chat.id, msg.message_id, parse_mode='html')

                print(f'\nsleep 15 sec.')
                time.sleep(random.randrange(10, 16))

            # compose dict for push to DB
            channel_statistic_dict = {
                'channel': channel_list,
                'id_user': id_user_list,
                'access_hash': access_hash_list,
                'username': username_list,
                'first_name': first_name_list,
                'last_name': last_name_list,
                'join_time': join_time_list,
                'is_bot': is_bot_list,
                'mutual_contact': mutual_contact_list,
            }

            # push to DB
            msg = await bot_aiogram.edit_message_text(
                f'{msg.text}\n\nAll getting statistics is writting to bd, please wait ... ', msg.chat.id,
                msg.message_id, parse_mode='html')

            db = DataBaseOperations(None)
            db.push_followers_statistics(channel_statistic_dict)

            df = pd.DataFrame(
                {
                'channel': channel_list,
                'id_user': id_user_list,
                'access_hash': access_hash_list,
                'username': username_list,
                'first_name': first_name_list,
                'last_name': last_name_list,
                'join_time': join_time_list,
                'is_bot': is_bot_list,
                'mutual_contact': mutual_contact_list,
                'is_admin': is_admin_list,
                }
            )

            df.to_excel(f'./excel/followers_statistics.xlsx', sheet_name='Sheet1')
            print(f'\nExcel was writting')

            await send_file_to_user(message, path='./excel/followers_statistics.xlsx')

        async def send_file_to_user(message, path):

            logs.write_log(f"invite_bot_2: function: send_file_to_user")

            with open(path, 'rb') as file:
                await bot_aiogram.send_document(message.chat.id, file, caption='Please, take it')

        async def get_last_session():

            logs.write_log(f"invite_bot_2: function: get_last_session")

            current_session = DataBaseOperations(None).get_all_from_db(
                table_name='current_session',
                param='ORDER BY id DESC LIMIT 1',
                without_sort=True,
                order=None,
                field='session',
                curs=None
            )
            for value in current_session:
                last_session = value[0]
            return last_session

        executor.start_polling(dp, skip_updates=True)


InviteBot().main_invitebot()