import asyncio
import time
from multiprocessing import Pool
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
from telethon.sync import TelegramClient
from telethon.tl import functions
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import InputUser, InputChannel, ChannelParticipantsSearch, PeerChannel, PeerUser
from db_operations.scraping_db import DataBaseOperations
from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
from progress.progress import ShowProgress
from scraping_telegramchats2 import WriteToDbMessages, main
from sites.parsing_sites_runner import ParseSites
from logs.logs import Logs
from sites.scraping_hh import HHGetInformation
from progress.progress import ShowProgress

logs = Logs()
import settings.os_getenv as settings
config = configparser.ConfigParser()
config.read("./settings/config.ini")

api_id = settings.api_id
api_hash = settings.api_hash
username = settings.username
token = settings.token

logging.basicConfig(level=logging.INFO)
bot_aiogram = Bot(token=token)
storage = MemoryStorage()
dp = Dispatcher(bot_aiogram, storage=storage)

all_participant = []
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
        self.valid_profession_list = ['designer', 'ba', 'game', 'product', 'mobile',
                                      'pm', 'sales_manager', 'analyst', 'frontend',
                                      'marketing', 'devops', 'hr', 'backend', 'qa', 'junior']
        self.markup = None
        # self.api_id = config['Ruslan']['api_id']
        # self.api_hash = config['Ruslan']['api_hash']
        self.api_id = api_id
        self.api_hash = api_hash
        self.current_session = ''
        self.current_customer = None
        self.api_id: int
        self.api_hash: str
        self.phone_number = '' # str
        self.hash_phone = '' # str
        self.code = '' # str
        self.password = '' #str
        self.peerchannel = False
        self.percent = None
        self.message = None
        self.last_id_message_agregator = None
        self.message_for_send = ''
        self.feature = ''
        self.quantity_in_statistics = 0
        self.quantity_entered_to_admin_channel = 0
        self.out_from_admin_channel = 0
        self.quantity_entered_to_shorts = 0
        self.participants_dict = {}
        self.white_admin_list = [1763672666, 556128576, 758905227, 945718420, 5755261667, 5884559465]
        self.marker = False
        self.all_participant = []
        self.channel = None
        self.db = DataBaseOperations(con=None)


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

        class Form_participants(StatesGroup):
            channel = State()

        class Form_params(StatesGroup):
            vacancy = State()

        class Form_delete(StatesGroup):
            date = State()

        class Form(StatesGroup):
            api_id = State()
            api_hash = State()
            phone_number = State()
            code = State()
            password = State()

        class Form_hh(StatesGroup):
            word = State()


        @dp.message_handler(commands=['start'])
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
            await bot_aiogram.send_message(1763672666, f'User {message.from_user.id} has started')

        @dp.message_handler(commands=['help'])
        async def get_logs(message: types.Message):
            await bot_aiogram.send_message(message.chat.id, '/log or /logs - get custom logs (useful for developer\n'
                                                            # '/refresh_pattern - to get the modify pattern from DB\n'
                                                            '/peerchannel - useful for a developer to get id channel\n'
                                                            '/getdata - get channel data\n'
                                                            '/check_parameters - get vacancy\'s parameters\n'
                                                            '/get_participants - ❗️get the channel follower numbers\n'
                                                            '/delete_till - ❗️delete old vacancy from admin DB till date\n'
                                                            '/magic_word - input word and get results from hh.ru\n'
                                                            '/download - ❗️you get excel from admin vacancies with search tags\n'
                                                            '/ambulance - if bot gets accident in hard pushing and you think you loose the shorts\n'
                                                            '/refresh - to rewrite the professions in all vacancies throgh the new filters logic\n'
                                                            '/add_statistics\n\n'
                                                            '❗️- it is admin options')

        @dp.message_handler(commands=['logs', 'log'])
        async def get_logs(message: types.Message):
            path = './logs/logs.txt'
            await send_file_to_user(message, path)

        @dp.message_handler(commands=['refresh'])
        async def refresh_vacancies(message: types.Message):
            await refresh(message)

        @dp.message_handler(commands=['peerchannel'])
        async def get_logs(message: types.Message):
            await bot_aiogram.send_message(message.chat.id, 'Type the channel link and get channel data')
            self.peerchannel = True

        @dp.message_handler(commands=['download'])
        async def download(message: types.Message):
            if message.from_user.id in self.white_admin_list:
                await get_excel_tags_from_admin(message)
            else:
                await bot_aiogram.send_message(message.chat.id, '🚀 Sorry, this options available only for admin')

        @dp.message_handler(commands=['magic_word'])
        async def magic_word(message: types.Message):
            await Form_hh.word.set()
            await bot_aiogram.send_message(message.chat.id, 'Type word for getting more vacancies from hh.ru\nor /cancel')

        # ------------------------ fill search word form ----------------------------------
        # word
        @dp.message_handler(state=Form_hh.word)
        async def process_api_id(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['word'] = message.text
                search_word = message.text
            hh = HHGetInformation(
                search_word=search_word,
                bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id}
            )
            await hh.get_content()

            # pool = Pool(processes=3)
            # result = pool.apply_async(hh.get_content, ())
            # print(result.get(timeout=1))

            await state.finish()

        @dp.message_handler(commands=['delete_till'])
        async def download(message: types.Message):
            if message.from_user.id in self.white_admin_list:
                await Form_delete.date.set()
                await bot_aiogram.send_message(message.chat.id,
                                               'Until what date to delete (exclusive)? Format YYYY-MM-DD\nor /cancel')
            else:
                await bot_aiogram.send_message(message.chat.id, '🚀 Sorry, this options available only for admin')

        # ------------------------ fill date form ----------------------------------
        # date
        @dp.message_handler(state=Form_delete.date)
        async def process_api_id(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['date'] = message.text
                await delete_since(tables_list=['admin_last_session'], param=f"""WHERE DATE(created_at)<'{data['date']}'""")

            await state.finish()

        @dp.message_handler(commands=['ambulance'])
        async def ambulance(message: types.Message):
            short = ''
            shorts_list = []
            with open('./ambulance/ambulance_shorts.txt', 'r') as file:
                shorts = file.read()
            if len(shorts)<4096:
                return await bot_aiogram.send_message(message.chat.id, shorts, parse_mode='html')
            else:
                shorts = shorts.split('\n\n')
                for i in shorts:
                    if len(f"{short}{i}\n\n") < 4096:
                        short += f"{i}\n\n"
                    else:
                        shorts_list.append(short)
                        short = f"{i}\n\n"
                n_count = 1
                for i in shorts_list:
                    await bot_aiogram.send_message(message.chat.id, i, parse_mode='html')
                    print(n_count, 'short ambulance')
                    n_count += 1
                    await asyncio.sleep(random.randrange(1, 3))

        @dp.message_handler(commands=['add_statistics'])
        async def add_statistics(message: types.Message):
            stat_dict = {}

            for channel in self.valid_profession_list:
                try:
                    messages = await get_tg_history_messages(
                        message=message,
                        channel=config['My_channels'][f"{channel}_channel"],
                        limit_msg=100
                    )
                    for vacancy in messages:
                        year = int(vacancy['date'].strftime('%Y'))
                        month = int(vacancy['date'].strftime('%m'))
                        day = int(vacancy['date'].strftime('%d'))

                        if datetime(year, month, day) > datetime(2022, 10, 15):
                            date = vacancy['date'].strftime("%d.%m.%y")
                            if date not in stat_dict:
                                stat_dict[date] = {}
                            if channel not in stat_dict[date]:
                                stat_dict[date][channel] = 0
                            stat_dict[date][channel] += len(re.findall(r"Вакансия: ", vacancy['message']))
                except:
                    print(f'channel {channel} has the accidence')

                await asyncio.sleep(3)

            for i in stat_dict:
                print(f"{i}: {stat_dict[i]}")

            pass


        @dp.message_handler(commands=['get_participants'])
        async def download(message: types.Message):
            await Form_participants.channel.set()
            await bot_aiogram.send_message(message.chat.id, 'Type the channel link\nor /cancel')

        # ------------------------ fill channel form ----------------------------------
        # channel
        @dp.message_handler(state=Form_participants.channel)
        async def process_api_id(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['channel'] = message.text
                wtdm = WriteToDbMessages(
                    client=client,
                    bot_dict={
                        'bot': bot_aiogram,
                        'chat_id': message.chat.id
                    }
                )
                path = await wtdm.dump_all_participants(channel=data['channel'])
            await state.finish()
            if path:
                with open(path, 'rb') as file:
                    await bot_aiogram.send_document(message.chat.id, file, caption='There are all subscribers from channel you order')
            else:
                await bot_aiogram.send_message(message.chat.id, 'Sorry, No file')

        @dp.message_handler(commands=['check_parameters'])
        async def check_parameters(message: types.Message):
            await Form_params.vacancy.set()
            await bot_aiogram.send_message(message.chat.id, 'Forward the vacancy now for checking outputs from pattern filter\nor /cancel')

        # ------------------------ fill parameters form ----------------------------------
        # parameters
        @dp.message_handler(state=Form_params.vacancy)
        async def process_api_id(message: types.Message, state: FSMContext):
            async with state.proxy() as data:
                data['vacancy'] = message.text
                vacancy = data['vacancy']
            await state.finish()
            if '\n' in vacancy:
                title = vacancy.split('\n', 1)[0]
                body = vacancy.split('\n', 1)[1]
            else:
                title = vacancy
                body = ''
            # dict_response = AlexSort2809().sort_by_profession_by_Alex(
            #     body=body,
            #     title=title,
            #     only_profession=True
            # )

            dict_response = AlexSort2809().sort_by_profession_by_Alex(
                body=body,
                title=title,
                check_contacts=False,
                check_vacancy=False
            )

            profession = dict_response['profession']

            message_for_send = "<b>PATTERN'S RESULTS:</b>\n\n"
            message_for_send += f"<b>PROFESSIONS:</b>\n{profession['profession']}\n" \
                                f"<b>MA:</b>\n{profession['tag']}\n" \
                                f"<b>MEX:</b>\n{profession['anti_tag']}"

            await bot_aiogram.send_message(message.chat.id, message_for_send, parse_mode='html')

        @dp.message_handler(commands=['refresh_pattern'])
        async def get_logs(message: types.Message):
            path = './patterns/pattern_test.py'
            await refresh_pattern(path)

        @dp.message_handler(commands=['id'])
        async def get_logs(message: types.Message):
            # 311614392
            # 533794904
            # 857262125
            # 1359259501
            # 537301906
            for i in [311614392, 533794904, 857262125, 1359259501, 537301906]:
                try:
                    # peer = PeerUser(i)
                    data = await client.get_entity(i)
                    await bot_aiogram.send_message(message.chat.id, str(data))
                    await asyncio.sleep(6)
                except Exception as e:
                    await bot_aiogram.send_message(message.chat.id, f"{i}: {str(e)}")
                    await asyncio.sleep(6)

        @dp.message_handler(commands=['restore'])
        async def get_logs(message: types.Message):
            profession_list = {}
            results_dict = {}

            for profession in self.valid_profession_list:
                channel = config['My_channels'][f'{profession}_channel']
                all_message = await get_tg_history_messages(message, channel)
                if all_message:
                    for vacancy in all_message:
                        results_dict['title'] = vacancy['message'].partition(f'\n')[0]
                        results_dict['body'] = vacancy['message'].replace(results_dict['title'], '').replace(f'\n\n', f'\n')
                        results_dict['time_of_public'] = (vacancy['date'] + timedelta(hours=3))
                        results_dict['created_at'] = results_dict['time_of_public']
                        results_dict['chat_name'] = ''
                        results_dict['vacancy'] = ''
                        results_dict['vacancy_url'] = ''
                        results_dict['company'] = ''
                        results_dict['english'] = ''
                        results_dict['relocation'] = ''
                        results_dict['job_type'] = ''
                        results_dict['city'] = ''
                        results_dict['salary'] = ''
                        results_dict['experience'] = ''
                        results_dict['contacts'] = ''
                        results_dict['session'] = '20221114114824'

                        is_exist = DataBaseOperations(None).get_all_from_db(
                            table_name=profession,
                            param=f"""WHERE title='{results_dict['title']}' AND body='{results_dict['body']}'"""
                        )
                        pass
                        if not is_exist:
                            print('NOT IN DB')
                            print('profession: ', profession)

                            profession_list['profession'] = [profession,]
                            DataBaseOperations(None).push_to_bd(
                                results_dict=results_dict,
                                profession_list=profession_list
                            )
                        else:
                            print('*** IN DB EXISTS ***')
                            print('profession: ', profession)

                            profession_list['profession'] = [profession, ]
                            DataBaseOperations(None).push_to_bd(
                                results_dict=results_dict,
                                profession_list=profession_list
                            )

            # vacancies loop
            # get one channel, get vacancies
            # check prof. db
            # if not exists do write

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
                    # await client.sign_in(phone=self.phone_number, password=self.password, code=self.code, phone_code_hash=self.hash_phone)
                    await client.sign_in(password=self.password, code=self.code)

                    await bot_aiogram.send_message(message.chat.id, 'Connection is ok')

            except Exception as e:
                await bot_aiogram.send_message(message.chat.id, str(e))


        @dp.callback_query_handler()
        async def catch_callback(callback: types.CallbackQuery):
            short_digest = ''
            response = []

            if callback.data == 'personal':
                await invite_users(
                    message=callback.message,
                    channel=self.channel,
                )

            if callback.data == 'group':
                await invite_set_users(
                    message=callback.message,
                    channel=self.channel,
                )

            if callback.data == 'consolidated_table':
                await output_consolidated_table(callback.message)

            if callback.data == 'go_by_admin': # next step if callback.data[2:] in self.valid_profession_list:
                # make the keyboard with all professions
                self.markup = await compose_inline_keyboard(prefix='admin')
                await bot_aiogram.send_message(callback.message.chat.id, 'choose the channel for vacancy checking', reply_markup=self.markup)

            if callback.data[0:5] == 'admin':

                # a='581'
                # message_attempt = "Дайджест\n\n"
                # message_attempt += hlink(title="Подробнее", url="https://t.me/agrerator_channel_fake/251")
                # message_attempt += '\n'
                # message_attempt += 'Еще немного текста\n'
                # message_attempt += hlink(title="Подробнее", url=f"{config['My_channels']['agregator_link']}/{a}")
                # print(message_attempt)
                # await write_to_logs_error(f'Rigth:\n{message_attempt}')
                # await bot_aiogram.send_message(callback.message.chat.id, message_attempt, parse_mode='html', disable_web_page_preview=True)

                pass

                try:
                    DataBaseOperations(None).delete_table('admin_temporary')
                except Exception as e:
                    print(e)
                    # await bot_aiogram.send_message(callback.message.chat.id, f'The attempt to delete admin_temporary is wrong\n{str(e)}')
                    # await asyncio.sleep(random.randrange(2, 3))

                # delete messages for channel will be clean to take new messages
                all_messages = await get_tg_history_messages(callback.message)
                for i in all_messages:
                    await client.delete_messages(PeerChannel(int(config['My_channels']['admin_channel'])), i['id'])

                # getting the last message_id
                last_admin_channel_id = await get_last_admin_channel_id(callback.message)


                profession = callback.data.split('/')[1]
                param = f"WHERE profession LIKE '%{profession}' OR profession LIKE '%{profession},%'"
                response = DataBaseOperations(None).get_all_from_db(table_name='admin_last_session', param=param, without_sort=True)

                self.quantity_in_statistics = len(response)

                if response:
                    self.percent = 0
                    length = len(response)
                    n = 0
                    self.message = await bot_aiogram.send_message(callback.message.chat.id, f'progress {self.percent}%')
                    await asyncio.sleep(random.randrange(2, 3))

                    self.quantity_entered_to_admin_channel = 0
                    for vacancy in response:
                        composed_message_dict = await compose_message(message=vacancy, one_profession=profession, full=True)
                        composed_message_dict['id_admin_channel'] = ''
                        composed_message_dict['id_admin_channel'] = last_admin_channel_id + 1
                        composed_message_dict['it_was_sending_to_agregator'] = ''
                        composed_message_dict['it_was_sending_to_agregator'] = vacancy[19]

                    # it needs the checking. It can be in DB. Do it after is better. At the moment writing ti admin las session. Does not matter to write it if it exists in DB

                        try:
                            # text = f"{vacancy[2]}\n{vacancy[3]}"
                            text = composed_message_dict['composed_message']
                            # text = re.sub(r'\<[A-Za-z\/=\"\-\>\s\._\<]{1,}\>', " ", text)
                            if len(text) > 4096:
                                text = text[:4093] + '...'
                            await bot_aiogram.send_message(config['My_channels']['admin_channel'], text, parse_mode='html', disable_web_page_preview=True)
                            last_admin_channel_id += 1
                            DataBaseOperations(None).push_to_admin_temporary(composed_message_dict)
                            self.quantity_entered_to_admin_channel += 1
                            await asyncio.sleep(random.randrange(2, 3))
                        except Exception as e:
                            await bot_aiogram.send_message(callback.message.chat.id, f"It hasn't been pushed to admin_channel : {e}")
                            await write_to_logs_error(
                                f"It hasn't been pushed to admin_channel\n{e}\n------------\n{vacancy[2]+vacancy[3]}\n-------------\n\n")
                            await asyncio.sleep(random.randrange(2, 3))
                        # write to temporary DB (admin_temporary) id_admin_message and id in db admin_last_session

                        n += 1
                        await show_progress(callback.message, n, length)

                        # to say the customer about finish
                    markup = InlineKeyboardMarkup()
                    push_full = InlineKeyboardButton(f'PUSH full to {profession.title()}', callback_data=f'PUSH full to {profession}')
                    button_shorts = InlineKeyboardButton(f'PUSH shorts to {profession.title()}', callback_data=f'PUSH shorts to {profession}')

                    markup.row(push_full, button_shorts)
                    await bot_aiogram.send_message(callback.message.chat.id, f'{profession.title()} in the Admin channel\n'
                                                                             f'When you will ready, press button PUSH',
                                                   reply_markup=markup)
                    await asyncio.sleep(random.randrange(2, 3))
                else:
                    await bot_aiogram.send_message(callback.message.chat.id, f'There are have not any vacancies in {profession}\n'
                                                                             f'Please choose others', reply_markup=self.markup)
                    await asyncio.sleep(random.randrange(2, 3))

            if callback.data == 'one_day_statistics':
                self.feature = 'one_day_statistics'
                await bot_aiogram.send_message(callback.message.chat.id, "Type the date (format YYYY-MM-DD)")
                # today_statistics = f"Statistics today {datetime.now().strftime('%Y-%m-%d')}:\n\n"
                # print(datetime.now().strftime('%Y-%m-%d'))

            if callback.data == 'hard_push':
                button_all_vacancies = InlineKeyboardButton('all', callback_data='all')
                button_each_vacancy = InlineKeyboardButton('choose profession', callback_data='each_profession')
                markup = InlineKeyboardMarkup()
                markup.row(button_all_vacancies, button_each_vacancy)
                await bot_aiogram.send_message(callback.message.chat.id, "It's the pushing without admin", reply_markup=markup)

            elif 'PUSH' in callback.data:
                profession_list = {}
                results_dict = {}

                self.percent = 0
                self.message = await bot_aiogram.send_message(callback.message.chat.id, f'progress {self.percent}%')
                await asyncio.sleep(random.randrange(1, 2))


                # self.last_id_message_agregator = await get_id_agregator()
                # to get last agregator id
                self.last_id_message_agregator = await get_last_admin_channel_id(
                    message=callback.message,
                    channel=config['My_channels']['agregator_channel']
                )

                profession = callback.data.split(' ')[-1]
                history_messages = await get_tg_history_messages(callback.message)

                self.out_from_admin_channel = len(history_messages)

                # self.message_for_send = f'<b>Дайджест вакансий для {profession} за {datetime.now().strftime("%d.%m.%Y")}:</b>\n\n'
                message_for_send = f'<i>Функционал дайджеста находится в состоянии альфа-тестирования, приносим свои ' \
                                   f'извинения, мы работаем над тем чтобы вы получали информацию максимально ' \
                                   f'качественную и в сжатые сроки</i>\n\n' \
                                   f'<b>Дайджест вакансий для {profession} за {datetime.now().strftime("%d.%m.%Y")}:</b>\n\n'
                length = len(history_messages)
                n=0

                self.quantity_entered_to_shorts = 0
                for vacancy in history_messages:
                    print('\npush vacancy\n')

                    response = DataBaseOperations(None).get_all_from_db('admin_temporary',
                                                                        param=f"WHERE id_admin_channel='{vacancy['id']}'",
                                                                        without_sort=True)
                    if response:


                        id_admin_last_session_table = int(response[0][2])
                        vacancy_from_admin = DataBaseOperations(None).get_all_from_db('admin_last_session',
                                                                                      param=f"WHERE id={id_admin_last_session_table}",
                                                                                      without_sort=True)
                        # if vacancy has sent in agregator already, it doesn't push again. And remove profess from profs or drop vacancy if there is profession alone
                        await push_vacancies_to_agregator_from_admin(
                            message=callback.message,
                            vacancy=vacancy,
                            vacancy_from_admin=vacancy_from_admin,
                            response=response,
                            profession=profession,
                            id_admin_last_session_table=id_admin_last_session_table
                        )

                        pass

                        if "full" in callback.data:
                        # ---------- the unique operation block for fulls = pushing to prof channel full message ----------
                            print('push vacancy in channel\n')
                            print(f"\n{vacancy['message'][0:40]}")
                            # response_dict = await compose_for_push_to_db(response, profession)
                            # if False in response_dict.values():
                            await bot_aiogram.send_message(int(config['My_channels'][f'{profession}_channel']), vacancy['message'])
                            await asyncio.sleep(random.randrange(2, 3))
                            # else:
                            #     print('It has been got True from db')
                        # ------------------- end of  pushing to prof channel full message -----------------
                        elif "shorts" in callback.data:
                            vacancy_from_admin = DataBaseOperations(None).get_all_from_db('admin_last_session',
                                                                                          param=f"WHERE id={id_admin_last_session_table}",
                                                                                          without_sort=True)
                            composed_message_dict = await compose_message(vacancy_from_admin[0], profession)
                            message_for_send += f"{composed_message_dict['composed_message']}\n"
                            self.quantity_entered_to_shorts += 1

                            await compose_data_and_push_to_db(
                                vacancy_from_admin=vacancy_from_admin,
                                profession=profession
                            )
                            prof_list = vacancy_from_admin[0][4].split(',')

                            profession_list['profession'] = [profession,]

                            # results_dict['chat_name'] = vacancy_from_admin[0][1]
                            # results_dict['title'] = vacancy_from_admin[0][2]
                            # results_dict['body'] = vacancy_from_admin[0][3]
                            # results_dict['profession'] = vacancy_from_admin[0][4]
                            # results_dict['vacancy'] = vacancy_from_admin[0][5]
                            # results_dict['vacancy_url'] = vacancy_from_admin[0][6]
                            # results_dict['company'] = vacancy_from_admin[0][7]
                            # results_dict['english'] = vacancy_from_admin[0][8]
                            # results_dict['relocation'] = vacancy_from_admin[0][9]
                            # results_dict['job_type'] = vacancy_from_admin[0][10]
                            # results_dict['city'] = vacancy_from_admin[0][11]
                            # results_dict['salary'] = vacancy_from_admin[0][12]
                            # results_dict['experience'] = vacancy_from_admin[0][13]
                            # results_dict['contacts'] = vacancy_from_admin[0][14]
                            # results_dict['time_of_public'] = vacancy_from_admin[0][15]
                            # results_dict['created_at'] = vacancy_from_admin[0][16]
                            # results_dict['agregator_link'] = vacancy_from_admin[0][17]
                            # results_dict['session'] = vacancy_from_admin[0][18]

                            await update_vacancy_admin_last_session(
                                results_dict=None,
                                profession=profession,
                                prof_list=prof_list,
                                id_admin_last_session_table=id_admin_last_session_table,
                                update_profession=True,
                                update_id_agregator=False
                            )
                        # response_dict = DataBaseOperations(None).push_to_bd(results_dict, profession_list, self.last_id_message_agregator)
                        await delete_used_vacancy_from_admin_temporary(vacancy, id_admin_last_session_table)
                    else:
                        await bot_aiogram.send_message(callback.message.chat.id, 'There is not response')

                    n += 1
                    await show_progress(callback.message, n, length)

                if "shorts" in callback.data:
                    vacancies_list = await cut_message_for_send(message_for_send)
                    for short in vacancies_list:
                        try:
                            await write_to_logs_error(f"Results:\n{short}\n")
                            # await bot_aiogram.send_message(int(config['My_channels'][f'{profession}_channel']), short, parse_mode='html', disable_web_page_preview=True)
                            await bot_aiogram.send_message(-1001671844820, short, parse_mode='html', disable_web_page_preview=True)

                        except Exception as e:
                            await bot_aiogram.send_message(callback.message.chat.id, str(e))
                            pass

                await delete_and_change_waste_vacancy(callback.message, last_id_message_agregator=self.last_id_message_agregator, profession=profession)

                # history_messages = await get_tg_history_messages(callback.message)
                # if not history_messages:

                DataBaseOperations(None).delete_table(
                    table_name='admin_temporary'
                )
                await bot_aiogram.send_message(callback.message.chat.id, f'<b>Done!</b>\n'
                                                                         f'- in to statistics: {self.quantity_in_statistics}\n'
                                                                         f'- in to admin {self.quantity_entered_to_admin_channel}\n'
                                                                         f'- out from admin {self.out_from_admin_channel}\n'
                                                                         f'- in to shorts {self.quantity_entered_to_shorts}',
                                               parse_mode='html')

            if callback.data == 'all':
                await hard_post(callback.message)

            if callback.data == 'each_profession':
                markup = await compose_inline_keyboard(prefix='each')
                await bot_aiogram.send_message(callback.message.chat.id, "Choose profession", reply_markup=markup, parse_mode='html')

            elif 'each' in callback.data:
                channel = callback.data.split('/')[1]
                await hard_post(callback.message, channels=channel)


            if callback.data == 'choose_one_channel':  # compose keyboard for each profession

                self.markup = await compose_inline_keyboard(prefix='//')
                await bot_aiogram.send_message(callback.message.chat.id, 'Choose the channel', reply_markup=self.markup)
                pass

            if callback.data[2:] in self.valid_profession_list:
                logs.write_log(f"invite_bot_2: Callback: one_of_profession {callback.data}")
                if not self.current_session:
                    self.current_session = await get_last_session()
                await WriteToDbMessages(
                    client,
                    bot_dict={'bot': bot_aiogram,
                              'chat_id': callback.message.chat.id}).get_last_and_tgpublic_shorts(
                    current_session=self.current_session,
                    shorts=False, fulls_all=True, one_profession=callback.data)  # get from profession's tables and put to tg channels
                pass

            if callback.data == 'show_info_last_records':
                """
                Show the parsing statistics
                """
                msg = await bot_aiogram.send_message(callback.message.chat.id, 'Please wait a few seconds ...')

                result_dict = {}

                logs.write_log(f"invite_bot_2: Callback: show_info_last_records")

                # --------- compose data from last session --------------
                result_dict['last_session'] = {}
                result_dict['all'] = {}

                if not self.current_session:
                    self.current_session = await get_last_session()

                param = f"WHERE session='{self.current_session}'"
                messages = DataBaseOperations(None).get_all_from_db('admin_last_session', param=param)

                for value in self.valid_profession_list:
                    result_dict['last_session'][value] = 0

                for message in messages:
                    professions = message[4].split(',')
                    for pro in professions:
                        pro = pro.strip()
                        if pro in self.valid_profession_list:
                            result_dict['last_session'][pro] += 1

                # --------- compose data from all unapproved sessions --------------
                messages = DataBaseOperations(None).get_all_from_db('admin_last_session')

                for value in self.valid_profession_list:
                    result_dict['all'][value] = 0

                for message in messages:
                    professions = message[4].split(',')
                    for pro in professions:
                        pro = pro.strip()
                        if pro in self.valid_profession_list:
                            result_dict['all'][pro] += 1

                # ------------ compose message to output ------------------

                message_to_send = f'<b><u>Statistics:</u></b>\n\nLast session ({self.current_session}) / All unapproved:\n'
                for i in result_dict['last_session']:
                    message_to_send += f"{i}: {result_dict['last_session'][i]}/{result_dict['all'][i]}\n"

                message_to_send += f"<b>Total: {sum(result_dict['last_session'].values())}/{sum(result_dict['all'].values())}</b>"

                await bot_aiogram.send_message(callback.message.chat.id, message_to_send, parse_mode='html', reply_markup=self.markup)

                pass

            if callback.data == 'download_excel':
                "function doesn't work"
                logs.write_log(f"invite_bot_2: Callback: download_excel")
                pass

            if callback.data == 'send_digest_full_all':
                logs.write_log(f"invite_bot_2: Callback: send_digest_full_aalll")
                if not self.current_session:
                    self.current_session = await get_last_session()
                await WriteToDbMessages(
                    client,
                    bot_dict={'bot': bot_aiogram,
                              'chat_id': callback.message.chat.id}).get_last_and_tgpublic_shorts(
                    current_session=self.current_session,
                    shorts=False, fulls_all=True)  # get from profession's tables and put to tg channels

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

            global all_participant, file_name, marker_code, client
            channel_to_send = None
            user_to_send = []
            msg = None
            if self.peerchannel:
                data = await client.get_entity(message.text)
                await bot_aiogram.send_message(message.chat.id, str(data))
                self.peerchannel = False

            if self.feature == 'one_day_statistics':
                one_day_statistics = f'<b>Statistics {message.text}</b>\n\n'
                counter = 0
                try:
                    for prof in self.valid_profession_list:
                        response = DataBaseOperations(con).get_all_from_db(
                            table_name=prof,
                            param=f"""WHERE DATE(created_at)='{message.text}'"""
                        )
                        one_day_statistics += f"{prof}: {len(response)} vacancies\n"
                        counter += len(response)
                    one_day_statistics += f"____________\nSumm: {counter}"
                    await bot_aiogram.send_message(message.chat.id, one_day_statistics, parse_mode='html')
                    self.feature = ''

                except Exception as e:
                    await bot_aiogram.send_message(message.chat.id, 'Type the correct date')


            if self.marker:
                self.channel = message.text
                markup = InlineKeyboardMarkup()
                button1 = InlineKeyboardButton('group', callback_data='group')
                button2 = InlineKeyboardButton('personal', callback_data='personal')
                markup.row(button1, button2)
                await bot_aiogram.send_message(message.chat.id, 'group or personal', reply_markup=markup)

                # await invite_users(
                #     message=message,
                #     channel=message.text,
                #     all_participant=all_participant
                # )

                # await invite_set_users(
                #     message=message,
                #     channel=message.text,
                #     all_participant=all_participant
                # )
#

            else:
                if message.text == 'Get participants':

                    if message.text == 'Get participants':

                        if message.from_user.id in self.white_admin_list:
                            logs.write_log(f"invite_bot_2: content_types: Get participants")

                            await bot_aiogram.send_message(
                                message.chat.id,
                                'it is parsing subscribers...',
                                parse_mode='HTML')
                            await main(client, bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id},
                                       action='get_participants')
                        else:
                            await bot_aiogram.send_message(message.chat.id,
                                                           '🚀 Sorry, this options available only for admin')

                if message.text == 'Get news from channels':

                    logs.write_log(f"invite_bot_2: content_types: Get news from channels")

# ----------------- make the current session and write it in DB ----------------------
                    self.current_session = datetime.now().strftime("%Y%m%d%H%M%S")
                    DataBaseOperations(None).write_current_session(self.current_session)
                    await bot_aiogram.send_message(message.chat.id, f'Session is {self.current_session}')
                    await asyncio.sleep(1)
                    self.start_time_scraping_channels = datetime.now()
                    print('time_start = ', self.start_time_scraping_channels)
                    # await bot_aiogram.send_message(message.chat.id, 'Scraping is starting')
                    await asyncio.sleep(1)

        # -----------------------parsing telegram channels -------------------------------------
                    await bot_aiogram.send_message(
                        message.chat.id,
                        'Bot is parsing the telegram channels...',
                        parse_mode='HTML')
                    await main(client, bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id})  # run parser tg channels and write to profession's tables
                    await bot_aiogram.send_message(
                        message.chat.id,
                        '...it has been successfully',
                        parse_mode='HTML')
                    await asyncio.sleep(2)

        # ---------------------- parsing the sites. List of them will grow ------------------------
                    await bot_aiogram.send_message(message.chat.id, 'Bot is parsing the sites...')
                    psites = ParseSites(client=client, bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id})
                    await psites.call_sites()
                    await bot_aiogram.send_message(message.chat.id, '...it has been successfully. Press <b>Digest</b> for the next step', parse_mode='html')


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
                        get_customer_from_db = DataBaseOperations(None).get_all_from_db(table_name='users', param=f"WHERE id_user='{id_customer}'", without_sort=True)
                        if not get_customer_from_db:
                            await Form.api_id.set()
                            return await bot_aiogram.send_message(message.chat.id, "Введите api_id (отменить /cancel)")

                        self.current_customer = get_customer_from_db[0]
                        self.api_id = int(self.current_customer[2])
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
                    but_show = InlineKeyboardButton('Unsorted vacancies (new vacancies)',
                                                    callback_data='show_info_last_records')
                    # but_send_digest_full = InlineKeyboardButton('Разлить fulls посл сессию',
                    #                                             callback_data='send_digest_full')
                    # but_send_digest_full_all = InlineKeyboardButton('Разлить fulls всё',
                    #                                                 callback_data='send_digest_full_all')
                    # but_separate_channel = InlineKeyboardButton('Залить в 1 канал',
                    #                                             callback_data='choose_one_channel')
                    but_do_by_admin = InlineKeyboardButton('ADMIN AREA👀✈️',
                                                                callback_data='go_by_admin')
                    but_stat_today = InlineKeyboardButton('One day statistics', callback_data='one_day_statistics')
                    but_excel_all_statistics = InlineKeyboardButton('Whole posted vacancies (EXCEL)', callback_data='consolidated_table')
                    but_hard_push = InlineKeyboardButton('HARD PUSHING 🧨🧨🧨', callback_data='hard_push')

                    # self.markup.row(but_show, but_send_digest_full)
                    # self.markup.row(but_send_digest_full_all, but_separate_channel)
                    self.markup.add(but_show)
                    self.markup.add(but_stat_today)
                    self.markup.add(but_excel_all_statistics)
                    self.markup.add(but_hard_push)
                    self.markup.add(but_do_by_admin)

                    time_start = await get_time_start()
                    await bot_aiogram.send_message(
                        message.chat.id,
                        f"Выберите действие со полученными и не распределенными вакансиями:", reply_markup=self.markup)
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

            global all_participant, file_name

            logs.write_log(f"invite_bot_2: function: content_type['document']")

            if client.is_connected():

                self.all_participant = []
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

                if 'id_participant' in excel_data_df.columns and 'access_hash' and 'status' in excel_data_df.columns:

                    excel_dict = {
                        'id_participant': excel_data_df['id_participant'].tolist(),
                        'access_hash': excel_data_df['access_hash'].tolist(),
                        'user': excel_data_df['username'].tolist(),
                        'status': excel_data_df['status'].tolist()
                    }
                    print(excel_dict)
                    self.participants_dict = excel_dict
                    n = 0
                    while n<len(excel_dict['id_participant']):
                        self.all_participant.append([int(excel_dict['id_participant'][n]), int(excel_dict['access_hash'][n]), excel_dict['user'][n], excel_dict['status'][n]])
                        n += 1

                    for iii in self.all_participant:
                        for jjj in iii:
                            print(jjj, type(jjj))

                    print('all_participant = ', self.all_participant)

                    await bot_aiogram.send_message(
                        message.chat.id,
                        f'Получен файл с {len(self.all_participant)} пользователями\n'
                        f'Введите url канала в формате https//t.me/<имя канала> без @:\n'
                    )

                    self.marker = True
                else:
                    await bot_aiogram.send_message(message.chat.id, 'В файле нет id_participant или access_hash')

            else:
                await bot_aiogram.send_message(message.chat.id, 'Для авторизации нажмите /start')


        async def invite_users(message, channel):
            logs.write_log(f"invite_bot_2: invite_users: if marker")
            msg = None
            channel_short_name = f"@{channel.split('/')[-1]}"
            # channel = message.text

        # receiving the channel entity
            try:
                channel = await client.get_entity(channel)
                channel_to_send = InputChannel(channel.id, channel.access_hash)  # был InputPeerChannel
            except Exception as e:
                # await bot_aiogram.send_message(message.chat.id, f'{e}\nУкажате канал в формате https//t.me/<имя канала> (без @)\n'
                #                                         f'Обратите внимание на то, что <b>и Вы и этот бот</b> в этом канале должны быть <b>администраторами</b>', parse_mode='html')
                await bot_aiogram.send_message(message.chat.id, 'Это сообщение вместо того, по которому была ошибка')
                return False

        #
            try:
                await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: Инвайт запущен',
                                               parse_mode='html')
                n = 0
                numbers_invite = 0
                numbers_failure = 0
                was_subscribe = 0

                self.participants_dict['status'] = []
                # ---------------------------- отправлять по одному инвайту---------------------------------

                print(f'\nLEN ALL_PARTICIPANTS IS {len(self.all_participant)}\n')

                sp = ShowProgress({'bot': bot_aiogram, 'chat_id': message.chat.id})
                current_step = 0
                length = len(self.all_participant)
                msg_2 = await bot_aiogram.send_message(message.chat.id, 'process 0%')

                for user in self.all_participant:
                    index = self.all_participant.index(user)

                    text = f"id:  {user[0]} hash {user[1]} username {user[2]} status {user[3]}\n"
                    await add_log_inviter(text)

                    print('id: ', user[0], 'hash', user[1], 'username', user[2], 'status', user[3])
                    id_user = int(user[0])
                    access_hash_user = int(user[1])
                    username = user[2]
                    status = user[3]

                    # -----------------------------------------------------try---------------------------------------------------------------
                    try:
                        user_channel_status = await bot_aiogram.get_chat_member(chat_id=channel_short_name,
                                                                                user_id=id_user)
                        if user_channel_status.status != types.ChatMemberStatus.LEFT:
                            if msg:
                                await msg.delete()
                                msg = None
                            # msg = await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: пользователь с id={id_user} уже подписан', parse_mode='html')
                            print('Пользователь уже подписан')
                            text = f"Пользователь уже подписан\n"
                            await add_log_inviter(text)
                            self.all_participant[index][-1] = 'user already subscribed'

                            # self.participants_dict['status'].append('уже подписан')
                            was_subscribe += 1
                            user_exists = True
                        else:
                            print('Пользователь новый')
                            text = f"Пользователь новый\n"
                            await add_log_inviter(text)

                            user_exists = False

                    except Exception as e:
                        print('Пользователь новый')
                        text = f"Пользователь новый\n"
                        await add_log_inviter(text)

                        user_exists = False
                        if msg:
                            await msg.delete()
                            msg = None
                        # await bot_aiogram.send_message(message.chat.id, f"813: {str(e)}")
                        print(f"#813: if username != None {str(e)}")
                        text = f"#813: if username != None {str(e)}\n"
                        await add_log_inviter(text)

                    # ----------------------------------------------------end---------------------------------------------------------------
                    if not user_exists and status.lower() == 'new':
                        if username != 'None':
                            # -----------------------------------------------------try---------------------------------------------------------------
                            try:
                                user_to_send = [await client.get_input_entity(username)]
                            except Exception as e:
                                try:
                                    await asyncio.sleep(5)
                                    user_to_send = [await client.get_entity(username)]
                                except Exception as e:
                                    try:
                                        user_to_send = [InputUser(id_user, access_hash_user)]
                                    except Exception as e:
                                        # await bot_aiogram.send_message(message.chat.id, f"#824: if username != None {str(e)}")
                                        print(f"#824: if username != None {str(e)}")
                                        text = f"#824: if username != None {str(e)}\n"
                                        await add_log_inviter(text)

                        # ----------------------------------------------------end---------------------------------------------------------------
                        else:
                            # -----------------------------------------------------try---------------------------------------------------------------
                            try:
                                user_to_send = [InputUser(id_user, access_hash_user)]  # (PeerUser(id_user))
                            except Exception as e:
                                # await bot_aiogram.send_message(message.chat.id, f"#831: if username = None {str(e)}")
                                print(f"#831: if username = None {str(e)}")
                                text = f"#831: if username = None {str(e)}\n"
                                await add_log_inviter(text)

                        # ----------------------------------------------------end---------------------------------------------------------------
                        # -----------------------------------------------------try---------------------------------------------------------------
                        if msg:
                            await msg.delete()
                            msg = None
                        try:
                            # client.invoke(InviteToChannelRequest(channel_to_send,  [user_to_send]))
                            # await client(InviteToChannelRequest(channel_to_send, user_to_send))  #work!!!!!
                            await client(functions.channels.InviteToChannelRequest(channel_to_send, user_to_send))
                            # self.participants_dict['status'].append('инвайт прошел')
                            self.all_participant[index][-1] = 'invite +'


                            msg = await bot_aiogram.send_message(message.chat.id,
                                                                 f'<b>{channel_short_name}:</b> {user[0]} заинвайлся успешно\n'
                                                                 f'({numbers_invite + 1} инвайтов)',
                                                                 parse_mode='html')
                            print(f'{channel_short_name}: {user[0]} заинвайлся успешно\n'
                                  f'({numbers_invite + 1} инвайтов)\n\n')
                            text = f"{channel_short_name}: {user[0]} заинвайлся успешно\n({numbers_invite + 1} инвайтов)\n\n\n"
                            await add_log_inviter(text)
                            await asyncio.sleep(random.randrange(15, 20))

                            numbers_invite += 1

                        except Exception as e:
                            if re.findall(r'seconds is required (caused by InviteToChannelRequest)', str(e)) or \
                                    str(e) == "Too many requests (caused by InviteToChannelRequest)" or re.findall(
                                r'seconds is required', str(e)) or 'maximum number of users has been exceeded' in str(e):
                                await bot_aiogram.send_message(message.chat.id, str(e))
                                print(str(e))
                                break
                            else:
                                if msg:
                                    await msg.delete()
                                    msg = None
                                # -----------------------------------------------------try---------------------------------------------------------------
                                try:
                                    # await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: Для пользователя id={user[0]}\n{str(e)}', parse_mode='html')
                                    # self.participants_dict['status'].append(str(e))
                                    self.all_participant[index][-1] = str(e)

                                    print(f'{channel_short_name}: Для пользователя id={user[0]}\n{str(e)}\n\n')
                                    text = f"{channel_short_name}: Для пользователя id={user[0]}\n{str(e)}\n\n\n"
                                    await add_log_inviter(text)

                                except Exception:
                                    print('exception: #861')
                                    # await bot_aiogram.send_message(message.chat.id, "exception: #861")
                                # ----------------------------------------------------end---------------------------------------------------------------
                                numbers_failure += 1
                                msg = None
                        # ----------------------------------------------------end---------------------------------------------------------------
                        print('---------------------------------------------')
                        text = f'{datetime.now().strftime("%d-%m %H:%M:%S")}\n'
                        await add_log_inviter(text)

                        # n += 1
                        # if n >= 198:
                        #     if msg:
                        #         await msg.delete()
                        #         msg = None
                        #     msg = await bot_aiogram.send_message(message.chat.id,
                        #                                          f'<b>{channel_short_name}</b>: инвайт продолжится через 24 часа из-за ограничений Телеграм.\nНе завершайте сессию с ботом.\n'
                        #                                          f'Пока запущено ожидание по каналу {channel_short_name}, Вы можете отправить еще один файл (с другим названием) для инвайта в <b>ДРУГОЙ канал</b>',
                        #                                          parse_mode='html')
                        #     await asyncio.sleep(60 * 24 + 15)
                        #     n = 0

                    current_step += 1
                    await sp.show_the_progress(msg_2, current_step, length)

                id = []
                for i in range(0, len(self.all_participant)):
                    id.append(self.all_participant[i][0])

                df = pd.DataFrame(
                    {
                        'id_participant': [str(self.all_participant[i][0]) for i in range(0, len(self.all_participant))],
                        'access_hash': [str(self.all_participant[i][1]) for i in range(0, len(self.all_participant))],
                        'username': [self.all_participant[i][2] for i in range(0, len(self.all_participant))],
                        'status': [self.all_participant[i][3] for i in range(0, len(self.all_participant))],
                    }
                )
                try:
                    df.to_excel(f'./excel/invite_report.xlsx', sheet_name='Sheet1')
                    print('got it')
                    await send_file_to_user(message, f'./excel/invite_report.xlsx')
                except Exception as e:
                    await bot_aiogram.send_message(message.chat.id, f"Something is wrong: {str(e)}")
                    print(f"Something is wrong: {str(e)}")
                # ---------------------------- end отправлять по одному инвайту---------------------------------

                if msg:
                    await msg.delete()
                    msg = None
                await bot_aiogram.send_message(message.chat.id,
                                               f'<b>{channel_short_name}</b>: {numbers_invite} пользователей заинвайтились, проверьте в канале\n'
                                               f'{numbers_failure} не заинватились в канал\n'
                                               f'{was_subscribe} были уже подписаны на канал', parse_mode='html')
                print(
                    f'886: {channel_short_name}: {numbers_invite} пользователей заинвайтились, проверьте в канале\n'
                    f'{numbers_failure} не заинватились в канал\n'
                    f'{was_subscribe} были уже подписаны на канал')
                self.all_participant = []
                self.marker = False
                os.remove(f'{file_name}')
            except Exception as e:
                if msg:
                    await msg.delete()
                    msg = None
                # await bot_aiogram.send_message(message.chat.id, f'bottom: #897: {e}')
                print(f'bottom: #897: {e}')

                await send_file_to_user(message, 'inviter_log.txt')

        async def invite_set_users(message, channel):
            logs.write_log(f"invite_bot_2: invite_set_users")
            msg = None
            channel_short_name = f"@{channel.split('/')[-1]}"

            # receiving the channel entity
            try:
                channel = await client.get_entity(channel)
                channel_to_send = InputChannel(channel.id, channel.access_hash)  # был InputPeerChannel
            except Exception as e:
                # await bot_aiogram.send_message(message.chat.id, f'{e}\nУкажате канал в формате https//t.me/<имя канала> (без @)\n'
                #                                         f'Обратите внимание на то, что <b>и Вы и этот бот</b> в этом канале должны быть <b>администраторами</b>', parse_mode='html')
                await bot_aiogram.send_message(message.chat.id, 'Это сообщение вместо того, по которому была ошибка')
                return False

            #
            await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: Инвайт запущен',
                                           parse_mode='html')

            while len(self.all_participant) > 0:
                if len(self.all_participant) > 50:
                    part_of_all_participant = self.all_participant
                    self.all_participant = self.all_participant[50:]
                else:
                    part_of_all_participant = self.all_participant
                    self.all_participant = []
                user_to_send = []
                for i in range(0, len(part_of_all_participant)):
                    user_to_send.append(InputUser(part_of_all_participant[i][0], part_of_all_participant[i][1]))

                try:
                    response_from_invite = await client(functions.channels.InviteToChannelRequest(channel_to_send, user_to_send))
                    print('!!!!!!!!!!!!!!! success!\n', response_from_invite)
                except Exception as e:
                    print('No invite: ', e)

                if len(self.all_participant)>0:
                    await bot_aiogram.send_message(message.chat.id, 'set has done')
                    await asyncio.sleep(15, 25)

            await bot_aiogram.send_message(message.chat.id, 'inviting has done, check please inside you channel')



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
        #
        # def send_to_db(id_user, api_id, api_hash, phone_number):
        #
        #     logs.write_log(f"invite_bot_2: function: send_to_db")
        #
        #     global con
        #
        #     if not con:
        #         con = db_connect()
        #
        #     cur = con.cursor()
        #     with con:
        #         cur.execute(f"""CREATE TABLE IF NOT EXISTS users (
        #             id SERIAL PRIMARY KEY,
        #             id_user INTEGER,
        #             api_id INTEGER,
        #             api_hash VARCHAR (50),
        #             phone_number VARCHAR (25),
        #             password VARCHAR (100)
        #             );"""
        #                     )
        #         con.commit()
        #
        #     with con:
        #         cur.execute(f"""SELECT * FROM users WHERE id_user={id_user}""")
        #         r = cur.fetchall()
        #
        #     if not r:
        #         with con:
        #             new_post = f"""INSERT INTO users (id_user, api_id, api_hash, phone_number)
        #                                             VALUES ({id_user}, {api_id}, '{api_hash}', '{phone_number}');"""
        #             cur.execute(new_post)
        #             con.commit()
        #             print(f'Пользователь {id_user} добавлен в базу')
        #             pass

        # def get_db(id_customer):
        #
        #     global con
        #
        #     logs.write_log(f"invite_bot_2: function: get_db")
        #
        #     if not con:
        #         con = db_connect()
        #
        #     cur = con.cursor()
        #
        #     query = f"""SELECT * FROM users WHERE id_user={id_customer}"""
        #     with con:
        #         cur.execute(query)
        #         r = cur.fetchall()
        #         print(r)
        #     return r

        async def hard_post(message, channels=None):
            status_agregator_send: bool
            statistics = {}
            progress = ShowProgress({'bot': bot_aiogram, 'chat_id': message.chat.id})
            try:
                await bot_aiogram.send_document(message.chat.id, "https://media.tenor.com/YIRu8WJDr6cAAAAC/dog-dogs.gif")
            except Exception as e:
                print("didn't push gif")


            if not channels:
                channels = self.valid_profession_list
            if type(channels) is str:
                channels = [channels]

            for profession in channels:
                await ambulance_saved_to_file("", rewrite=True)

                statistics[profession] = 0
                # choose from db regarding profession
                response = self.db.get_all_from_db(
                    table_name='admin_last_session',
                    param = f"WHERE profession LIKE '%{profession}' OR profession LIKE '%{profession},%'"
                )
                if response:
                    await bot_aiogram.send_message(message.chat.id, f"{profession} in progress...")
                    self.last_id_message_agregator = await get_last_admin_channel_id(
                        message=message,
                        channel=config['My_channels']['agregator_channel']
                    )
                    message_for_send = f'<i>Функционал дайджеста находится в состоянии альфа-тестирования, приносим свои ' \
                                       f'извинения, мы работаем над тем чтобы вы получали информацию максимально ' \
                                       f'качественную и в сжатые сроки</i>\n\n' \
                                       f'<b>Дайджест вакансий для {profession} за {datetime.now().strftime("%d.%m.%Y")}:</b>\n\n'

                    length = len(response)
                    n = 0
                    self.quantity_entered_to_shorts = 0
                    progress_message = await bot_aiogram.send_message(message.chat.id, "progress 0%")
                    await progress.reset_percent()
                    for vacancy in response:

                        id_admin_last_session_table = vacancy[0]

                        # to compose the vacancy message for sending to agregator
                        composed_message = await compose_message(vacancy, one_profession=profession, full=True)
                        composed_message['message'] = composed_message['composed_message']
                        pass

                        try:
                            # push to the admin channel
                            await push_vacancies_to_agregator_from_admin(
                                message=message,
                                vacancy=composed_message,
                                vacancy_from_admin=[vacancy],
                                response=[vacancy],
                                profession=profession,
                                id_admin_last_session_table=id_admin_last_session_table,
                                from_admin_temporary=False
                            )
                            status_agregator_send = True

                        except Exception as e:
                            print(f'It Couldnt send to agregator: {e}')
                            status_agregator_send = False
                            print(f'\n!!!!!! It has not sent to agregator channel because: {e}\ndata: prof: {profession}, id: {vacancy[0]}')
                        pass

                        if status_agregator_send:
                            # add to shorts
                            response = self.db.get_all_from_db(
                                table_name='admin_last_session',
                                param=f"WHERE id={id_admin_last_session_table}"
                            ) # for to refresh vacancy regarding agregator id if it has written
                            vacancy = response[0]
                            composed_message = await compose_message(vacancy, profession, full=False)
                            message_for_send += f"{composed_message['composed_message']}\n"

                            await ambulance_saved_to_file(f"{composed_message['composed_message']}")

                            statistics[profession] += 1
                            self.quantity_entered_to_shorts += 1

                            await compose_data_and_push_to_db(
                                vacancy_from_admin=[vacancy],
                                profession=profession
                            )
                            prof_list = vacancy[4].split(',')

                            # change field profession on DB or delete
                            await update_vacancy_admin_last_session(
                                results_dict=None,
                                profession=profession,
                                prof_list=prof_list,
                                id_admin_last_session_table=id_admin_last_session_table,
                                update_profession=True,
                                update_id_agregator=False
                            )
                            n += 1
                            progress_message = await progress.show_the_progress(progress_message, n, length)

                    vacancies_list = await cut_message_for_send(message_for_send)
                    n_count = 1
                    for short in vacancies_list:
                        try:
                            await write_to_logs_error(f"Results:\n{short}\n")
                            # push shorts
                            await bot_aiogram.send_message(config['My_channels'][f'{profession}_channel'], short, parse_mode='html',
                                                           disable_web_page_preview=True)
                            print(n_count, 'print shorts')
                            n_count += 1
                            await asyncio.sleep(random.randrange(1, 3))

                        except Exception as e:
                            await bot_aiogram.send_message(config['My_channels']['temporary_channel'], f'It did not send to {profession}. Please, do it manually', parse_mode='html')
                            await bot_aiogram.send_message(config['My_channels']['temporary_channel'], short, parse_mode='html',
                                                           disable_web_page_preview=True)
                else:
                    pass
            await bot_aiogram.send_message(message.chat.id, "The HARD pushing has done")
            statistics_message = 'The hard pushing statistics:\n\n'
            for profession in statistics:
                statistics_message += f"{profession}: {statistics[profession]}\n"
            await bot_aiogram.send_message(message.chat.id, statistics_message)
            try:
                await bot_aiogram.send_document(message.chat.id, "https://media.tenor.com/50IjyLmv8mQAAAAd/will-smith-clap.gif")
            except Exception as e:
                print("didn't push gif")

        async def ambulance_saved_to_file(text, rewrite=False):
            if rewrite:
                status = "w"
            else:
                status = "a+"
            with open("./ambulance/ambulance_shorts.txt", f"{status}") as file:
                try:
                    file.write(text)
                except Exception as e:
                    print(f"!!!!!! error for write in ambulance: {e}:\n text = {text}")


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
                self.marker = False
                channel_name = channel
                channel = config['My_channels'][f'{channel}_channel']

                offset_user = 0  # номер участника, с которого начинается считывание
                limit_user = 100  # максимальное число записей, передаваемых за один раз

                all_participants = []  # список всех участников канала
                filter_user = ChannelParticipantsSearch('')

                # channel = channel[4:]
                try:
                    channel = await client.get_input_entity(int(channel))
                    self.marker = True
                except:
                    try:
                        channel = channel[4:]
                        channel = await client.get_input_entity(int(channel))
                        self.marker = True
                    except Exception as e:
                        await bot_aiogram.send_message(message.chat.id, f'The error with channel {channel}: {str(e)}')
                        time.sleep(random.randrange(3, 6))

                if self.marker:
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

                    print(f'\nsleep...')
                    time.sleep(random.randrange(3, 6))

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

        async def send_file_to_user(message, path, caption='Please take it'):

            logs.write_log(f"invite_bot_2: function: send_file_to_user")

            with open(path, 'rb') as file:
                await bot_aiogram.send_document(message.chat.id, file, caption=caption)

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

        async def refresh_pattern(path):
            pattern = "pattern = " + "{\n"
            response = DataBaseOperations(None).get_all_from_db('pattern', without_sort=True)
            for i in response:
                print(i)
                pattern += f'{i}\n'
            with open(path, mode='w', encoding='utf-8') as f:
                f.write(pattern)
            pass

        async def compose_inline_keyboard(prefix=None):
            markup = InlineKeyboardMarkup(row_width=4)
            button_marketing = InlineKeyboardButton('marketing', callback_data=f'{prefix}/marketing')
            button_ba = InlineKeyboardButton('ba', callback_data=f'{prefix}/ba')
            button_game = InlineKeyboardButton('game', callback_data=f'{prefix}/game')
            button_product = InlineKeyboardButton('product', callback_data=f'{prefix}/product')
            button_mobile = InlineKeyboardButton('mobile', callback_data=f'{prefix}/mobile')
            button_pm = InlineKeyboardButton('pm', callback_data=f'{prefix}/pm')
            button_sales_manager = InlineKeyboardButton('sales_manager', callback_data=f'{prefix}/sales_manager')
            button_designer = InlineKeyboardButton('designer', callback_data=f'{prefix}/designer')
            button_devops = InlineKeyboardButton('devops', callback_data=f'{prefix}/devops')
            button_hr = InlineKeyboardButton('hr', callback_data=f'{prefix}/hr')
            button_backend = InlineKeyboardButton('backend', callback_data=f'{prefix}/backend')
            button_frontend = InlineKeyboardButton('frontend', callback_data=f'{prefix}/frontend')
            button_qa = InlineKeyboardButton('qa', callback_data=f'{prefix}/qa')
            button_junior = InlineKeyboardButton('junior', callback_data=f'{prefix}/junior')
            button_analyst = InlineKeyboardButton('analyst', callback_data=f'{prefix}/analyst')

            markup.row(button_marketing, button_ba, button_game, button_product)
            markup.row(button_mobile, button_pm, button_sales_manager, button_designer)
            markup.row(button_devops, button_hr, button_backend, button_frontend)
            markup.row(button_qa, button_junior, button_analyst)
            return markup

        async def compose_message(message, one_profession, full=False):
            profession_list = {}
            results_dict = {}
            profession_amount = []

            if message[4]:
                profession_list['profession'] = []
                print(message[4])
                if ',' in message[4]:
                    pro = message[4].split(',')
                else:
                    pro = [message[4]]

                for i in pro:
                    profession_list['profession'].append(i.strip())

                if one_profession:
                    professions_amount = profession_list['profession']  # count how much of professions
                    profession_list['profession'] = [one_profession, ]  # rewrite list if one_profession

                results_dict['chat_name'] = message[1]
                results_dict['title'] = message[2] #full
                results_dict['body'] = message[3] #full
                results_dict['profession'] = message[4]
                results_dict['vacancy'] = message[5] #+
                results_dict['vacancy_url'] = message[6] #+  #full
                results_dict['company'] = message[7] #+
                results_dict['english'] = message[8] #+
                results_dict['relocation'] = message[9] #+
                results_dict['job_type'] = re.sub(r'\<[a-zA-Z\s\.\-\'"=!\<_\/]+\>', " ", message[10]) #+
                results_dict['city'] = message[11] #+
                results_dict['salary'] = message[12] #full
                results_dict['experience'] = message[13] #full
                results_dict['contacts'] = message[14] #full
                results_dict['time_of_public'] = message[15]
                results_dict['created_at'] = message[16]
                results_dict['agregator_link'] = message[17]
                results_dict['session'] = message[18]
                sended_to_agregator = message[19]

                title = message[2]
                body = message[3]
                params = AlexSort2809().sort_by_profession_by_Alex(title, body, check_profession=False, check_contacts=False, check_vacancy=False)['params']


                # compose message_to_send
                # message_for_send = f'Вакансия {one_profession.title()}\n'
                message_for_send = ''
                if results_dict['vacancy']:
                    if not full:
                        message_for_send += f"<a href=\"{config['My_channels']['agregator_link']}/{sended_to_agregator}\"><b>Вакансия: {results_dict['vacancy']}</b>\n</a>"
                    else:
                        message_for_send += f"<b>Вакансия: {results_dict['vacancy']}</b>\n"
                elif params['vacancy']:
                    if not full:
                        message_for_send += f"<a href=\"{config['My_channels']['agregator_link']}/{sended_to_agregator}\"><b>Вакансия: {params['vacancy']}</b>\n</a>"
                    else:
                        message_for_send += f"<b>Вакансия: {params['vacancy']}</b>\n"
                else:
                    if not full:
                        message_for_send += f"<a href=\"{config['My_channels']['agregator_link']}/{sended_to_agregator}\"><b>Вакансия: #{random.randrange(100, 5000)}</b>\n</a>"
                    else:
                        message_for_send += f"<b>Вакансия: #{random.randrange(100, 5000)}</b>\n"


                if results_dict['company']:
                    message_for_send += f"Компания: {results_dict['company']}\n"
                elif params['company']:
                    message_for_send += f"Компания: {params['company']}\n"

                if results_dict['city']:
                    message_for_send += f"Город/страна: {results_dict['city']}\n"

                if results_dict['english']:
                    message_for_send += f"English: {results_dict['english']}\n"
                elif params['english']:
                    message_for_send += f"English: {params['english']}\n"

                if results_dict['job_type']:
                    message_for_send += f"Формат работы: {results_dict['job_type']}\n"
                elif params['job_type']:
                    message_for_send += f"Формат работы: {params['job_type']}\n"

                if results_dict['relocation']:
                    message_for_send += f"Релокация: {results_dict['relocation']}\n"
                elif params['relocation']:
                    message_for_send += f"Релокация: {params['relocation']}\n"

                # if not full:
                #     if sended_to_agregator and sended_to_agregator != "None":
                #         message_for_send += f"{config['My_channels']['agregator_link']}/{sended_to_agregator}\n"
                #         message_for_send += f"<a href=\"{config['My_channels']['agregator_link']}/{sended_to_agregator}\">Подробнее</a>"
                #         # message_for_send += hlink(title="Подробнее", url=f"{config['My_channels']['agregator_link']}/{sended_to_agregator}")
                #         message_for_send += '\n'

                if not message_for_send:
                    message_for_send = 'The vacancy not found\n\n'
                    await write_to_logs_error(f'The vacancy not found\n{title}{body}')

                if full:
                    if results_dict['salary']:
                        message_for_send += f"Зарплата: {results_dict['salary']}\n"

                    if results_dict['experience']:
                        message_for_send += f"Опыт работы: {results_dict['experience']}\n"

                    if results_dict['contacts']:
                        message_for_send += f"Контакты: {results_dict['contacts']}\n"
                    elif results_dict['vacancy_url']:
                        message_for_send += f"Ссылка на вакансию: {results_dict['vacancy_url']}\n"

                    if results_dict['vacancy'].strip() != results_dict['title'].strip() or (results_dict['vacancy'] and results_dict['title']):
                        message_for_send += f"\n<b>{results_dict['title']}</b>\n"
                    message_for_send += results_dict['body']

                    # message_for_send = re.sub(r'\<[A-Za-z\/=\"\-\>\s\._\<]{1,}\>', " ", message_for_send)

                # else:
                #     message_for_send += f"https://t.me/it_jobs_agregator/{sended_to_agregator}\n"

                # message_for_send = message_for_send.replace('\xa0', '')
                if len(message_for_send) > 4096:
                    message_for_send = message_for_send[0:4092] + '...'


                return {'composed_message': message_for_send, 'db_id': message[0]}

        async def get_last_admin_channel_id(message, channel=config['My_channels']['admin_channel']):
            last_admin_channel_id = None
            await bot_aiogram.send_message(channel, 'test')
            await asyncio.sleep(1)
            logs.write_log(f"scraping_telethon2: function: get_last_id_agregator")

            if channel != config['My_channels']['admin_channel']:
                limit_msg=1
            else:
                limit_msg=100

            try:
                all_messages = await get_tg_history_messages(message, channel, limit_msg)
                last_admin_channel_id = all_messages[0]['id']

                peer_channel = PeerChannel(int(channel))
                for i in all_messages:
                    await client.delete_messages(peer_channel, i['id'])
            except Exception as e:
                await bot_aiogram.send_message(message.chat.id, f'for admin channel: {e}')

            return last_admin_channel_id

        async def get_tg_history_messages(
                message,
                channel=config['My_channels']['admin_channel'],
                limit_msg=None):

            peer = await client.get_entity(int(channel))
            await asyncio.sleep(2)
            channel = PeerChannel(peer.id)
            if not limit_msg:
                limit_msg = 3000
            logs.write_log(f"scraping_telethon2: function: dump_all_messages")

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
                    history = await client(GetHistoryRequest(
                        peer=channel,
                        offset_id=offset_msg,
                        offset_date=None, add_offset=0,
                        limit=limit_msg, max_id=0, min_id=0,
                        hash=0))
                except Exception as e:
                    print(f'\n***Cant get messages from admin***\n{e}\n')
                    await bot_aiogram.send_message(message.chat.id, f'\n***Cant get messages from admin***\n{e}\n')
                    # await self.bot_dict['bot'].send_message(
                    #     self.bot_dict['chat_id'],
                    #     f"Getting history:\n{str(e)}: {channel}\npause 25-30 seconds...",
                    #     parse_mode="HTML",
                    #     disable_web_page_preview=True)
                    time.sleep(2)

                if not history:
                    print(f'Not history for channel {channel}')
                    await bot_aiogram.send_message(message.chat.id, f'Not history for channel {channel}')
                    break
                messages = history.messages
                if not messages:
                    return all_messages
                for message in messages:
                    if not message.message:  # если сообщение пустое, например "Александр теперь в группе"
                        pass
                    else:
                        all_messages.append(message.to_dict())

                if not len(all_messages):
                    return []
                else:
                    offset_msg = messages[len(messages) - 1].id
                total_messages = len(all_messages)
                if (total_count_limit != 0 and total_messages >= total_count_limit) or not len(all_messages):
                    break
                await asyncio.sleep(2)

            # logs.write_log(f"scraping_telethon2: function: get_admin_history_messages")
            #
            # print('get_admin_history_messages')
            # offset_msg = 0  # номер записи, с которой начинается считывание
            # # limit_msg = 1   # максимальное число записей, передаваемых за один раз
            # # limit_msg = 100
            # all_messages = []  # список всех сообщений
            # total_messages = 0
            # total_count_limit = limit_msg  # значение 0 = все сообщения
            # history = None
            #
            # peer = await client.get_entity(int(channel))
            # await asyncio.sleep(2)
            # channel = PeerChannel(peer.id)
            #
            # # channel = int(config['My_channels']['admin_channel'])
            # # while True:
            # try:
            #     history = await client(GetHistoryRequest(
            #         peer=channel,
            #         offset_id=offset_msg,
            #         offset_date=None, add_offset=0,
            #         limit=limit_msg, max_id=0, min_id=0,
            #         hash=0))
            # except Exception as e:
            #     await bot_aiogram.send_message(
            #         message.chat.id,
            #         f"Getting history:\n{str(e)}: {channel}\npause 25-30 seconds...",
            #         parse_mode="HTML",
            #         disable_web_page_preview=True)
            #     time.sleep(2)
            #
            # # if not history.messages:
            # if not history:
            #     print(f'Not history for channel {channel}')
            #     await bot_aiogram.send_message(message.chat.id, f'Not history for channel {channel}')
            #     # break
            # messages = history.messages
            # for message in messages:
            #     if not message.message:  # если сообщение пустое, например "Александр теперь в группе"
            #         pass
            #     else:
            #         all_messages.append(message.to_dict())

            return all_messages
            # offset_msg = messages[len(messages) - 1].id
            # total_messages = len(all_messages)
            # if total_count_limit != 0 and total_messages >= total_count_limit:
            #     break
            #
            # await self.process_messages(channel, all_messages)
            # print('pause 25-35 sec.')
            # time.sleep(random.randrange(15, 20))

        async def update_vacancy_admin_last_session(
                results_dict=None,
                profession=None,
                prof_list=None,
                id_admin_last_session_table=None,
                update_profession=False,
                update_id_agregator=False
        ):

            if update_profession:
                len_prof_list = len(prof_list)
                if len_prof_list < 2:
                    # print('ЖАРА в invite bot 2010')
                    #
                    # # if it is not in any tables, then write to no_sort table
                    # get_response = DataBaseOperations(None).get_all_from_db(table_name='admin_last_session', param=f"""WHERE id={id_admin_last_session_table}""")
                    # title = get_response[0][2]
                    # body = get_response[0][3]
                    # n=0
                    # valid_profession = self.valid_profession_list
                    # valid_profession.append('no_sort')
                    # for i in valid_profession:
                    #     get_response = DataBaseOperations(None).get_all_from_db(table_name=i,
                    #                                                             param=f"""WHERE body LIKE '%{body}%' AND title LIKE '%{title}%'""")
                    #     if get_response:
                    #         n +=1
                    #
                    # if n == 0:
                    #     print('ЖАРА в invite bot 2026')
                    #     profession_list = {}
                    #     profession_list['profession'] = {'no_sort',}
                    #     DataBaseOperations(None).push_to_bd(results_dict=results_dict, profession_list=profession_list)
                    # else:
                    DataBaseOperations(None).delete_data(
                        table_name='admin_last_session',
                        param=f"WHERE id={id_admin_last_session_table}"
                    )
                # 5. if more that delete current profession from column profession
                else:
                    new_profession = ''
                    for i in prof_list:
                        i = i.strip()
                        if i != profession:
                            new_profession += f'{i}, '
                    new_profession = new_profession[:-2].strip()
                    DataBaseOperations(None).run_free_request(
                        request=f"UPDATE admin_last_session SET profession='{new_profession}' WHERE id={id_admin_last_session_table}",
                        output_text='profession has updated'
                    )

                # # check the changes
                # response_check = DataBaseOperations(None).get_all_from_db(
                #     table_name='admin_last_session',
                #     param=f"WHERE id={id_admin_last_session_table}",
                #     without_sort=True
                # )
                # print('changed profession = ', response_check[0][4])

            if update_id_agregator:
                # 6 Mark vacancy like sended to agregator (write to column sended_to_agregator id_agregator)
                DataBaseOperations(None).run_free_request(
                    request=f"UPDATE admin_last_session SET sended_to_agregator='{self.last_id_message_agregator}' WHERE id={id_admin_last_session_table}",
                    output_text = 'sended_to_agregator has updated'
                )

                # check the changes
                response_check = DataBaseOperations(None).get_all_from_db(
                    table_name='admin_last_session',
                    param=f"WHERE id={id_admin_last_session_table}",
                    without_sort=True
                )
                print('changed id agreg = ', response_check[0][19])

            # await asyncio.sleep(1)

        async def compose_data_and_push_to_db(vacancy_from_admin, profession):
            date = datetime.now()
            profession_list = {}
            profession_list['profession'] = []
            results_dict = {}

            profession_list['profession'] = [profession, ]

            results_dict['chat_name'] = vacancy_from_admin[0][1]
            results_dict['title'] = vacancy_from_admin[0][2]
            results_dict['body'] = vacancy_from_admin[0][3]
            results_dict['profession'] = vacancy_from_admin[0][4]
            results_dict['vacancy'] = vacancy_from_admin[0][5]
            results_dict['vacancy_url'] = vacancy_from_admin[0][6]
            results_dict['company'] = vacancy_from_admin[0][7]
            results_dict['english'] = vacancy_from_admin[0][8]
            results_dict['relocation'] = vacancy_from_admin[0][9]
            results_dict['job_type'] = vacancy_from_admin[0][10]
            results_dict['city'] = vacancy_from_admin[0][11]
            results_dict['salary'] = vacancy_from_admin[0][12]
            results_dict['experience'] = vacancy_from_admin[0][13]
            results_dict['contacts'] = vacancy_from_admin[0][14]
            results_dict['time_of_public'] = vacancy_from_admin[0][15]
            results_dict['created_at'] = vacancy_from_admin[0][16]
            results_dict['agregator_link'] = vacancy_from_admin[0][17]
            results_dict['session'] = vacancy_from_admin[0][18]

            response_from_db = self.db.push_to_bd(
                results_dict=results_dict,
                profession_list=profession_list
            )
            return response_from_db

        async def delete_and_change_waste_vacancy(message, last_id_message_agregator, profession):
            # There are messages, which user deleted in admin. Their profession must be correct (delete current profession)
            response_admin_temporary = DataBaseOperations(None).get_all_from_db(
                table_name='admin_temporary',
                without_sort=True
            )
            length = len(response_admin_temporary)
            n = 0
            self.percent = 0

            if response_admin_temporary:
                await bot_aiogram.send_message(message.chat.id, 'It clears the temporary database')
                await asyncio.sleep(1)
                self.message = await bot_aiogram.send_message(message.chat.id, f'progress {self.percent}%')
                await asyncio.sleep(1)

            # theese vacancy we need to make profession changes
            for i in response_admin_temporary:
                id_admin_last_session_table = i[2]
                response_admin_last_session = DataBaseOperations(None).get_all_from_db(
                    table_name='admin_last_session',
                    param=f"WHERE id='{id_admin_last_session_table}'",
                    without_sort=True
                )
                prof_list = response_admin_last_session[0][4].split(', ')
                try:
                    await update_vacancy_admin_last_session(
                        results_dict=None,
                        profession=profession,
                        prof_list=prof_list,
                        id_admin_last_session_table=id_admin_last_session_table,
                        update_profession=True,
                        update_id_agregator=False
                    )
                except Exception as e:
                    print('error with deleting from admin temporary ', e)
                n = + 1
                await show_progress(message, n, length)
                # -------------------end ----------------------------

        async def push_vacancies_to_agregator_from_admin(
                message,
                vacancy,
                vacancy_from_admin,
                response,
                profession,
                id_admin_last_session_table,
                from_admin_temporary=True
        ):

            """
            :param message: message from class bot_aiorgam
            :param vacancy: one vacancy from vacancies list from TG adminka history. Will send to agregator channel
            :param vacancy_from_admin: the same vacancy, but from db admin last session
            :param response: the technical data. [0][3] show agregator id
            :param profession: solo profession
            :param id_admin_last_session_table: last message id from agregator
            :return:
            """
            if from_admin_temporary:
                index=3
            else:
                index=19

            # sending to agregator channel
            if response[0][index] == 'None' or not response[0][index]:  # response[0][3] indicates message was sended to agregator already
                print('\npush vacancy in agregator\n')
                print(f"\n{vacancy['message'][0:40]}")

                # sending the raw message without fields vacancy city etc
                await bot_aiogram.send_message(int(config['My_channels']['agregator_channel']), vacancy['message'],
                                               parse_mode='html',
                                               disable_notification=True)
                await asyncio.sleep(random.randrange(1, 2))
                self.last_id_message_agregator += 1

                # 3. writing id agregator in vacancy in admin last session because it has been sent to agregator

                if vacancy_from_admin:
                    prof_list = vacancy_from_admin[0][4].split(', ')

                    # 4. if one that delete vacancy from admin_last_session
                    await update_vacancy_admin_last_session(
                        id_admin_last_session_table=id_admin_last_session_table,
                        update_id_agregator=True, results_dict={})
                else:
                    await bot_aiogram.send_message(message.chat.id,
                                                   f"<b>For the developer</b>: Hey, bot didn't find this vacancy in admin_last_session",
                                                   parse_mode='html')
            else:
                # await bot_aiogram.send_message(message.chat.id, 'It has sent in agregator some time ago')
                print('It has sent in agregator some time ago')

        async def delete_used_vacancy_from_admin_temporary(vacancy, id_admin_last_session_table):
            # ------------------- cleaning the areas for the used vacancy  -------------------
            print('\ndelete vacancy\n')
            await client.delete_messages(int(config['My_channels']['admin_channel']), vacancy['id'])
            await asyncio.sleep(random.randrange(2, 3))

            # ----------------- deleting this vacancy's data from admin_temporary -----------------
            DataBaseOperations(None).delete_data(
                table_name='admin_temporary',
                param=f"WHERE id_admin_last_session_table='{id_admin_last_session_table}'"
            )

        # ------------------- end -------------------------

        async def cut_message_for_send(message_for_send):
            vacancies_list = []
            if len(message_for_send)>4096:
                message_limit = ''
                messages = message_for_send.split('\n\n')
                for i in messages:
                    if len(message_limit + f"{i}\n\n") < 4096:
                        message_limit += f"{i}\n\n"
                    else:
                        vacancies_list.append(message_limit)
                        message_limit = f"{i}\n\n"
                vacancies_list.append(message_limit)
            else:
                vacancies_list = [message_for_send]
            return vacancies_list

        async def show_progress(message, n, len):
            check = n * 100 // len
            if check > self.percent:
                quantity = check // 5
                self.percent = check
                self.message = await bot_aiogram.edit_message_text(
                    f"progress {'|' * quantity} {self.percent}%", self.message.chat.id, self.message.message_id)
            await asyncio.sleep(random.randrange(1, 2))

        async def write_to_logs_error(text):
            with open("./logs/logs_errors.txt", "a", encoding='utf-8') as file:
                file.write(text)

        async def get_excel_tags_from_admin(message):
            sp = ShowProgress(
                bot_dict={
                    'bot': bot_aiogram,
                    'chat_id': message.chat.id
                }
            )
            excel_list = {}
            excel_list['title'] = []
            excel_list['body'] = []
            excel_list['profession'] = []
            excel_list['tag'] = []
            excel_list['anti_tag'] = []
            excel_list['vacancy'] = []
            n = 0
            for i in ['admin_last_session']:
                response = DataBaseOperations(None).get_all_from_db(
                    table_name=f'{i}',
                    param="""WHERE profession <> 'no_sort'""",
                    # param="""WHERE profession LIKE '%designer%'""",
                    without_sort=True
                )

                # if n > 200:
                #     break
                await bot_aiogram.send_message(message.chat.id, f'There are {len(response)} records from {i}\nPlease wait...')
                msg = await bot_aiogram.send_message(message.chat.id, 'progress 0%')
                n=0
                length=len(response)
                for vacancy in response:
                    title = vacancy[2]
                    body = vacancy[3]
                    vac = vacancy[5]
                    response_from_filter = AlexSort2809().sort_by_profession_by_Alex(title=title, body=body)
                    profession = response_from_filter['profession']
                    params = response_from_filter['params']
                    if vac:
                        excel_list['vacancy'].append(vac)
                    elif params['vacancy']:
                        excel_list['vacancy'].append(params['vacancy'])
                    else:
                        excel_list['vacancy'].append('-')
                    excel_list['title'].append(title)
                    excel_list['body'].append(body)
                    excel_list['profession'].append(profession['profession'])
                    excel_list['tag'].append(profession['tag'])
                    excel_list['anti_tag'].append(profession['anti_tag'])
                    n += 1
                    print(f'step {n} passed')
                    msg = await sp.show_the_progress(
                        message=msg,
                        current_number=n,
                        end_number = length
                    )
                    # if n>200:
                    #     break
            df = pd.DataFrame(
                {
                    'title': excel_list['title'],
                    'body': excel_list['body'],
                    'vacancy': excel_list['vacancy'],
                    'profession': excel_list['profession'],
                    'tag': excel_list['tag'],
                    'anti_tag': excel_list['anti_tag']
                }
            )

            df.to_excel(f'./excel/statistics.xlsx', sheet_name='Sheet1')
            print('got it')
            await send_file_to_user(message, f'./excel/statistics.xlsx')

        async def delete_since(tables_list=None, ids_list=None, param=None):
            """
            delete records since time in params in tables in list[]
            """
            """
            DATE(created_at) > '2022-09-24'
            """
            if not tables_list:
                tables_list = ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst', 'mobile', 'qa',
                               'hr', 'game',
                               'ba', 'marketing', 'junior', 'sales_manager', 'no_sort', 'admin_last_session']
            for i in tables_list:
                if not ids_list:
                    DataBaseOperations(None).delete_data(table_name=i, param=param)
                else:
                    for id in ids_list:
                        DataBaseOperations(None).delete_data(table_name=i, param=f"WHERE id={id}")
                        print(f'Was deleted id={id} from {i}')

        async def output_consolidated_table(message):
            dates = []

            info_dict: dict
            info_dict = {}
            for i in self.valid_profession_list:
                info_dict[i] = []
                info_dict['date'] = []
            db = DataBaseOperations(con)
            date_now = datetime.now()
            start_data = datetime(2022, 9, 15, 0, 0, 0, 0)
            delta = int(str(date_now - start_data).split(' ', 1)[0])
            for date_offset in range(0, delta):
                date = date_now-timedelta(days=date_offset)
                print(date)
                date = date.strftime('%Y-%m-%d')
                info_dict['date'].append(date)
                for table in self.valid_profession_list:
                    response = db.get_all_from_db(
                        table_name=table,
                        param=f"""WHERE DATE(created_at)='{date}'"""
                    )
                    info_dict[table].append(len(response))
            # compose table
            df = pd.DataFrame({
                'date': info_dict['date'],
                'marketing': info_dict['marketing'],
                'backend': info_dict['backend'],
                'ba': info_dict['ba'],
                'game': info_dict['game'],
                'product': info_dict['product'],
                'mobile': info_dict['mobile'],
                'pm': info_dict['pm'],
                'sales_manager': info_dict['sales_manager'],
                'analyst': info_dict['analyst'],
                'frontend': info_dict['frontend'],
                'designer': info_dict['designer'],
                'devops': info_dict['devops'],
                'hr': info_dict['hr'],
                'qa': info_dict['qa'],
                'junior': info_dict['junior']
            })
            path = f'./excel/consolidated_table.xlsx'
            df.to_excel(path, sheet_name='Sheet1')
            print('got it')
            await send_file_to_user(message, path)

            print(info_dict)

        async def add_log_inviter(text):
            with open('inviter_log.txt', 'a+') as file:
                file.write(text)

        async def print_log(text):
            print(f"{datetime.now().strftime('%H:%M:%S')}:\n{text}")

        async def refresh(message):
            from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
            filter = AlexSort2809()
            db = DataBaseOperations(None)
            profession = {}
            title_list = []
            body_list = []
            old_prof_list = []
            new_prof_list = []
            tag_list = []
            anti_tag = []

            await bot_aiogram.send_message(message.chat.id, 'It will rewrite the professions in all vacancies through the new filter logic\nPlease wait few seconds for start')

            with open('pr.txt', 'w') as file:
                file.write('')

            response = DataBaseOperations(None).get_all_from_db(
                table_name='admin_last_session'
            )
            show = ShowProgress(bot_dict={'bot': bot_aiogram, 'chat_id': message.chat.id})
            n=0
            length = len(response)
            msg = await bot_aiogram.send_message(message.chat.id, 'progress 0%')
            for i in response:
                print(i[2])
                print(f'old prof [{i[4]}]')
                title = i[2]
                body = i[3]

                if 'https://t.me' in i[1]:
                    profession = filter.sort_by_profession_by_Alex(title, body, get_params=False)
                else:
                    profession = filter.sort_by_profession_by_Alex(title, body, check_contacts=False, check_vacancy=False, get_params=False)

                print('new2', profession['profession']['profession'])
                print(f"{profession['profession']['tag']}")
                print(f"{profession['profession']['anti_tag']}")
                print('--------------------------')
                try:
                    with open('pr.txt', 'a+') as file:
                        file.write(
                            f"{i[2]}\nold prof [{i[4]}]\n"
                            f"___\n"
                            f"new prof2 (title+body): {profession['profession']['profession']}\n"
                            f"{profession['profession']['tag']}\n"
                            f"{profession['profession']['anti_tag']}\n"
                            f"__________________________________\n"

                        )
                except:
                    pass

                profession_str = ''
                for prof in profession['profession']['profession']:
                    profession_str += f"{prof}, "

                title_list.append(i[2])
                body_list.append(i[3])
                old_prof_list.append(i[4])
                new_prof_list.append(profession_str)
                tag_list.append(profession['profession']['tag'])
                anti_tag.append(profession['profession']['anti_tag'])

                profession_str = profession_str[:-2]
                print(profession_str, '\n________________\n')
                pass

                db.run_free_request(
                    request=f"""UPDATE admin_last_session SET profession='{profession_str}' WHERE id={i[0]}""",
                    output_text='updated\n___________\n\n'
                )
                n += 1
                await show.show_the_progress(msg, n, length)

            df = pd.DataFrame(
                {
                    'title': title_list,
                    'body': body_list,
                    'old_prof': old_prof_list,
                    'new_prof': new_prof_list,
                    'tag': tag_list,
                    'anti_tag': anti_tag
                }
            )
            path = './excel/professions_rewrite.xls'

            try:
                df.to_excel(path, sheet_name='Sheet1')
                await send_file_to_user(message, path, caption='You win! Take the logs for checking how it was and how it is')
                print('got it')
            except:
                try:
                    await send_file_to_user(message, './other_operations/pr.txt', caption="It did not send excel so take txt logs")
                except:
                    await bot_aiogram.send_message(message.chat.id, 'Done')


        executor.start_polling(dp, skip_updates=True)

InviteBot().main_invitebot()