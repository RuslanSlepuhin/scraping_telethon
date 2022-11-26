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
from telethon.sync import TelegramClient
from telethon.tl import functions
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import InputUser, InputChannel, ChannelParticipantsSearch, PeerChannel, PeerUser
from db_operations.scraping_db import DataBaseOperations
from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
from scraping_telegramchats2 import WriteToDbMessages, main
from sites.parsing_sites_runner import ParseSites
from logs.logs import Logs
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

        class Form(StatesGroup):
            api_id = State()
            api_hash = State()
            phone_number = State()
            code = State()
            password = State()

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
                                                            '/refresh_pattern - to get the modify pattern from DB\n'
                                                            '/peerchannel - useful for a developer to get id channel\n'
                                                            '/getdata - get channel data')

        @dp.message_handler(commands=['logs', 'log'])
        async def get_logs(message: types.Message):
            path = './logs/logs.txt'
            await send_file_to_user(message, path)

        @dp.message_handler(commands=['peerchannel'])
        async def get_logs(message: types.Message):
            await bot_aiogram.send_message(message.chat.id, 'Type the channel link and get channel data')
            self.peerchannel = True

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

        # async def get_id_agregator():
        #     # Need to get id last message from agregator. To push 'test', get id and delete 'push' from
        #     # push 'test'
        #     id_agregator_channel = int(config['My_channels']['agregator_channel'])
        #     await bot_aiogram.send_message(int(config['My_channels']['agregator_channel']), 'test')
        #     # await asyncio.sleep(random.randrange(1, 2))
        #
        #     all_messages = await get_tg_history_messages(callback.message)
        #
        #     wtdb = WriteToDbMessages(
        #         client=client,
        #         bot_dict=None
        #     )
        #     last_id_message_agregator = await wtdb.get_last_id_agregator()
        #     await client.delete_messages(id_agregator_channel, last_id_message_agregator)
        #
        #     return last_id_message_agregator

        @dp.callback_query_handler()
        async def catch_callback(callback: types.CallbackQuery):
            short_digest = ''
            response = []

            if callback.data == 'go_by_admin':
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
                    pass
                    # await bot_aiogram.send_message(callback.message.chat.id, f'The attempt to delete admin_temporary is wrong\n{str(e)}')
                    # await asyncio.sleep(random.randrange(2, 3))

                # delete messages for channel will be clean to take new messages
                all_messages = await get_tg_history_messages(callback.message)
                for i in all_messages:
                    await client.delete_messages(PeerChannel(int(config['My_channels']['admin_channel'])), i['id'])

                # to get last message_id
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
                    # for i in response:
                    #     print(i)

                    # composed_message_dict = {}

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
                            if len(text) > 4096:
                                text = text[:4093] + '...'
                            await bot_aiogram.send_message(config['My_channels']['admin_channel'], text, parse_mode='html')
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


            if 'PUSH' in callback.data:
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
                        await push_vacancies_from_admin(
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
                            composed_message_dict={}
                            composed_message_dict = await compose_message(vacancy_from_admin[0], profession)
                            message_for_send += f"{composed_message_dict['composed_message']}\n"
                            self.quantity_entered_to_shorts += 1
                            prof_list = vacancy_from_admin[0][4].split(',')
                            await update_vacancy_admin_last_session(
                                profession=profession,
                                prof_list=prof_list,
                                id_admin_last_session_table=id_admin_last_session_table,
                                update_profession=True,
                                update_id_agregator=False
                            )
                            # write to prof DB
                            profession_list['profession'] = [profession,]
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

                        response_dict = DataBaseOperations(None).push_to_bd(results_dict, profession_list, self.last_id_message_agregator)
                        await delete_used_vacancy_from_tg_db(vacancy, id_admin_last_session_table)
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

                DataBaseOperations(None).delete_table(
                    table_name='admin_temporary'
                )
                await bot_aiogram.send_message(callback.message.chat.id, f'<b>Done!</b>\n'
                                                                         f'- in to statistics: {self.quantity_in_statistics}\n'
                                                                         f'- in to admin {self.quantity_entered_to_admin_channel}\n'
                                                                         f'- out from admin {self.out_from_admin_channel}\n'
                                                                         f'- in to shorts {self.quantity_entered_to_shorts}',
                                               parse_mode='html')


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

            global all_participant, marker, file_name, marker_code, client
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

#-----------------------------------------------------try---------------------------------------------------------------
                            try:
                                user_channel_status = await bot_aiogram.get_chat_member(chat_id=channel_short_name, user_id=id_user)
                                if user_channel_status.status != types.ChatMemberStatus.LEFT:
                                    if msg:
                                        await msg.delete()
                                        msg = None
                                    # msg = await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: пользователь с id={id_user} уже подписан', parse_mode='html')
                                    print('Пользователь уже подписан')
                                    await asyncio.sleep(1)
                                    was_subscribe += 1
                                    user_exists = True
                                else:
                                    print('Пользователь новый')
                                    user_exists = False

                            except Exception as e:
                                print('Пользователь новый')
                                user_exists = False
                                if msg:
                                    await msg.delete()
                                    msg = None
                                # await bot_aiogram.send_message(message.chat.id, f"813: {str(e)}")
                                print(f"813: if username != None {str(e)}")
# ----------------------------------------------------end---------------------------------------------------------------
                            if not user_exists:
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
                                                await bot_aiogram.send_message(message.chat.id, f"824: if username != None {str(e)}")
                                                print(f"824: if username != None {str(e)}")
# ----------------------------------------------------end---------------------------------------------------------------
                                else:
                                    # -----------------------------------------------------try---------------------------------------------------------------
                                    try:
                                        user_to_send = [InputUser(id_user, access_hash_user)]  # (PeerUser(id_user))
                                    except Exception as e:
                                        await bot_aiogram.send_message(message.chat.id, f"831: if username = None {str(e)}")
                                        print(f"831: if username = None {str(e)}")
# ----------------------------------------------------end---------------------------------------------------------------
# -----------------------------------------------------try---------------------------------------------------------------
                                if msg:
                                    await msg.delete()
                                    msg = None
                                try:
                                    # client.invoke(InviteToChannelRequest(channel_to_send,  [user_to_send]))
                                    # await client(InviteToChannelRequest(channel_to_send, user_to_send))  #work!!!!!
                                    await client(functions.channels.InviteToChannelRequest(channel_to_send, user_to_send))

                                    msg = await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}:</b> {user[0]} заинвайлся успешно\n'
                                                                                  f'({numbers_invite+1} инвайтов)', parse_mode='html')
                                    numbers_invite += 1

                                except Exception as e:
                                    if re.findall(r'seconds is required (caused by InviteToChannelRequest)', str(e)) or \
                                            str(e) == "Too many requests (caused by InviteToChannelRequest)":
                                        break
                                    else:
                                        if msg:
                                            await msg.delete()
                                            msg = None
# -----------------------------------------------------try---------------------------------------------------------------
                                        try:
                                            await bot_aiogram.send_message(message.chat.id, f'<b>{channel_short_name}</b>: Для пользователя id={user[0]}\n{str(e)}', parse_mode='html')
                                        except Exception:
                                            print('exception: 861')
                                            await bot_aiogram.send_message(message.chat.id, "exception: 861")
# ----------------------------------------------------end---------------------------------------------------------------
                                        numbers_failure += 1
                                        msg = None
# ----------------------------------------------------end---------------------------------------------------------------

                                n += 1
                                await asyncio.sleep(random.randrange(10, 15))
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
                        print(f'886: {channel_short_name}: {numbers_invite} пользователей заинвайтились, проверьте в канале\n'
                                           f'{numbers_failure} не заинватились в канал\n'
                                           f'{was_subscribe} были уже подписаны на канал')
                        all_participant = []
                        marker = False
                        os.remove(f'{file_name}')
                    except Exception as e:
                        if msg:
                            await msg.delete()
                            msg = None
                        await bot_aiogram.send_message(message.chat.id, f'bottom: 897: {e}')
                        print(f'bottom: 897: {e}')

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
                    # await bot_aiogram.send_message(message.chat.id, 'Scraping is starting')
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
                    await psites.call_sites()
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
                        get_customer_from_db = DataBaseOperations(None).get_all_from_db(table_name='users', param=f"WHERE id_user='{id_customer}'", without_sort=True)
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
                    but_show = InlineKeyboardButton('Show the statistics',
                                                    callback_data='show_info_last_records')
                    # but_send_digest_full = InlineKeyboardButton('Разлить fulls посл сессию',
                    #                                             callback_data='send_digest_full')
                    # but_send_digest_full_all = InlineKeyboardButton('Разлить fulls всё',
                    #                                                 callback_data='send_digest_full_all')
                    # but_separate_channel = InlineKeyboardButton('Залить в 1 канал',
                    #                                             callback_data='choose_one_channel')
                    but_do_by_admin = InlineKeyboardButton('Go by admin',
                                                                callback_data='go_by_admin')
                    but_stat_today = InlineKeyboardButton('One day statistics', callback_data='one_day_statistics')
                    # self.markup.row(but_show, but_send_digest_full)
                    # self.markup.row(but_send_digest_full_all, but_separate_channel)
                    self.markup.add(but_show)
                    self.markup.add(but_stat_today)
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
                marker = False
                channel_name = channel
                channel = config['My_channels'][f'{channel}_channel']

                offset_user = 0  # номер участника, с которого начинается считывание
                limit_user = 100  # максимальное число записей, передаваемых за один раз

                all_participants = []  # список всех участников канала
                filter_user = ChannelParticipantsSearch('')

                # channel = channel[4:]
                try:
                    channel = await client.get_input_entity(int(channel))
                    marker = True
                except:
                    try:
                        channel = channel[4:]
                        channel = await client.get_input_entity(int(channel))
                        marker = True
                    except Exception as e:
                        await bot_aiogram.send_message(message.chat.id, f'The error with channel {channel}: {str(e)}')
                        time.sleep(random.randrange(3, 6))

                if marker:
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
            markup.row(button_marketing, button_ba, button_game, button_product)
            markup.row(button_mobile, button_pm, button_sales_manager, button_designer)
            markup.row(button_devops, button_hr, button_backend, button_frontend)
            markup.row(button_qa, button_junior)
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
                results_dict['job_type'] = message[10] #+
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
                params = AlexSort2809().sort_by_profession_by_Alex(title, body)['params']


                # compose message_to_send
                # message_for_send = f'Вакансия {one_profession.title()}\n'
                message_for_send = ''
                if results_dict['vacancy']:
                    message_for_send += f"<b>Вакансия: {results_dict['vacancy']}</b>\n"
                elif params['vacancy']:
                    message_for_send += f"<b>Вакансия: {params['vacancy']}</b>\n"
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

                if not full:
                    if sended_to_agregator and sended_to_agregator != "None":
                        # message_for_send += f"{config['My_channels']['agregator_link']}/{sended_to_agregator}\n"
                        message_for_send += f"<a href=\"{config['My_channels']['agregator_link']}/{sended_to_agregator}\">Подробнее</a>"
                        # message_for_send += hlink(title="Подробнее", url=f"{config['My_channels']['agregator_link']}/{sended_to_agregator}")
                        message_for_send += '\n'

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

        async def get_tg_history_messages(message, channel=config['My_channels']['admin_channel'], limit_msg=100):
            logs.write_log(f"scraping_telethon2: function: get_admin_history_messages")

            print('get_admin_history_messages')
            offset_msg = 0  # номер записи, с которой начинается считывание
            # limit_msg = 1   # максимальное число записей, передаваемых за один раз
            # limit_msg = 100
            all_messages = []  # список всех сообщений
            total_messages = 0
            total_count_limit = limit_msg  # значение 0 = все сообщения
            history = None

            peer = await client.get_entity(int(channel))
            await asyncio.sleep(2)
            channel = PeerChannel(peer.id)
            # while True:
            try:
                history = await client(GetHistoryRequest(
                    peer=channel,
                    offset_id=offset_msg,
                    offset_date=None, add_offset=0,
                    limit=limit_msg, max_id=0, min_id=0,
                    hash=0))
            except Exception as e:
                await bot_aiogram.send_message(
                    message.chat.id,
                    f"Getting history:\n{str(e)}: {channel}\npause 25-30 seconds...",
                    parse_mode="HTML",
                    disable_web_page_preview=True)
                time.sleep(2)

            # if not history.messages:
            if not history:
                print(f'Not history for channel {channel}')
                await bot_aiogram.send_message(message.chat.id, f'Not history for channel {channel}')
                # break
            messages = history.messages
            for message in messages:
                if not message.message:  # если сообщение пустое, например "Александр теперь в группе"
                    pass
                else:
                    all_messages.append(message.to_dict())

            return all_messages
            # offset_msg = messages[len(messages) - 1].id
            # total_messages = len(all_messages)
            # if total_count_limit != 0 and total_messages >= total_count_limit:
            #     break
            #
            # await self.process_messages(channel, all_messages)
            # print('pause 25-35 sec.')
            # time.sleep(random.randrange(15, 20))

        async def get_data(message, url_channel=None, id_channel=None):
            if url_channel:
                input_data=url_channel
            elif id_channel:
                input_data=id_channel
            data = await client.get_entity(input_data)
            await bot_aiogram(message.chat.id, str(data))


        async def update_vacancy_admin_last_session(
                profession=None,
                prof_list=None,
                id_admin_last_session_table=None,
                update_profession=False,
                update_id_agregator=False
        ):

            if update_profession:
                len_prof_list = len(prof_list)
                if len_prof_list < 2:
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
                        request=f"UPDATE admin_last_session SET profession='{new_profession}' WHERE id={id_admin_last_session_table}"
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
                    request=f"UPDATE admin_last_session SET sended_to_agregator='{self.last_id_message_agregator}' WHERE id={id_admin_last_session_table}"
                )

                # check the changes
                response_check = DataBaseOperations(None).get_all_from_db(
                    table_name='admin_last_session',
                    param=f"WHERE id={id_admin_last_session_table}",
                    without_sort=True
                )
                print('changed id agreg = ', response_check[0][19])

            await asyncio.sleep(random.randrange(1, 3))

        async def compose_for_push_to_db(response, profession):
            date = datetime.now()
            profession_list = {}
            profession_list['profession'] = []
            results_dict = {
                'chat_name': '',
                'title': '',
                'body': '',
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
                'created_at': '',
                'session': ''
            }

            results_dict['chat_name'] = response[0][1]
            results_dict['title'] = response[0][2]
            results_dict['body'] = response[0][3]
            results_dict['profession'] = response[0][4]
            results_dict['vacancy'] = response[0][5]
            results_dict['vacancy_url'] = response[0][6]
            results_dict['company'] = response[0][7]
            results_dict['english'] = response[0][8]
            results_dict['relocation'] = response[0][9]
            results_dict['job_type'] = response[0][10]
            results_dict['city'] = response[0][11]
            results_dict['salary'] = response[0][12]
            results_dict['experience'] = response[0][13]
            results_dict['contacts'] = response[0][14]
            results_dict['time_of_public'] = response[0][15]
            results_dict['created_at'] = response[0][16]
            results_dict['agregator_link'] = response[0][17]
            results_dict['session'] = response[0][18]
            results_dict['sended_to_agregator'] = response[0][19]

            profession_list['profession'] = [profession]
            response_from_db = DataBaseOperations(None).push_to_bd(
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
                await asyncio.sleep(random.randrange(2, 3))
                self.message = await bot_aiogram.send_message(message.chat.id, f'progress {self.percent}%')
                await asyncio.sleep(random.randrange(2, 3))

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
                        profession,
                        prof_list,
                        id_admin_last_session_table,
                        update_profession=True,
                        update_id_agregator=False
                    )
                except Exception as e:
                    print('error with deleting from admin temporary ', e)
                n = + 1
                await show_progress(message, n, length)
                # -------------------end ----------------------------

        async def push_vacancies_from_admin(
                message,
                vacancy,
                vacancy_from_admin,
                response,
                profession,
                id_admin_last_session_table
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

            # sending to agregator channel
            if response[0][3] == 'None' or not response[0][3]:  # response[0][3] indicates message was sended to agregator already
                print('\npush vacancy in agregator\n')
                print(f"\n{vacancy['message'][0:40]}")

                # sending the raw message without fields vacancy city etc
                await bot_aiogram.send_message(int(config['My_channels']['agregator_channel']), vacancy['message'])
                await asyncio.sleep(random.randrange(2, 3))
                self.last_id_message_agregator += 1

                # 3. writing id agregator in vacancy in admin last session because it has been sent to agregator

                if vacancy_from_admin:
                    prof_list = vacancy_from_admin[0][4].split(', ')

                    # 4. if one that delete vacancy from admin_last_session
                    await update_vacancy_admin_last_session(
                        id_admin_last_session_table=id_admin_last_session_table,
                        update_id_agregator=True)
                else:
                    await bot_aiogram.send_message(message.chat.id,
                                                   f"<b>For the developer</b>: Hey, bot didn't find this vacancy in admin_last_session",
                                                   parse_mode='html')
            else:
                await bot_aiogram.send_message(message.chat.id, 'It has sent in agregator some time ago')

        async def delete_used_vacancy_from_tg_db(vacancy, id_admin_last_session_table):
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

        executor.start_polling(dp, skip_updates=True)

InviteBot().main_invitebot()