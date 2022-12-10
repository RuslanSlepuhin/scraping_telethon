
from sites.scraping_geekjob import GeekJobGetInformation
from sites.scraping_finder import FindJobGetInformation
from sites.scraping_hh import HHGetInformation
from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
from db_operations.scraping_db import DataBaseOperations
from scraping_telegramchats2 import WriteToDbMessages
from progress.progress import ShowProgress
import configparser
from logs.logs import Logs
logs = Logs()

config = configparser.ConfigParser()
config.read("./settings/config.ini")

class ParseSites:

    def __init__(self, client, bot_dict):
        self.client = client
        self.current_session = ''
        self.bot = bot_dict['bot']
        self.chat_id = bot_dict['chat_id']


    async def call_sites(self):

        logs.write_log(f"scraping_telethon2: function: call_sites")

        bot_dict = {'bot': self.bot, 'chat_id': self.chat_id}

        await HHGetInformation(bot_dict).get_content()
        # messages_list = await self.compose_message_for_sending(response_dict_hh, do_write_companies=True)

        # response_dict_geek = await GeekJobGetInformation(bot_dict).get_content()
        # messages_list = await self.compose_message_for_sending(response_dict_geek, do_write_companies=True)
        #
        # response_dict_finder = await FindJobGetInformation(bot_dict).get_content()
        # messages_list = await self.compose_message_for_sending(response_dict_finder, do_write_companies=True)


        print(' -----------------------FINAL -------------------------------')

#     async def compose_message_for_sending(self, response_dict, do_write_companies=False):
#
#         logs.write_log(f"scraping_telethon2: function: compose_message_for_sending")
#
#         messages_list = []
#
#         # ---------------------- write or not companies to db --------------------------
#         if do_write_companies:
#             con=''
#             DataBaseOperations(con=con).write_to_db_companies(set(response_dict['company']))
#
# # -------------------------------- compose messages --------------------------------
#         await self.bot.send_message(self.chat_id, 'Пишет в админку')
#         msg = await self.bot.send_message(self.chat_id, 'progress 0%')
#         bot_dict = {'bot': self.bot, 'chat_id': self.chat_id}
#         sp = ShowProgress(bot_dict)
#
#         last_id_agregator = await WriteToDbMessages(client=self.client, bot_dict=None).get_last_id_agregator() + 1
#         message = ''
#         body = ''
#         for each_element in range(0, len(response_dict['title'])):
#             # result_dict = {
#             #     "chat_name": "",
#             #     "vacancy": "",
#             #     "company": "",
#             #     "body": "",
#             #     "english": "",
#             #     "relocation": "",
#             #     "job_type": "",
#             #     "city": "",
#             #     "salary": "",
#             #     "experience": "",
#             #     "time_of_public": "",
#             #     "contacts": "",
#             #     "vacancy_url": "",
#             #     "title": "",
#             # }
#
#             result_dict = {
#                 "chat_name": response_dict['chat_name'][each_element] if  response_dict['chat_name'][each_element] else "",
#                 "vacancy": response_dict['vacancy'][each_element] if response_dict['vacancy'][each_element] else "",
#                 "company": response_dict['company'][each_element] if response_dict['company'][each_element] else "",
#                 "body": response_dict['body'][each_element] if response_dict['body'][each_element] else "",
#                 "english": response_dict['english'][each_element] if response_dict['english'][each_element] else "",
#                 "relocation": response_dict['relocation'][each_element] if response_dict['relocation'][each_element] else "",
#                 "job_type": response_dict['job_type'][each_element] if response_dict['job_type'][each_element] else "",
#                 "city": response_dict['city'][each_element] if response_dict['city'][each_element] else "",
#                 "salary": response_dict['salary'][each_element] if response_dict['salary'][each_element] else "",
#                 "experience": response_dict['experience'][each_element] if response_dict['experience'][each_element] else "",
#                 "time_of_public": response_dict['time_of_public'][each_element] if response_dict['time_of_public'][each_element] else "",
#                 "contacts": response_dict['contacts'][each_element] if response_dict['contacts'][each_element] else "",
#                 "vacancy_url": response_dict['vacancy_url'][each_element] if response_dict['vacancy_url'][each_element] else "",
#                 "title": response_dict['vacancy'][each_element] if response_dict['vacancy'][each_element] else ""
#             }
#
# #__________________________________________________________________________________
#             # if response_dict['chat_name'][each_element]:
#             #     result_dict['chat_name'] = response_dict['chat_name'][each_element]
#             #
#             # if response_dict['vacancy'][each_element]:
#             #     message += f"Вакансия: {response_dict['vacancy'][each_element]}\n"
#             #     result_dict['vacancy'] += f"{response_dict['vacancy'][each_element]}"
#             #     result_dict['title'] = f"Вакансия: {response_dict['vacancy'][each_element]}"
#             #
#             # if response_dict['company'][each_element]:
#             #     message += f"Компания: {response_dict['company'][each_element]}\n"
#             #     result_dict['body'] += f"Компания: {response_dict['company'][each_element]}\n"
#             #     result_dict['company'] = response_dict['company'][each_element]
#             #
#             # if response_dict['english'][each_element]:
#             #     message += f"Язык: {response_dict['english'][each_element]}\n"
#             #     result_dict['body'] += f"Язык: {response_dict['english'][each_element]}\n"
#             #     result_dict['english'] = response_dict['english'][each_element]
#             #
#             # if response_dict['relocation'][each_element]:
#             #     message += f"Релокация: {response_dict['relocation'][each_element]}\n"
#             #     result_dict['body'] += f"Релокация: {response_dict['relocation'][each_element]}\n"
#             #     result_dict['relocation'] = response_dict['relocation'][each_element]
#             #
#             # if response_dict['job_type'][each_element]:
#             #     message += f"Тип работы: {response_dict['job_type'][each_element]}\n"
#             #     result_dict['body'] += f"Тип работы: {response_dict['job_type'][each_element]}\n"
#             #     result_dict['job_type'] = response_dict['job_type'][each_element]
#             #
#             # if response_dict['city'][each_element]:
#             #     message += f"Город/страна: {response_dict['city'][each_element]}\n"
#             #     result_dict['body'] += f"Город/страна: {response_dict['city'][each_element]}\n"
#             #     result_dict['city'] = response_dict['city'][each_element]
#             #
#             # if response_dict['salary'][each_element]:
#             #     message += f"Зарплата: {response_dict['salary'][each_element]}\n"
#             #     result_dict['body'] += f"Зарплата: {response_dict['salary'][each_element]}\n"
#             #     result_dict['salary'] = response_dict['salary'][each_element]
#             #
#             # if response_dict['experience'][each_element]:
#             #     message += f"Опыт работы: {response_dict['experience'][each_element]}\n"
#             #     result_dict['body'] += f"Опыт работы: {response_dict['experience'][each_element]}\n"
#             #     result_dict['experience'] = response_dict['experience'][each_element]
#             #
#             # if response_dict['time_of_public'][each_element]:
#             #     result_dict['time_of_public'] = response_dict['time_of_public'][each_element]
#             #
#             # if response_dict['contacts'][each_element]:
#             #     message += f"Контакты: {response_dict['contacts'][each_element]}\n\n"
#             #     result_dict['body'] += f"Контакты: {response_dict['contacts'][each_element]}\n\n"
#             # elif response_dict['vacancy_url'][each_element]:
#             #     message += f"Ссылка на вакансию: {response_dict['vacancy_url'][each_element]}\n\n"
#             #     result_dict['body'] += f"Ссылка на вакансию: {response_dict['vacancy_url'][each_element]}\n\n"
#             #
#             # if response_dict['body'][each_element]:
#             #     body = response_dict['body'][each_element].replace(':', ': ').replace(';', ';\n').replace('\n\n', '')
#             #     message += f"{body}\n"
#             #     result_dict['body'] += body
# #----------------------------------------------------------------------------
#             current_session = DataBaseOperations(None).get_all_from_db(
#                 table_name='current_session',
#                 param='ORDER BY id DESC LIMIT 1',
#                 without_sort=True,
#                 order=None,
#                 field='session',
#                 curs=None
#             )
#             for value in current_session:
#                 self.current_session = value[0]
#
#             result_dict['session'] = self.current_session
#
#             # get a profession
#             profession_list = AlexSort2809().sort_by_profession_by_Alex(
#                 title=response_dict['title'][each_element],
#                 body=response_dict['body'][each_element],
#                 get_params=False,
#                 only_profession=True,
#             )
#             # write to db (append fields)
#             # r_response_dict = DataBaseOperations(con=None).push_to_bd(result_dict, profession_list, agregator_id=last_id_agregator)
#
#             DataBaseOperations(None).push_to_admin_table(result_dict, profession_list['profession'])
#
#             await sp.show_the_progress(
#                 message=msg,
#                 current_number=each_element+1,
#                 end_number=len(response_dict['title'])
#             )
#             pass
#             # send to agregator
#
#         return messages_list