import configparser
import json
import re
import pandas as pd
import psycopg2
from datetime import datetime
from filters.scraping_get_profession_Alex_Rus import AlexRusSort
from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809

# from scraping_send_to_bot import PushToDB
config = configparser.ConfigParser()
config.read("./settings/config.ini")

# ---------------------DB operations ----------------------
class DataBaseOperations:

    def __init__(self, con):
        self.con = con
        if not con:
            self.connect_db()

    def connect_db(self):

        self.con = None
        database = config['DB3']['database']
        user = config['DB3']['user']
        password = config['DB3']['password']
        host = config['DB3']['host']
        port = config['DB3']['port']
        try:
            self.con = psycopg2.connect(
                database=database,
                user=user,
                password=password,
                host=host,
                port=port
            )
        except:
            print('No connect with db')
        return self.con

    #-------------participants-------------------------
    def push_to_bd_participants(self, participant, all_user_dictionary, channel_name, channel_username):

        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        with self.con:

            cur.execute("""CREATE TABLE IF NOT EXISTS participant_table (
                        id SERIAL PRIMARY KEY,
                        id_participant VARCHAR(40),
                        first_name VARCHAR(150),
                        last_name VARCHAR (150),
                        user_name VARCHAR (40),
                        phone VARCHAR (40),
                        is_bot BOOLEAN,
                        channel VARCHAR (150),
                        entity JSONB
                        );"""
                                )
            self.con.commit()


        with self.con:

            channel = channel_name
            print('all user len = ', len(all_user_dictionary))
            for i in all_user_dictionary:

                id_participant = i['id']
                first_name = i['first_name']
                last_name = i['last_name']
                user_name = i['user']
                phone = i['phone']
                is_bot = i['is_bot']
                entity = json.dumps(i)

                print(i)


                query = f"""SELECT * FROM participant_table WHERE id_participant='{id_participant}' AND channel='{channel_name}'"""
                cur.execute(query)
                response = cur.fetchall()

                if not response:
                    new_post = f"""INSERT INTO participant_table (id_participant, first_name, last_name, user_name, phone, is_bot, channel, entity) 
                                            VALUES ('{id_participant}', '{first_name}', '{last_name}', '{user_name}', '{phone}', '{is_bot}', '{channel}', {entity});"""
                    try:
                        cur.execute(new_post)

                        # self.con.commit()

                        print('!!!!!!!!!!!!!add to users ', i)
                    except Exception as e:
                        print(e)
                else:
                    print('This user exist already', i)

    #--------------------------------------------------

    def check_or_create_table(self, cur, table_name):
        with self.con:

            cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                chat_name VARCHAR(150),
                title VARCHAR(1000),
                body VARCHAR (6000),
                profession VARCHAR (30),
                time_of_public TIMESTAMP,
                created_at TIMESTAMP
                );"""
                        )
            self.con.commit()

    def push_to_bd(self, results_dict):

        response_dict = {}
        # response_dict['time_sleep'] = False

        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        print(f'\n****************************************\n'
              f'INPUT IN DB FUNC = \n', results_dict)

        chat_name = results_dict['chat_name']
        title = results_dict['title'].replace(f'\'', '"')
        body = str(results_dict['body']).replace(f'\'', '"')

        prof = AlexSort2809()
        params = prof.sort_by_profession_by_Alex(title, body)
        if params['block']:
            response_dict['block'] = True
            return response_dict

        pro = params['profession']

# -------------------- if add or no_sort -------------------
        if params['profession'] in ['ad', 'no_sort'] and len(params)>1:
            params = {'profession': params['profession']}
        # response_dict['profession'] = pro

        print(f'\nResponse DB: profession = {pro}\n')

        time_of_public = results_dict['time_of_public']
        try:
            created_at = results_dict['created_at']
        except:
            created_at = datetime.now()

        self.quant = 1

        if type(pro) is list or type(pro) is set:
            pro_set = pro

            for pro in pro_set:
                self.check_or_create_table(cur, pro)
                self.push_to_db_write_message(cur, pro, title, body, chat_name, time_of_public, created_at, response_dict)

        else:
            self.check_or_create_table(cur, pro)
            response_dict = self.push_to_db_write_message(cur, pro, title, body, chat_name, time_of_public, created_at, response_dict)

            # self.check_or_create_table(cur, pro)

    # ------------ add to db table's name pro message ----------------
    #     with self.con:
    #         try:
    #             query = f"""SELECT * FROM {pro} WHERE title='{title}' AND body='{body}'"""
    #             cur.execute(query)
    #             r = cur.fetchall()
    #
    #             if not r:
    #                 response_dict[pro] = False
    #                 # response_dict['time_sleep'] = True
    #
    #                 new_post = f"""INSERT INTO {pro} (chat_name, title, body, profession, time_of_public, created_at)
    #                             VALUES ('{chat_name}', '{title}', '{body}', '{pro}', '{time_of_public}', '{created_at}');"""
    #                 cur.execute(new_post)
    #                 self.con.commit()
    #                 print(self.quant, f'= Added to DB {pro}\n')
    #                 self.quant += 1
    #
    #             else:
    #                 response_dict[pro] = True
    #                 print(self.quant, f'!!!! This message exists already in {pro}\n')
    #
    #         except Exception as e:
    #             print('Dont push in db, error = ', e)
    #             # return response_dict['error', e]

# ------------ add to db table's name params message ----------------

        # for param in params:
        #     if param not in ['profession', 'block'] and params[param]:
        #
        #         self.check_or_create_table(cur, param)
        #         response_dict = self.push_to_db_write_params(cur, pro, param, title, body, chat_name, time_of_public, created_at, response_dict)
        #
        #         with self.con:
        #             try:
        #                 query = f"""SELECT * FROM {param} WHERE title='{title}' AND body='{body}'"""
        #                 cur.execute(query)
        #                 r = cur.fetchall()
        #
        #                 if not r:
        #                     response_dict[param] = False
        #                     # response_dict['time_sleep'] = True
        #
        #                     new_post = f"""INSERT INTO {param} (chat_name, title, body, profession, time_of_public, created_at)
        #                                 VALUES ('{chat_name}', '{title}', '{body}', '{pro}', '{time_of_public}', '{created_at}');"""
        #                     cur.execute(new_post)
        #                     self.con.commit()
        #                     print(self.quant, f'= Added to DB {param}\n')
        #                     self.quant += 1
        #
        #                 else:
        #                     print(self.quant, f'!!!!! This message exists already in {param}\n')
        #                     response_dict[param] = True
        #
        #             except Exception as e:
        #                 print('Dont push in db, error = ', e)
        #                 # return response_dict['error', e]
        pass
        return response_dict

    def push_to_db_write_message(self, cur, pro, title, body, chat_name, time_of_public, created_at, response_dict):
        with self.con:
            try:
                query = f"""SELECT * FROM {pro} WHERE title='{title}' AND body='{body}'"""
                cur.execute(query)
                r = cur.fetchall()

                if not r:
                    response_dict[pro] = False
                    # response_dict['time_sleep'] = True

                    new_post = f"""INSERT INTO {pro} (chat_name, title, body, profession, time_of_public, created_at) 
                                VALUES ('{chat_name}', '{title}', '{body}', '{pro}', '{time_of_public}', '{created_at}');"""
                    # cur.execute(new_post) # !!!!!!!!!!!!!!!!!!!!!!!
                    print(self.quant, f'= Added to DB {pro}\n')
                    self.quant += 1

                else:
                    response_dict[pro] = True
                    print(self.quant, f'!!!! This message exists already in {pro}\n')

            except Exception as e:
                print('Dont push in db, error = ', e)
                # return response_dict['error', e]
        pass
        return response_dict
    # def push_to_db_write_params(self, cur, pro, param, title, body, chat_name, time_of_public, created_at, response_dict):
    #     with self.con:
    #         try:
    #             query = f"""SELECT * FROM {param} WHERE title='{title}' AND body='{body}'"""
    #             cur.execute(query)
    #             r = cur.fetchall()
    #
    #             if not r:
    #                 response_dict[param] = False
    #                 # response_dict['time_sleep'] = True
    #
    #                 new_post = f"""INSERT INTO {param} (chat_name, title, body, profession, time_of_public, created_at)
    #                             VALUES ('{chat_name}', '{title}', '{body}', '{pro}', '{time_of_public}', '{created_at}');"""
    #                 cur.execute(new_post)
    #                 self.con.commit()
    #                 print(self.quant, f'= Added to DB {param}\n')
    #                 self.quant += 1
    #
    #             else:
    #                 print(self.quant, f'!!!!! This message exists already in {param}\n')
    #                 response_dict[param] = True
    #
    #         except Exception as e:
    #             print('Dont push in db, error = ', e)
    #             # return response_dict['error', e]
    #

    def get_all_from_db(self, table_name, param=None):
        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        if not param:
            query = f"""SELECT * FROM {table_name} ORDER BY time_of_public"""
        else:
            query = f"""SELECT * FROM {table_name} {param} ORDER BY time_of_public"""

        with self.con:
            cur.execute(query)
            response = cur.fetchall()
            n=0
            for i in response:
                print('n = ', n)
                print(i)
                n += 1
                # for j in i:
                #     print('j = ', j)
                #     n += 1
        return response

    def delete_data(self, table_name):
        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        query = f"""DELETE FROM {table_name}"""
        with self.con:
            cur.execute(query)

#-----------просто в одну таблицу записать все сообщения без професии, чтобы потом достать, рассортировать и записать в файл ------------------
    def write_to_one_table(self, results_dict):
        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        self.check_or_create_table(cur, 'all_messages')

        chat_name = results_dict['chat_name']
        title = results_dict['title'].replace(f'\'', '"')
        body = str(results_dict['body']).replace(f'\'', '"')
        time_of_public = results_dict['time_of_public']
        created_at = datetime.now()

        with self.con:
            try:
                query = f"""SELECT * FROM all_messages WHERE title='{title}' AND body='{body}'"""
                cur.execute(query)
                r = cur.fetchall()

                if not r:

                    new_post = f"""INSERT INTO all_messages (chat_name, title, body, profession, time_of_public, created_at) 
                                               VALUES ('{chat_name}', '{title}', '{body}', '{None}', '{time_of_public}', '{created_at}');"""
                    # cur.execute(new_post) #!!!!!!!!!!!!!!!!!!!!!!!!!
                    print(f'= Added to DB all_messages\n')

                else:
                    print(f'!!!!! This message exists already in all_messages\n')

            except Exception as e:
                print('Dont push in db, error = ', e)
                # return response_dict['error', e]
            pass

# ---------------- это для того, чтобы достать неотсортированные сообщения из базы и прогнать через оба алгоритма ---------
    def get_from_bd_for_analyze_python_vs_excel(self):
        """
        Get in DB messages and write it to Excel file to check
        :return: nothing
        """

        profession_alex = []
        profession_rus = []
        profession_channel = []
        profession_title = []
        profession_body = []
        profession_alex_tag = []
        profession_alex_antitag = []
        profession_rus_tag = []

        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        query = f"""SELECT * FROM all_messages WHERE DATE(created_at) > '2022-09-24' ORDER BY time_of_public"""
        with self.con:
            cur.execute(query)
            r = cur.fetchall()
        for item in r:
            pro_alex = ''
            pro_rus = ''
            created_at = ''

            print('r = ', item)
            channel = item[1]
            title = item[2].replace('Обсуждение вакансии в чате @devops_jobs', '')
            body = item[3].replace('Обсуждение вакансии в чате @devops_jobs', '')
            time_public = item[5]
            created_at = item[6]
            # alex_old = AlexSort().sort_by_profession_by_Alex(title, body)
            alex = AlexSort2809().sort_by_profession_by_Alex(title, body)
            rus = AlexRusSort().sort_by_profession_by_AlexRus(title, body)


            for pro in alex['profession']:
                pro_alex += pro + ' '
            pro_rus = rus['profession']

            profession_channel.append(channel)
            profession_alex.append(pro_alex)
            profession_alex_tag.append(alex['tag'])
            profession_alex_antitag.append(alex['anti_tag'])
            profession_rus.append(pro_rus)
            try:
                profession_rus_tag.append(rus['tag'])
            except:
                pass
            profession_title.append(title)
            profession_body.append(body)


        df = pd.DataFrame(
            {
               'channel':  profession_channel,
                'pro_Alex_28092022_nigth': profession_alex,
                # 'tag_Alex': profession_alex_tag,
                # 'antitag_Alex': profession_alex_antitag,
                'alternative': profession_rus,
                # 'tag_Rus': profession_rus_tag,
                'title': profession_title,
                'body': profession_body,
                'created_at': created_at,
                'time_public': time_public,
            }
        )

        df.to_excel('all_messages.xlsx', sheet_name='Sheet1')

    # def send_to_bot(self, response):
    #     """
    #     It bring message to WriteTelegramChats class, where it sends to bot
    #     :param response: all records from DB with all messages
    #     :return: nothing
    #     """
    #     print(response)
    #     for record in response:
    #
    #         channel_name = record[1]
    #         title = self.clear_text_control(record[2]).replace('\u200b', '')
    #         body = self.clear_text_control(record[3]).replace('\u200b', '')
    #         # time_of_public = record[5]
    #         # created_at = record[6]
    #
    #         # profession = Professions().sort_by_profession(title, body)
    #         profession = AlexSort2809().sort_by_profession_by_Alex(title, body)
    #         print('PROFESSION scr_db  ============ ', profession)
    #         pass
    #
    #         profession_message = self.collect_data_for_send_to_bot(profession) # собрать в правильном виде для отправки в бота
    #
    #         if re.findall(r'/', profession_message):
    #             len_profession_message = len(profession_message.split('/'))-1
    #         else:
    #             len_profession_message = 1
    #
    #         message = f'{title}\n{body}'
    #         print('len = ', len_profession_message)
    #         print('message = ', message.replace(f'\n', '')[0:40])
    #
    #         pass
    #         PushToDB().start_push_to_bot(length=len_profession_message, prof=profession_message, message=message)
    #         pass

    def collect_data_for_send_to_bot(self, profession):
        """

        :param profession: get dict and collect phrase type of qa/middle/senior/
        :return: this phrase
        """
        profession_str = ''

        if not profession['block']:
            if profession['profession'] not in ['ad', 'no_sort']:

                if type(profession['profession']) is set: # we get data in list from Alex filter
                    for i in profession['profession']:
                        profession_str += i + '/'
                else:  # we get str from Ruslan filter
                    profession_str = profession['profession'] + '/'

                if profession['junior']>0:
                    profession_str += 'junior/'
                if profession['middle']>0:
                    profession_str += 'middle/'
                if profession['senior']>0:
                    profession_str += 'senior/'
            else:
                profession_str = profession['profession'] + '/'
        else:
            pass
        pass

        return profession_str

    def clear_text_control(self, text):
        text = re.sub(r'<[\W\w\d]{1,7}>', '\n', text)
        return text

    def find_last_record(self, response, title_search=None, body_search=None):
        result = None
        marker = False
        new_response = []

        print('len response = ', len(response))
        print('Last element = ', response[-1])

        for record in response:

            if marker:
                new_response.append(record)

            elif not marker:
                if re.findall(title_search, record[2]) or re.findall(body_search, record[3]):
                    print(f'Find!!! id = {record[0]}\ntext{record[2]}\n{record[3]}')
                    marker = True
        return new_response



# con=''
# for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'fullstack', 'mobile', 'qa', 'hr',
#               'game', 'ba', 'marketing', 'junior', 'middle', 'senior', 'ad']:
#
#     DataBaseOperations(con).delete_data(i)
#     print(f'Удалены записи из {i}')
#     DataBaseOperations(con).get_all_from_db(i)

# DataBaseOperations(con).get_from_bd_for_analyze_python_vs_excel()

# -------------- get one message from test file ---------------
# response = []
# response_gl = []
# with open('text.txt', 'r', encoding='utf-8') as file:
#     text = file.read()
# text = text.split(f'\n', 1)
# t_text_title2 = text[0].lower()
# t_text_body2 = text[1].lower()
# response.append('0')
# response.append('1')
# response.append(t_text_title2)
# response.append(t_text_body2)
# response_gl.append(response)
# DataBaseOperations(con).send_to_bot(response_gl)
# -------------- end get one message from test file ---------------

# -------------- get all messages from all_messages to send in channels -------------
# param = "WHERE DATE(time_of_public) > '2022-10-01'"
# response = DataBaseOperations(con).get_all_from_db('all_messages', param=param)

# response_start_since_text = DataBaseOperations(con).find_last_record(
#     response,
#     title_search='#CV #Javascript #Angular #junior #frontend #',
#     body_search='HTML-coder/Junior Frontend developer(Angular)'
# )
# pass

# DataBaseOperations(con).send_to_bot(response)
# ----------------------------------------------------------------------------------

#
# profession = Professions().sort_by_profession(t_text_title2, t_text_body2)
# print('profession = ', profession)

# --------------- delete old bases and rewrite to them messages ------------------
# delete bases
# get from all messages, check profession and write to profession db


