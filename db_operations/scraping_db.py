import configparser
import json
import re
import pandas as pd
import psycopg2
from datetime import datetime
from filters.scraping_get_profession_Alex_Rus import AlexRusSort
from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809

# from scraping_send_to_bot import PushToDB
from patterns import pattern_Alex2809

config = configparser.ConfigParser()
config.read("./../settings/config.ini")

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
                created_at TIMESTAMP,
                agregator_link VARCHAR(200)
                );"""
                        )
            self.con.commit()

    def push_to_bd(self, results_dict, profession_list=None, agregator_id=None):

        response_dict = {}
        if not self.con:
            self.connect_db()
        cur = self.con.cursor()
        chat_name = results_dict['chat_name']
        title = results_dict['title'].replace(f'\'', '"')
        body = str(results_dict['body']).replace(f'\'', '"')
        pro = profession_list['profession']
# -------------------- if add or no_sort -------------------
        if profession_list['profession'] in ['ad', 'no_sort'] and len(profession_list)>1:
            profession_list = {'profession': profession_list['profession']}
        print(f'\nResponse DB: profession = {pro}\n')
        time_of_public = results_dict['time_of_public']
        try:
            created_at = results_dict['created_at']
        except:
            created_at = datetime.now()
        self.quant = 1
# -------------------------- create short message --------------------------------
        if type(pro) is list or type(pro) is set:
            pro_set = pro

            for pro in pro_set:
                self.check_or_create_table(cur, pro)
                self.push_to_db_write_message(cur, pro, title, body, chat_name, time_of_public, created_at, response_dict, agregator_id)
        else:
            self.check_or_create_table(cur, pro)
            response_dict = self.push_to_db_write_message(cur, pro, title, body, chat_name, time_of_public, created_at, response_dict, agregator_id)
        return response_dict

    def push_to_db_write_message(self, cur, pro, title, body, chat_name, time_of_public, created_at, response_dict, agregator_id):
        with self.con:
            try:
                query = f"""SELECT * FROM {pro} WHERE title='{title}' AND body='{body}'"""
                cur.execute(query)
                r = cur.fetchall()
                if not r:
                    response_dict[pro] = False
                    new_post = f"""INSERT INTO {pro} (chat_name, title, body, profession, time_of_public, created_at, agregator_link) 
                                VALUES ('{chat_name}', '{title}', '{body}', '{pro}', '{time_of_public}', '{created_at}', '{agregator_id}');"""
                    cur.execute(new_post) # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    print(self.quant, f'Has added to DB {pro}\n')
                    self.quant += 1

                else:
                    response_dict[pro] = True
                    print(self.quant, f'!!!! This message exists already in {pro}\n')

            except Exception as e:
                print('Dont push in db, error = ', e)
        return response_dict

    def get_all_from_db(self, table_name, param=None, without_sort=False, order=None):
        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        if not order:
            order = "ORDER BY time_of_public"

        if not param:
            query = f"""SELECT * FROM {table_name} {order}"""
        else:
            query = f"""SELECT * FROM {table_name} {param} {order}"""
        if without_sort:
            query = f"""SELECT * FROM {table_name}"""

        print('query = ', query)

        with self.con:
            cur.execute(query)
            response = cur.fetchall()
            # n=0
            # for i in response:
            #     print('n = ', n)
            #     print(i)
            #     n += 1
                # for j in i:
                #     print('j = ', j)
                #     n += 1
        return response

    def delete_data(self, table_name, param):
        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        query = f"""DELETE FROM {table_name} {param}"""
        print('query: ', query)
        with self.con:
            cur.execute(query)
#-----------просто в одну таблицу записать все сообщения без професии, чтобы потом достать, рассортировать и записать в файл ------------------
    def write_to_one_table(self, results_dict):
        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        self.check_or_create_table(cur, 'all_messages')  #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1

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
                    # cur.execute(new_post) #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
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

    def write_to_db_companies(self, companies):
        con = self.connect_db()
        cur = con.cursor()

        query = """CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            company VARCHAR(100)
            );
            """
        with con:
            cur.execute(query)

        for value in companies:

            query = f"""SELECT * FROM companies WHERE company='{value}'"""
            with con:
                try:
                    cur.execute(query)
                    response = cur.fetchall()
                except Exception as e:
                    print(e)


            if not response:
                query = f"""INSERT INTO companies (company) VALUES ('{value}')"""
                with con:
                    cur.execute(query)
                    print(f'to put: {value}')

    def rewrite_to_archive(self):
        for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst',
                                    'fullstack', 'mobile', 'qa', 'hr', 'game', 'ba', 'marketing', 'junior',
                                    'sales_manager']:
        # for i in ['no_sort', 'middle', 'senior']:
            response = self.get_all_from_db(i)
            if not self.con:
                self.connect_db()
            cur = self.con.cursor()
            table_archive = f"{i}_archive"
            self.check_or_create_table(cur=cur, table_name=table_archive)
            for message in response:
                query = f"""INSERT INTO {table_archive} (chat_name, title, body, profession, time_of_public, created_at) 
                        VALUES ('{message[1]}', '{message[2]}', '{message[3]}', '{message[4]}', '{message[5]}', '{message[6]}')"""
                with self.con:
                    try:
                        cur.execute(query)
                        print(f'{i} rewrited to {table_archive}')
                    except Exception as e:
                        print('error: ', e)

    def add_columns_to_tables(self):
        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst',
                                    'fullstack', 'mobile', 'qa', 'hr', 'game', 'ba', 'marketing', 'junior',
                                    'sales_manager', 'middle', 'senior']:

            query = f"""ALTER TABLE {i} ADD COLUMN agregator_link VARCHAR(200)"""
            with self.con:
                cur.execute(query)
                print(f'Added agr_link to {i}')

    def output_tables(self):

        db_tables = []

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        query = """select * from information_schema.tables where table_schema='public';"""
        with self.con:
            cur.execute(query)
            result = cur.fetchall()
        summ = 0
        for i in result:
            # print(i[2])
            query = f"SELECT MAX(id) FROM {i[2]}"
            with self.con:
                cur.execute(query)
                result = cur.fetchall()[0][0]
                print(f"{i[2]} = {result}")
                if result:
                    summ += result
        print(f'\nвсего записей: {summ}')

    def delete_table(self, table_name):

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        query = f"""DROP TABLE {table_name};"""
        with self.con:
            cur.execute(query)
            print(f'{table_name} was deleted')


# con=''
# for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'fullstack', 'mobile', 'qa', 'hr',
#               'game', 'ba', 'marketing', 'junior', 'middle', 'senior', 'ad', 'sales_manager']:
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

# DataBaseOperations(con=None).add_columns_to_tables()
# pass
# for i in ['tag']:
#     DataBaseOperations(con=None).delete_table(table_name=i)
# DataBaseOperations(con=None).output_tables()

# response = DataBaseOperations(None).get_all_from_db('backend', order="ORDER BY created_at")
# for i in response:
#     print(i[6])

# for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst',
#                                     'mobile', 'qa', 'hr', 'game', 'ba', 'marketing', 'junior',
#                                     'sales_manager']:
#     response = DataBaseOperations(None).get_all_from_db(i, param="WHERE created_at > '2022-10-11 19:00:00'")
#     for resp in response:
#         print(i, resp[6])
#     DataBaseOperations(None).delete_data(table_name=i, param="WHERE created_at > '2022-10-11 19:00:00'")
#     response = DataBaseOperations(None).get_all_from_db(i, param="WHERE created_at > '2022-10-11 19:00:00'")
#     if not response:
#         print('Is empty')
#     else:
#         for resp in response:
#             print(i, resp[6])

