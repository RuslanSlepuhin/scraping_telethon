import configparser
import json
import re
import pandas as pd
import psycopg2
from datetime import datetime
# from filters.scraping_get_profession_Alex_Rus import AlexRusSort
# from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
from logs.logs import Logs
logs = Logs()

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

        logs.write_log(f"scraping_db: function: connect_db")

        self.con = None
        config.read("./../settings/config.ini")
        try:
            database = config['DB3']['database']
            user = config['DB3']['user']
            password = config['DB3']['password']
            host = config['DB3']['host']
            port = config['DB3']['port']
        except:
            config.read("./settings/config.ini")
            database = config['DB_local_clone']['database']
            user = config['DB_local_clone']['user']
            password = config['DB_local_clone']['password']
            host = config['DB_local_clone']['host']
            port = config['DB_local_clone']['port']
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

        logs.write_log(f"scraping_db: function: push_to_bd_participants")

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

        logs.write_log(f"scraping_db: function: check_or_create_table")

        cur = self.con.cursor()

        with self.con:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                            id SERIAL PRIMARY KEY,
                            chat_name VARCHAR(150),
                            title VARCHAR(1000),
                            body VARCHAR (6000),
                            profession VARCHAR (30),
                            vacancy VARCHAR (700),
                            vacancy_url VARCHAR (150),
                            company VARCHAR (200),
                            english VARCHAR (100),
                            relocation VARCHAR (100),
                            job_type VARCHAR (700),
                            city VARCHAR (150),
                            salary VARCHAR (300),
                            experience VARCHAR (700),
                            contacts VARCHAR (500),
                            time_of_public TIMESTAMP,
                            created_at TIMESTAMP,
                            agregator_link VARCHAR(200),
                            session VARCHAR(15),
                            FOREIGN KEY (session) REFERENCES current_session(session)
                            );"""
                        )
            print(f'table {table_name} has been crated or exists')

    def push_to_bd(self, results_dict, profession_list=None, agregator_id=None):

        logs.write_log(f"scraping_db: function: push_to_bd")

        response_dict = {}
        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

#         chat_name = results_dict['chat_name']
#         title = results_dict['title'].replace(f'\'', '"')
#         body = str(results_dict['body']).replace(f'\'', '"')
        pro = profession_list['profession']
# # -------------------- if add or no_sort -------------------
#         if profession_list['profession'] in ['ad', 'no_sort'] and len(profession_list)>1:
#             profession_list = {'profession': profession_list['profession']}
#         print(f'\nResponse DB: profession = {pro}\n')
#         time_of_public = results_dict['time_of_public']
#         try:
#             created_at = results_dict['created_at']
#         except:
#             created_at = datetime.now()
        self.quant = 1
# -------------------------- create short message --------------------------------
        if type(pro) is list or type(pro) is set:
            pro_set = pro

            for pro in pro_set:
                self.check_or_create_table(cur, pro)
                self.push_to_db_write_message(cur, pro, results_dict, response_dict, agregator_id)
        else:
            self.check_or_create_table(cur, pro)
            response_dict = self.push_to_db_write_message(cur, pro, results_dict, response_dict, agregator_id)
        return response_dict

    def push_to_db_write_message(self, cur, pro, results_dict, response_dict, agregator_id):

        logs.write_log(f"scraping_db: function: push_to_db_write_message")

        results_dict['title'] = self.clear_title_or_body(results_dict['title'])
        results_dict['body'] = self.clear_title_or_body(results_dict['body'])
        results_dict['company'] = self.clear_title_or_body(results_dict['company'])

        query = f"""SELECT * FROM {pro} WHERE title='{results_dict['title']}' AND body='{results_dict['body']}'"""
        query_double = f"""SELECT * FROM {pro} 
                        WHERE title LIKE '%{results_dict['title'].strip()}%' AND 
                        body LIKE '%{results_dict['body'].strip()}%'"""
        with self.con:
            try:
                cur.execute(query)
                r = cur.fetchall()
            except Exception as e:
                print(f'\nError in request or exists in DB {e}\n')

        with self.con:
            try:
                cur.execute(query_double)
                r2 = cur.fetchall()
            except Exception as e:
                print(f'\nError in request or exists in DB {e}\n')

        if not r and r2:
            pass


        if not r and not r2:
            response_dict[pro] = False
            new_post = f"""INSERT INTO {pro} (
            chat_name, title, body, profession, vacancy, vacancy_url, company, english, relocation, job_type, 
            city, salary, experience, contacts, time_of_public, created_at, agregator_link, session) 
                        VALUES ('{results_dict['chat_name']}', '{results_dict['title']}', '{results_dict['body']}', 
                        '{pro}', '{results_dict['vacancy']}', '{results_dict['vacancy_url']}', '{results_dict['company']}', 
                        '{results_dict['english']}', '{results_dict['relocation']}', '{results_dict['job_type']}', 
                        '{results_dict['city']}', '{results_dict['salary']}', '{results_dict['experience']}', 
                        '{results_dict['contacts']}', '{results_dict['time_of_public']}', '{datetime.now()}', '{agregator_id}', 
                        '{results_dict['session']}');"""
            # print('query in db: ', new_post)
            with self.con:
                try:
                    cur.execute(new_post)
                    print(self.quant, f'+++++++++++++ The vacancy has been added to DB {pro}\n')
                except Exception as e:
                    print('didnt push in DB ', e)
                    pass

                self.quant += 1
            pass
        else:
            pass
            response_dict[pro] = True
            print(self.quant, f'!!!! This message exists already in {pro}\n')

        return response_dict

    def clear_title_or_body(self, text):
        text = text.replace('\'', '\"')
        return text

    def get_all_from_db(self, table_name, param='', without_sort=False, order=None, field='*', curs=None):

        logs.write_log(f"scraping_db: function: get_all_from_db")

        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        if not order:
            order = "ORDER BY time_of_public"

        if not without_sort:
            query = f"""SELECT {field} FROM {table_name} {param} {order}"""
        else:
            query = f"""SELECT {field} FROM {table_name} {param} """

        # print('query = ', query)

        with self.con:

            cur.execute(query)
            response = cur.fetchall()

        if curs:
            return cur
        return response

    def write_current_session(self, current_session):

        logs.write_log(f"scraping_db: function: write_current_session")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        query = """CREATE TABLE IF NOT EXISTS current_session (
                    id SERIAL PRIMARY KEY,
                    session VARCHAR(15) UNIQUE
                    );"""
        with self.con:
            cur.execute(query)
            print('session create or exists')

        query = f"""INSERT INTO current_session (session) VALUES ({current_session})"""
        with self.con:
            try:
                cur.execute(query)
                print(f'session {current_session} was writing')
            except Exception as e:
                print(e)
            pass

    def delete_data(self, table_name, param):

        logs.write_log(f"scraping_db: function: delete_data")

        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        query = f"""DELETE FROM {table_name} {param}"""
        # print('query: ', query)
        with self.con:
            try:
                cur.execute(query)
                print(f'got it, delete data from {table_name}')
            except Exception as e:
                print(f'didnt delete the data from {table_name}: {e}')

#-----------просто в одну таблицу записать все сообщения без професии, чтобы потом достать, рассортировать и записать в файл ------------------
    def write_to_one_table(self, results_dict):

        logs.write_log(f"scraping_db: function: write_to_one_table")

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
#     def get_from_bd_for_analyze_python_vs_excel(self):
#         """
#         Get in DB messages and write it to Excel file to check
#         :return: nothing
#         """
#         logs.write_log(f"scraping_db: function: get_from_bd_for_analyze_python_vs_excel")
#
#         profession_alex = []
#         profession_rus = []
#         profession_channel = []
#         profession_title = []
#         profession_body = []
#         profession_alex_tag = []
#         profession_alex_antitag = []
#         profession_rus_tag = []
#
#         if not self.con:
#             self.connect_db()
#
#         cur = self.con.cursor()
#
#         query = f"""SELECT * FROM all_messages WHERE DATE(created_at) > '2022-09-24' ORDER BY time_of_public"""
#         with self.con:
#             cur.execute(query)
#             r = cur.fetchall()
#         for item in r:
#             pro_alex = ''
#             pro_rus = ''
#             created_at = ''
#
#             print('r = ', item)
#             channel = item[1]
#             title = item[2].replace('Обсуждение вакансии в чате @devops_jobs', '')
#             body = item[3].replace('Обсуждение вакансии в чате @devops_jobs', '')
#             time_public = item[5]
#             created_at = item[6]
#             # alex_old = AlexSort().sort_by_profession_by_Alex(title, body)
#             alex = AlexSort2809().sort_by_profession_by_Alex(title, body)
#             rus = AlexRusSort().sort_by_profession_by_AlexRus(title, body)
#
#
#             for pro in alex['profession']:
#                 pro_alex += pro + ' '
#             pro_rus = rus['profession']
#
#             profession_channel.append(channel)
#             profession_alex.append(pro_alex)
#             profession_alex_tag.append(alex['tag'])
#             profession_alex_antitag.append(alex['anti_tag'])
#             profession_rus.append(pro_rus)
#             try:
#                 profession_rus_tag.append(rus['tag'])
#             except:
#                 pass
#             profession_title.append(title)
#             profession_body.append(body)
#
#
#         df = pd.DataFrame(
#             {
#                'channel':  profession_channel,
#                 'pro_Alex_28092022_nigth': profession_alex,
#                 # 'tag_Alex': profession_alex_tag,
#                 # 'antitag_Alex': profession_alex_antitag,
#                 'alternative': profession_rus,
#                 # 'tag_Rus': profession_rus_tag,
#                 'title': profession_title,
#                 'body': profession_body,
#                 'created_at': created_at,
#                 'time_public': time_public,
#             }
#         )
#
#         df.to_excel('all_messages.xlsx', sheet_name='Sheet1')

    def collect_data_for_send_to_bot(self, profession):
        """

        :param profession: get dict and collect phrase type of qa/middle/senior/
        :return: this phrase
        """

        logs.write_log(f"scraping_db: function: collect_data_for_send_to_bot")

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

        logs.write_log(f"scraping_db: function: clear_text_control")

        text = re.sub(r'<[\W\w\d]{1,7}>', '\n', text)
        return text

    def find_last_record(self, response, title_search=None, body_search=None):

        logs.write_log(f"scraping_db: function: find_last_record")

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

    def check_table_companies(self):

        if not self.con:
            self.con = self.connect_db()
        cur = self.con.cursor()
        query = """CREATE TABLE IF NOT EXISTS companies (
            id SERIAL PRIMARY KEY,
            company VARCHAR(100)
            );
            """
        with self.con:
            cur.execute(query)
            print('Table companies has been created or exists')

    def write_to_db_companies(self, companies):

        logs.write_log(f"scraping_db: function: write_to_db_companies")

        con = self.connect_db()
        cur = con.cursor()

        for company in companies:

            # if company is recruiter, is not the company, do not write to DB
            if not re.findall(r'[Рр]екрутер', company):
                if '\'' in company:
                    company = company.replace('\'', '')
                query = f"""SELECT * FROM companies WHERE company='{company}'"""
                with con:
                    try:
                        cur.execute(query)
                        response = cur.fetchall()
                    except Exception as e:
                        print(e)


                if not response:
                    query = f"""INSERT INTO companies (company) VALUES ('{company}')"""
                    with con:
                        try:
                            cur.execute(query)
                            print(f'to put: {company}')
                        except Exception as e:
                            print('Company has not been write to db:\n', e)

    def rewrite_to_archive(self):

        logs.write_log(f"scraping_db: function: rewrite_to_archive")

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

        logs.write_log(f"scraping_db: function: add_columns_to_tables")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        # for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst',
        #                             'fullstack', 'mobile', 'qa', 'hr', 'game', 'ba', 'marketing', 'junior',
        #                             'sales_manager', 'middle', 'senior']:
        for i in ['admin_last_session',]:

            query = f"""ALTER TABLE {i} ADD COLUMN sended_to_agregator VARCHAR(30)"""
            with self.con:
                cur.execute(query)
                print(f'Added agr_link to {i}')

    def output_tables(self):

        logs.write_log(f"scraping_db: function: output_tables")
        tables_list = []
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
                    tables_list.append(i[2])
        print(f'\nвсего записей: {summ}')
        return tables_list

    def delete_table(self, table_name):

        logs.write_log(f"scraping_db: function: delete_table")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        query = f"""DROP TABLE {table_name};"""
        with self.con:
            cur.execute(query)
            print(f'{table_name} was deleted')

    def append_columns(self, table_name_list, column):

        logs.write_log(f"scraping_db: function: append_columns")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        for table in table_name_list:
            # query = f"""ALTER TABLE {table}
            #     ADD COLUMN IF NOT EXISTS vacancy VARCHAR (700),
            #     ADD COLUMN IF NOT EXISTS vacancy_url VARCHAR (150),
            #     ADD COLUMN IF NOT EXISTS company VARCHAR (200),
            #     ADD COLUMN IF NOT EXISTS english VARCHAR (100),
            #     ADD COLUMN IF NOT EXISTS relocation VARCHAR (100),
            #     ADD COLUMN IF NOT EXISTS job_type VARCHAR (700),
            #     ADD COLUMN IF NOT EXISTS city VARCHAR (150),
            #     ADD COLUMN IF NOT EXISTS salary VARCHAR (300),
            #     ADD COLUMN IF NOT EXISTS experience VARCHAR (700),
            #     ADD COLUMN IF NOT EXISTS contacts VARCHAR (500),
            #     ADD COLUMN IF NOT EXISTS agregator_link VARCHAR(200);
            # """
            query = f"""ALTER TABLE {table} 
                ADD COLUMN IF NOT EXISTS {column};
            """
            with self.con:
                cur.execute(query)
                print(f'Added columns to table {table}')

    def run_free_request(self, request, output_text=None):

        logs.write_log(f"scraping_db: function: run_free_request")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        if not output_text:
            output_text = 'free_request has got'

        query = request
        with self.con:
            try:
                cur.execute(query)
                print(output_text)
            except Exception as e:
                print(e)
            pass

    def write_pattern_new(self, key, ma, mex, value, table_name='pattern'):

        logs.write_log(f"scraping_db: function: write_pattern_new")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    key VARCHAR(100),
                    ma BOOLEAN,
                    mex BOOLEAN,
                    value VARCHAR(250)
                    );"""
        with self.con:
            cur.execute(query)

        query = f"""SELECT * FROM {table_name} WHERE key='{key}' AND value='{value}' AND ma={ma} AND mex={mex}"""
        with self.con:
            cur.execute(query)

        if not cur.fetchall():
            query = f"""INSERT INTO {table_name} (key, ma, mex, value) VALUES ('{key}', {ma}, {mex}, '{value}')"""
            with self.con:
                try:
                    cur.execute(query)
                    print(f'add to {table_name} key {key} ma {ma} mex {mex} value {value}')
                except Exception as e:
                    print('error', e)
        else:
            print(f'exists key {key} ma {ma} mex {mex} value {value}')

    def write_pattern2(self, table_name, values):

        logs.write_log(f"scraping_db: function: write_pattern2")

        """
        :param table_name:
        :param values: dict = {'ma': [tuple], 'mex': [tuple]
        :return:
        """

        table_name = f"pattern_{table_name}"
        if not self.con:
            self.connect_db()
        cur = self.con.cursor()
        query = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    mex VARCHAR(70),
                    ma VARCHAR(70)
                    );"""
        with self.con:
            cur.execute(query)

        for i in values['ma']:
            with self.con:
                try:
                    query = f"""SELECT * FROM {table_name} WHERE ma='{i}'"""
                    cur.execute(query)
                except Exception as e:
                    print(e)

                if not cur.fetchall():

                    try:
                        query_ma = f"""INSERT INTO {table_name} (ma) VALUES ('{i}')"""
                        cur.execute(query_ma)
                        print(table_name, i)
                    except Exception as e:
                        print(e)
            pass

        for i in values['mex']:
            with self.con:
                try:
                    query = f"""SELECT * FROM {table_name} WHERE mex='{i}'"""
                    cur.execute(query)
                except Exception as e:
                    print(e)

                if not cur.fetchall():

                    try:
                        query_mex = f"""INSERT INTO {table_name} (mex) VALUES ('{i}')"""
                        cur.execute(query_mex)
                        print(table_name, i)
                    except Exception as e:
                        print(e)
            pass
        pass

    def check_or_create_table_admin(self, cur):

        logs.write_log(f"scraping_db: function: check_or_create_table_admin")

        with self.con:

            cur.execute(f"""CREATE TABLE IF NOT EXISTS admin_last_session (
                            id SERIAL PRIMARY KEY,
                            chat_name VARCHAR(150),
                            title VARCHAR(1000),
                            body VARCHAR (6000),
                            profession VARCHAR (30),
                            vacancy VARCHAR (700),
                            vacancy_url VARCHAR (150),
                            company VARCHAR (200),
                            english VARCHAR (100),
                            relocation VARCHAR (100),
                            job_type VARCHAR (700),
                            city VARCHAR (150),
                            salary VARCHAR (300),
                            experience VARCHAR (700),
                            contacts VARCHAR (500),
                            time_of_public TIMESTAMP,
                            created_at TIMESTAMP,
                            agregator_link VARCHAR(200),
                            session VARCHAR(15),
                            sended_to_agregator VARCHAR(30),
                            FOREIGN KEY (session) REFERENCES current_session(session)
                            );"""
                        )

            self.con.commit()

    def push_to_admin_table(self, results_dict, profession, params=None):
        response_from_db = {}
        logs.write_log(f"scraping_db: function: push_to_admin_table")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()
        self.check_or_create_table_admin(cur)
        pro = ''

        results_dict['title'] = self.clear_title_or_body(results_dict['title'])
        results_dict['body'] = self.clear_title_or_body(results_dict['body'])
        results_dict['company'] = self.clear_title_or_body(results_dict['company'])

        list_prof = profession['profession']
        # print('\n\nНюанс в 805 строке scarping db\n\n')

        if type(list_prof) is set and 'no_sort' not in list_prof:
            list_prof.add('no_sort')
        elif type(list_prof) is list and 'no_sort' not in list_prof:
            list_prof.append('no_sort')

        for i in list_prof:
            # pro += f'{i}, '

            # m---- get response from each prof tables and let True if exists and False if don't ---------
            response = self.get_all_from_db(
                table_name=i,
                param=f"WHERE title='{results_dict['title']}' AND body='{results_dict['body']}'"
            )
            if not response:
                pro += f'{i}, '
            else:
                print(f'This vacancy exists in {i} table')
        if pro:
            results_dict['profession'] = pro[0:-2]

            # check message for exists in admin table
            query_check = f"SELECT * FROM admin_last_session WHERE title='{results_dict['title']}' AND body = '{results_dict['body']}'"
            with self.con:
                cur.execute(query_check)
                r = cur.fetchall()

            if not r:
                response_from_db = False
                new_post = f"""INSERT INTO admin_last_session (
                                    chat_name, title, body, profession, vacancy, vacancy_url, company, english, relocation, job_type, 
                                    city, salary, experience, contacts, time_of_public, created_at, session) 
                                                VALUES ('{results_dict['chat_name']}', '{results_dict['title']}', '{results_dict['body']}', 
                                                '{results_dict['profession']}', '{results_dict['vacancy']}', '{results_dict['vacancy_url']}', '{results_dict['company']}', 
                                                '{results_dict['english']}', '{results_dict['relocation']}', '{results_dict['job_type']}', 
                                                '{results_dict['city']}', '{results_dict['salary']}', '{results_dict['experience']}', 
                                                '{results_dict['contacts']}', '{results_dict['time_of_public']}', '{datetime.now()}', 
                                                '{results_dict['session']}');"""
                with self.con:
                    try:
                        cur.execute(new_post)
                        print(f'+++++++++++++ The vacancy has been added to DB admin_last_session\n')
                    except Exception as e:
                        print(f'-------------- Didn"t push in ADMIN LAST SESSION {e}\n')
                        pass
            else:
                if r:
                    response_from_db = True
                    print(f'!!!!!!!!!!! Message exists in admin_last_session\n')
        else:
            response_from_db = True
            print('NO, Bro')

        return response_from_db


    def push_followers_statistics(self, channel_statistic_dict:dict):

        logs.write_log(f"scraping_db: function: push_followers_statistics")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        for number in range(0, len(channel_statistic_dict['channel'])):
            channel = channel_statistic_dict['channel'][number]
            id_user = channel_statistic_dict['id_user'][number]
            access_hash = channel_statistic_dict['access_hash'][number]
            username = channel_statistic_dict['username'][number]
            first_name = channel_statistic_dict['first_name'][number]
            last_name = channel_statistic_dict['last_name'][number]
            join_time = channel_statistic_dict['join_time'][number]
            is_bot = channel_statistic_dict['is_bot'][number]
            mutual_contact = channel_statistic_dict['mutual_contact'][number]

            print('join_time = ', join_time, type(join_time))
            if type(join_time) is str:
                join_time = join_time.split(' ')
                date = join_time[0].split('-')
                time = join_time[1].split(':')
                join_time = datetime(int(date[2]), int(date[1]), int(date[0]), int(time[0]), int(time[1]), int(time[2]))
            else:
                join_time = None

            with self.con:
                cur.execute(f"""CREATE TABLE IF NOT EXISTS followers_statistics (
                                            id SERIAL PRIMARY KEY,
                                            channel VARCHAR(150),
                                            id_user VARCHAR(30),
                                            access_hash VARCHAR (100),
                                            username VARCHAR (100),
                                            first_name VARCHAR (100),
                                            last_name VARCHAR (100),
                                            join_time TIMESTAMP,
                                            is_bot BOOLEAN,
                                            mutual_contact BOOLEAN
                                            );"""
                            )
            query_check = f"""SELECT * FROM followers_statistics 
                            WHERE channel='{channel}' AND id_user='{id_user}'"""
            with self.con:
                cur.execute(query_check)
                r = cur.fetchall()
            if not r:
                pass
                if join_time:
                    new_participant = f"""INSERT INTO followers_statistics
                                    (channel, id_user, access_hash, username, first_name,
                                    last_name, join_time, is_bot, mutual_contact)
                                    VALUES ('{channel}', '{id_user}', '{access_hash}', '{username}', '{first_name}',
                                    '{last_name}', '{join_time}', {is_bot}, {mutual_contact});"""
                else:
                    new_participant = f"""INSERT INTO followers_statistics
                                                        (channel, id_user, access_hash, username, first_name,
                                                        last_name, is_bot, mutual_contact)
                                                        VALUES ('{channel}', '{id_user}', '{access_hash}', '{username}', '{first_name}',
                                                        '{last_name}', {is_bot}, {mutual_contact});"""

                with self.con:
                    cur.execute(new_participant)
                    print(f'{id_user} in {channel} was writed')
            else:
                print(f'{id_user} in {channel} exists already')

    def try_and_delete_after(self):

        logs.write_log(f"scraping_db: function: try_and_delete_after")

        a = 'Mother"s fucker'
        b = f"Mother's fucker 2"
        if not self.con:
            self.connect_db()
        cur = self.con.cursor()
        query = """CREATE TABLE IF NOT EXISTS try (
                    id SERIAL PRIMARY KEY,
                    value VARCHAR(1000)
                    );"""
        with self.con:
            cur.execute(query)
            print('Table created or exists')

        query = f"""INSERT INTO try (value) VALUES ('{a}')"""
        with self.con:
            try:
                cur.execute(query)
                print('Data was creating')
            except Exception as e:
                print(f'Error ', e)

    def add_password_to_user(self, id, password):

        logs.write_log(f"scraping_db: function: add_password_to_user")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        query = f"""UPDATE users SET password='{password}' WHERE id={id}"""
        try:
            with self.con:
                cur.execute(query)
                print('password added')
        except Exception as e:
            print('Something is wrong ', e)

    def create_table_users(self):
        if not self.con:
            self.connect_db()
        cur = self.con.cursor()
        with self.con:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                id_user VARCHAR(20),
                api_id VARCHAR(20),
                api_hash VARCHAR (50),
                phone_number VARCHAR (25),
                password VARCHAR (100)
                );"""
                        )


    def write_user_without_password(self, id_user, api_id, api_hash, phone_number):
        logs.write_log(f"scraping_db: function: write_user_without_password")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        self.create_table_users()

        query_does_user_exist = f"""SELECT * FROM users WHERE api_id='{api_id}'"""
        with self.con:
            cur.execute(query_does_user_exist)
        r= cur.fetchall()

        if not r:
            query = f"""INSERT INTO users (id_user, api_id, api_hash, phone_number) VALUES ('{id_user}', '{api_id}', '{api_hash}', '{phone_number}')"""
            try:
                with self.con:
                    cur.execute(query)
                    print(f'user {id_user}has been added to db')
            except Exception as e:
                print(f"Didn't write the user to db. Because: {e}")
        else:
            print('user exists')

    def change_type_column(self, list_table_name, name_and_type='title VARCHAR(4096)'):

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()

        for table_name in list_table_name:
            # query_for_change_type = f"""ALTER TABLE {table_name} ALTER COLUMN {name_and_type}"""

            query_for_change_type = f"""ALTER TABLE {table_name} ALTER COLUMN {name_and_type}"""
            with self.con:
                try:
                    cur.execute(query_for_change_type)
                    print(f'changed title in {table_name}')
                except Exception as e:
                    print(f"title in {table_name} didn't change for reason {e}")

    def check_admin_temporary(self, cur):
        cur = self.con.cursor()
        with self.con:

            cur.execute(f"""CREATE TABLE IF NOT EXISTS admin_temporary (
                            id SERIAL PRIMARY KEY,
                            id_admin_channel VARCHAR(20),
                            id_admin_last_session_table VARCHAR(20),
                            sended_to_agregator VARCHAR(30)
                            );"""
                        )

    def push_to_admin_temporary(self, composed_message_dict):

        logs.write_log(f"scraping_db: function: push_to_admin_temporary")

        if not self.con:
            self.connect_db()
        cur = self.con.cursor()
        self.check_admin_temporary(cur)

        id_admin_channel = composed_message_dict['id_admin_channel']
        id_admin_last_session_table = composed_message_dict['db_id']
        it_was_sending_to_agregator = composed_message_dict['it_was_sending_to_agregator']

        query_check = f"""SELECT * FROM admin_temporary 
                WHERE id_admin_channel='{id_admin_channel}' 
                AND id_admin_last_session_table = '{id_admin_last_session_table}'"""

        with self.con:
            cur.execute(query_check)

        if not cur.fetchall():

            query = f"""INSERT INTO admin_temporary (id_admin_channel, id_admin_last_session_table, sended_to_agregator)
                        VALUES ('{id_admin_channel}', '{id_admin_last_session_table}', '{it_was_sending_to_agregator}')"""

            with self.con:
                try:
                    cur.execute(query)
                    print(f'Writed to admin_temporary {id_admin_channel}-{id_admin_last_session_table}-{it_was_sending_to_agregator}')
                except Exception as e:
                    print('Error in admin_temporary ', e)

        else:
            print('Record exists in admin_temporary')

        pass

    def drop_profession_in_admin_db(self, drop_profession):
        current_id_agregator = ''
        query2 = ''
        response_admin_temporary = self.get_all_from_db('admin_temporary', without_sort=True)
        for item in response_admin_temporary:
            new_profession = ''
            id_db = item[2]
            query = f"""SELECT profession FROM admin_last_session WHERE id={id_db}"""
            if not self.con:
                self.connect_db()
            cur = self.con.cursor()
            with self.con:
                cur.execute(query)
            r = cur.fetchall()
            if r:
                professions = r[0][0].split(',')
                print(professions, len(professions))

                if len(professions)>1:
                    for i in professions:
                        i = i.strip()
                        if i != drop_profession:
                            new_profession += f'{i}, '
                    new_profession = new_profession[:-2]
                    query = f"""UPDATE admin_last_session SET profession='{new_profession}' WHERE id={id_db}"""
                    query2 = f"""UPDATE admin_last_session SET sended_to_agregator='{current_id_agregator}' WHERE id={id_db}"""
                else:
                    query = f"""DELETE FROM admin_last_session WHERE id={id_db}"""

                with self.con:
                    try:
                        cur.execute(query)
                        print('got it')
                        if query2:
                            cur.execute(query)
                            print('got it 2')
                    except Exception as e:
                        print('Not changing profession ', e)

        self.delete_table('admin_temporary')
        pass
