import configparser
import json
import time

import psycopg2
from datetime import datetime
from scraping_get_profession import Professions
config = configparser.ConfigParser()
config.read("config.ini")

# ---------------------DB operations ----------------------
class DataBaseOperations:

    def __init__(self, con):
        self.con = con

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
        #     cur.execute("""CREATE TABLE IF NOT EXISTS channels_table3 (
        #                 id SERIAL PRIMARY KEY,
        #                 channel VARCHAR(100),
        #                 link VARCHAR (100)
        #                 );"""
        #     )
        #     con.commit()
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

        # with con:
        #     channel_link = f'https://t.me/{channel_username}'
        #     query = f"""SELECT * FROM channels_table3 WHERE channel='{channel_name}'"""
        #     cur.execute(query)
        #     response = cur.fetchall()
        #
        #     if not response:
        #         new_channel = f"""INSERT INTO channels_table3 (channel, link)
        #                                     VALUES ('{channel_name}', '{channel_link}');"""
        #         try:
        #             cur.execute(new_channel)
        #             con.commit()
        #             print('*********add to channels ', channel_name, channel_link)
        #         except Exception as e:
        #             print(e)
        #     else:
        #         print('This CHANNEL exist already')

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

        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        print(f'\n****************************************\n'
              f'INPUT IN DB FUNC = \n', results_dict)

        chat_name = results_dict['chat_name']
        title = results_dict['title'].replace(f'\'', '"')
        body = str(results_dict['body']).replace(f'\'', '"')

        prof = Professions()
        params = prof.sort_by_profession(title, body)
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
        created_at = datetime.now()

        self.check_or_create_table(cur, pro)
        self.quant = 1

# ------------ add to db table's name pro message ----------------
        with self.con:
            try:
                query = f"""SELECT * FROM {pro} WHERE title='{title}' AND body='{body}'"""
                cur.execute(query)
                r = cur.fetchall()

                if not r:
                    response_dict[pro] = False

                    new_post = f"""INSERT INTO {pro} (chat_name, title, body, profession, time_of_public, created_at) 
                                VALUES ('{chat_name}', '{title}', '{body}', '{pro}', '{time_of_public}', '{created_at}');"""
                    cur.execute(new_post)
                    self.con.commit()
                    print(self.quant, f'= Added to DB {pro}\n')
                    self.quant += 1

                else:
                    response_dict[pro] = True
                    print(self.quant, f'!!!! This message exists already in {pro}\n')

            except Exception as e:
                print('Dont push in db, error = ', e)
                # return response_dict['error', e]

# ------------ add to db table's name params message ----------------

        for param in params:
            if param not in ['profession', 'block'] and params[param]:

                self.check_or_create_table(cur, param)

                with self.con:
                    try:
                        query = f"""SELECT * FROM {param} WHERE title='{title}' AND body='{body}'"""
                        cur.execute(query)
                        r = cur.fetchall()

                        if not r:
                            response_dict[param] = False

                            new_post = f"""INSERT INTO {param} (chat_name, title, body, profession, time_of_public, created_at) 
                                        VALUES ('{chat_name}', '{title}', '{body}', '{pro}', '{time_of_public}', '{created_at}');"""
                            cur.execute(new_post)
                            self.con.commit()
                            print(self.quant, f'= Added to DB {param}\n')
                            self.quant += 1
                        else:
                            print(self.quant, f'!!!!! This message exists already in {param}\n')
                            response_dict[param] = True

                    except Exception as e:
                        print('Dont push in db, error = ', e)
                        # return response_dict['error', e]

        time.sleep(1)

        return response_dict


    def get_all_from_db(self):
        if not self.con:
            self.connect_db()

        cur = self.con.cursor()

        query = """SELECT * FROM devops"""
        with self.con:
            cur.execute(query)
            r = cur.fetchall()
            n=0
            for i in r:
                print(n)
                n += 1
                for j in i:
                    print(j)

# DataBaseOperations().get_all_from_db()