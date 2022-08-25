import configparser
import json

import psycopg2
from datetime import datetime, timedelta
from database_settings import database, user, password, host, port
from scraping_get_profession import Professions
# from scrapping_write_to_excel import write_to_excel
import time
config = configparser.ConfigParser()
config.read("config.ini")

# ---------------------DB operations ----------------------
class DataBaseOperations:

    def connect_db(self):

        con = None
        database = config['DB3']['database']
        user = config['DB3']['user']
        password = config['DB3']['password']
        host = config['DB3']['host']
        port = config['DB3']['port']
        try:
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

    #-------------participants-------------------------
    def push_to_bd_participants(self, participant, all_user_dictionary, channel_name, channel_username):
        con = self.connect_db()
        cur = con.cursor()

        with con:
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
            con.commit()

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

        with con:

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
                        con.commit()
                        print('!!!!!!!!!!!!!add to users ', i)
                    except Exception as e:
                        print(e)
                else:
                    print('This user exist already', i)

    #--------------------------------------------------

    def push_to_bd(self, results_dict):

        or_exists = False
        global quant


        con = self.connect_db()
        cur = con.cursor()

        print(f'\n****************************************\n'
              f'INPUT IN DB FUNC = \n', results_dict)

        chat_name = results_dict['chat_name']
        title = results_dict['title'].replace(f'\'', '"')
        body = str(results_dict['body']).replace(f'\'', '"')

        prof = Professions()
        pro = prof.sort_by_profession3(title, body)
        print('profession = ', pro)

        time_of_public = results_dict['time_of_public']
        created_at = datetime.now()

        with con:

            cur.execute(f"""CREATE TABLE IF NOT EXISTS {pro} (
                id SERIAL PRIMARY KEY,
                chat_name VARCHAR(150),
                title VARCHAR(1000),
                body VARCHAR (6000),
                profession VARCHAR (30),
                time_of_public TIMESTAMP,
                created_at TIMESTAMP
                );"""
                        )
            con.commit()
        quant = 1
        with con:
            try:
                query = f"""SELECT * FROM {pro} WHERE title='{title}' AND body='{body}'"""
                cur.execute(query)
                r = cur.fetchall()

                if not r:
                    new_post = f"""INSERT INTO {pro} (chat_name, title, body, profession, time_of_public, created_at) 
                                VALUES ('{chat_name}', '{title}', '{body}', '{pro}', '{time_of_public}', '{created_at}');"""
                    cur.execute(new_post)
                    con.commit()
                    print(quant, f'= Added to DB = {pro}\n', results_dict)
                    or_exists = True
                    quant += 1
                else:
                    print(quant, f'This message exists already = ')
                    or_exists = False

            except Exception as e:
                print('Dont push in db, error = ', e)

            return {'or_exists': or_exists, 'profession': pro}


    def get_all_from_db(self):
        con = self.connect_db()

        cur = con.cursor()

        query = """SELECT * FROM devops"""
        with con:
            cur.execute(query)
            r = cur.fetchall()
            n=0
            for i in r:
                print(n)
                n += 1
                for j in i:
                    print(j)

# DataBaseOperations().get_all_from_db()