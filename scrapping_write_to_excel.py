import configparser
from datetime import datetime, time, timedelta
import random
import time
import pandas as pd
import os
from telethon import TelegramClient
# from database_settings import database, user, password, host, port
import psycopg2

config = configparser.ConfigParser()
config.read("config.ini")
api_id = int(config['Telegram']['api_id'])
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']
bot = config['My_channels']['bot']
phone = '+375296449690'

client = TelegramClient(username, api_id, api_hash)
client.start()


def write_to_excel(chat_name, title, body, pro, time_of_public, created_at):

    df = pd.DataFrame(
        {
            'chat_name': chat_name,
            'title': title,
            'body': body,
            'profession': pro,
            'time_of_public': time_of_public,
            'created_at': created_at
        }
    )

    df.to_excel(f'./participants_from_{pro}.xlsx', engine='openpyxl', mode='a', sheet_name='Sheet1')

def connect_db():
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


def write_to_excel_from_db():
    con = connect_db()

    cur = con.cursor()

    profession_list = ['backend', 'frontend', 'qa', 'fullstack', 'designer', 'mobile', 'pm',
                       'product', 'game', 'ba', 'devops', 'marketing', 'hr', 'ad', 'no_sort',
                       'junior', 'middle', 'senior']

    for pro in profession_list:

        query = f"""SELECT * FROM {pro} WHERE created_at > {datetime(13, 9, 14, 0, 0, 0, 0).strftime('%Y-%m-%d %H:%M:%S.%MS')}"""
        with con:
            cur.execute(query)
            r = cur.fetchall()

            chat_name = [str(i[1]) for i in r]
            title = [str(i[2]) for i in r]
            body = [str(i[3]) for i in r]
            profession = [str(i[4]) for i in r]
            time_of_public = [str(i[5]) for i in r]
            created_at = [str(i[6]) for i in r]

            df = pd.DataFrame(
                {
                    'chat_name': chat_name,
                    'title': title,
                    'body': body,
                    'profession': profession,
                    'time_of_public': time_of_public,
                    'created_at': created_at
                }
            )

            # df.to_excel(f'./participants_from_{pro}.xlsx', engine='openpyxl', mode='a', sheet_name='Sheet1')
            df.to_excel(f'./messages/{pro}_messages.xlsx', sheet_name='Sheet1')

async def send_to_channel_from_db():
    con = connect_db()
    cur = con.cursor()

    profession_list = ['frontend', 'qa', 'fullstack', 'designer', 'mobile', 'pm', 'product', 'game', 'ba',
                       'devops', 'marketing', 'hr', 'ad', 'no_sort']

    for pro in profession_list:
        query = f"""SELECT * FROM {pro}"""
        with con:
            cur.execute(query)
            r = cur.fetchall()
            for i in r:
                print(i[5])
                if i[5]>datetime(2022, 8, 11, 0, 0):
                    print(i)
                    await client.send_message(entity=bot, message=f"{pro}/{i[2]}{i[3]}")
                    time.sleep(random.randrange(25,40))

                else:
                    print('data is not valid', i)


# with client:
#     client.loop.run_until_complete(send_to_channel_from_db())
# send_to_channel_from_db()

write_to_excel_from_db()