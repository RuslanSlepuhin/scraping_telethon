import re

import psycopg2

from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
from db_operations.scraping_db import DataBaseOperations
# from scraping_telegramchats2 import WriteToDbMessages
import pandas as pd

database2 = 'fake'
user2 = 'ruslan'
password2 = '12345'
host2 = 'localhost'
port2 = '5432'

con2 = psycopg2.connect(
    database=database2,
    user=user2,
    password=password2,
    host=host2,
    port=port2
)
cur = con2.cursor()

valid_profession_list = ['marketing', 'ba', 'game', 'product', 'mobile',
                                  'pm', 'sales_manager', 'analyst', 'frontend',
                                  'designer', 'devops', 'hr', 'backend', 'qa', 'junior']

def delete_since(tables_list=None, ids_list=None, param=None):
    """
    delete records since time in params in tables in list[]
    """
    """
    DATE(created_at) > '2022-09-24'
    """
    if not tables_list:
        tables_list = ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst', 'mobile', 'qa', 'hr', 'game',
              'ba', 'marketing', 'junior', 'sales_manager', 'no_sort', 'admin_last_session']
    for i in tables_list:
        if not ids_list:
            DataBaseOperations(None).delete_data(table_name=i, param=param)
        else:
            for id in ids_list:
                DataBaseOperations(None).delete_data(table_name=i, param=f"WHERE id={id}")
                print(f'Was deleted id={id} from {i}')

def write_pattern_to_db():
    from patterns.pattern_Alex2809 import pattern

    for key in pattern:
        for ma_or_mex in pattern[key]:
            if ma_or_mex == 'ma':
                ma = True
                mex = False
            else:
                ma = False
                mex = True

            for value in pattern[key][ma_or_mex]:
                DataBaseOperations(None).write_pattern_new(key=key, ma=ma, mex=mex, value=value)
                pass
            pass

def show_all_tables():
    DataBaseOperations(None).output_tables()

def delete_tables(tables_delete=None):
    # if not tables_delete:
    #     tables_delete = ['followers_statistics',]

    for i in tables_delete:
        DataBaseOperations(None).delete_table(i)

def show_all():
    response = DataBaseOperations(None).get_all_from_db('mex', without_sort=True)
    # print(response)
    for i in response:
        print(i)

def append_columns():
    DataBaseOperations(None).append_columns(['admin_temporary',], column='sended_to_agregator VARCHAR(30)')

    # DataBaseOperations(None).append_columns(
    #     ['marketing', 'ba', 'game', 'product', 'mobile', 'pm', 'sales_manager', 'analyst', 'frontend', 'designer',
    #      'devops', 'hr', 'backend', 'qa', 'junior'], column='session VARCHAR(15)')
        # "current_session VARCHAR(15)\nFOREIGN KEY(current_session) \nPREFERENCES current_session(session) ON DELETE CASCADE")
    pass

def run_free_request():
    for table_name in ['marketing', 'ba', 'game', 'product', 'mobile', 'pm', 'sales_manager', 'analyst', 'frontend',
                       'designer',
                       'devops', 'hr', 'backend', 'qa', 'junior']:
        request = f"""
            ALTER TABLE {table_name} ADD FOREIGN KEY (session) REFERENCES current_session(session);
        """
        DataBaseOperations(None).run_free_request(request=request)

def show_how_much_from_source():
    count = 1
    for item in ['marketing', 'ba', 'game', 'product', 'mobile', 'pm', 'sales_manager', 'analyst', 'frontend', 'designer',
             'devops', 'hr', 'backend', 'qa', 'junior', 'no_sort']:
        response = DataBaseOperations(None).get_all_from_db(table_name=item, param="WHERE chat_name='https://finder.vc'")

        for i in response:
            print(count, item, i[2][0:40], i[1], i[5].strftime('%Y-%m-%d %H:%M'))
            count += 1

def filter_text_from_file_txt():
    companies = DataBaseOperations(None).get_all_from_db('companies', without_sort=True)
    with open('./../file.txt', 'r', encoding='utf-8') as file:
        text = file.read()

    text = text.split(f'\n', 1)
    title = text[0]
    body = text[1]
    print(title)
    print(body)
    result = AlexSort2809().sort_by_profession_by_Alex(title, body, companies)
    print(f"\n{result['profession']['profession']}")

def find_message_in_db_write_to_file_get_prof():
    search = 'Network administration'
    where = 'body'
    response = DataBaseOperations(None).get_all_from_db('backend', param=f"WHERE {where} LIKE '%{search}%'")
    print('len = ', len(response))

    if len(response) == 2:
        if response[0][2] == response[1][2] and response[0][3] == response[1][3]:
            print('DOUBLE!!?')
        else:
            print('different')

    # if len(response)==2:
    for i in range(0, len(response)):

        message = response[i][2] + response[i][3]
        with open(f'./../file{i}.txt', 'w', encoding='utf-8') as file:
            file.write(message)

    print(response)
    filter_text_from_file_txt()

def try_and_delete_after():
    DataBaseOperations(None).try_and_delete_after()

def get_double_records():
    for item in ['marketing', 'ba', 'game', 'product', 'mobile', 'pm', 'sales_manager', 'analyst', 'frontend', 'designer',
             'devops', 'hr', 'backend', 'qa', 'junior', 'no_sort']:

        response = DataBaseOperations(None).get_all_from_db(item)
        counter = 0

        with open('./../logs/logs_double.txt', 'w') as file:
            file.write('')

        for record in range(0, len(response)):
            print(f'\nin {item} getting {response[record][0]} {response[record][2][0:40]}')

            for other_record in range(counter+1, len(response)):
                if response[record][2].replace('\'', '').replace('\"', '') == response[other_record][2].replace('\'', '').replace('\"', '') \
                        and response[record][3].replace('\'', '').replace('\"', '') == response[other_record][3].replace('\'', '').replace('\"', ''):

                    print(f'Match in {response[record][0]} and {response[other_record][0]}')

                    table = item
                    id_element = response[record][0]
                    id_match = response[other_record][0]
                    title_element = response[record][2][0:40]
                    title_match = response[other_record][2][0:40]
                    body_element = response[record][3][0:40]
                    body_match = response[other_record][3][0:40]



                    with open('./../logs/logs_double.txt', 'a', encoding="utf-8") as file:
                        # file.write(f'table={item}\n{str(id_element)}, title={title_element}, body={body_element}')
                        file.write(f'\nMatch in {item}:\n'
                                   f'id={str(id_element)}, title={title_element}, body={body_element}\n'
                                   f'id_match={str(id_match)}, title_match={title_match}, body_match={body_match}\n\n')

            counter += 1

def send_fulls(time_start=None):

    current_session = DataBaseOperations(None).get_all_from_db(
        table_name='current_session',
        param='ORDER BY id DESC LIMIT 1',
        without_sort=True,
        order=None,
        field='session',
        curs=None
    )
    for value in current_session:
        current_session = value[0]

    profession_list = {}
    profession_list['profession'] = []
    results_dict = {}

    response_messages = DataBaseOperations(None).get_all_from_db('admin_last_session', param=f"WHERE session='{current_session}'")

    for message in response_messages:
        pro = message[4].split(',')
        for i in pro:
            profession_list['profession'].append(i.strip())

        results_dict['chat_name'] = message[1]
        results_dict['title'] = message[2]
        results_dict['body'] = message[3]
        results_dict['profession'] = message[4]
        results_dict['vacancy'] = message[5]
        results_dict['vacancy_url'] = message[6]
        results_dict['company'] = message[7]
        results_dict['english'] = message[8]
        results_dict['relocation'] = message[9]
        results_dict['job_type'] = message[10]
        results_dict['city'] = message[11]
        results_dict['salary'] = message[12]
        results_dict['experience'] = message[13]
        results_dict['contacts'] = message[14]
        results_dict['time_of_public'] = message[15]
        results_dict['created_at'] = message[16]
        results_dict['agregator_link'] = message[17]
        results_dict['session'] = message[18]
        pass
        response_from_db = DataBaseOperations(None).push_to_bd(results_dict, profession_list, agregator_id=145)

def change_column(list_table_name):
    db=DataBaseOperations(None)
    db.change_type_column(list_table_name=list_table_name)

def check_english():
    with open('../file.txt', 'r') as f:
        text = f.read()

    from patterns.pattern_Alex2809 import english_pattern

    for i in text:
        match = re.findall(english_pattern, i)
        if match:
            print('match = ', match)
        else:
            print('no match')

def check_company():
    profession = 'backend'
    response = DataBaseOperations(None).get_all_from_db('admin_last_session', without_sort=True)
    for i in response:
        # for j in i:
        #     print('field = ', j)
        """
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
        """
        title = i[2]
        body = i[3]
        vacancy = i[5]
        company = i[7]
        english = i[8]
        relocation = i[9]
        job_type = i[10]
        city = i[11]
        sended_to_agregator = i[19]

        prof = AlexSort2809().sort_by_profession_by_Alex(title, body)
        params = prof['params']

        print("\n it's instance -------------------------------------------------\n")

        message_for_send = ''
        if vacancy:
            message_for_send += f"Вакансия1: {vacancy}\n"

        if company:
            message_for_send += f"Компания1: {company}\n"
        elif params['company_hiring']:
            message_for_send += f"Компания2: {params['company_hiring']}\n"

        if city:
            message_for_send += f"Город/страна1: {company}\n"

        if english:
            message_for_send += f"English1: {english}\n"
        elif params['english']:
            message_for_send += f"English2: {params['english']}\n"

        if job_type:
            message_for_send += f"Тип работы1: {job_type}\n"
        elif params['jobs_type']:
            message_for_send += f"Тип работы2: {params['jobs_type']}\n"

        if relocation:
            message_for_send += f"Релокация1: {relocation}\n"
        elif params['relocation']:
            message_for_send += f"Релокация2: {params['relocation']}\n"
        if sended_to_agregator and sended_to_agregator != "None":
            message_for_send += f"https://t.me/it_jobs_agregator/{sended_to_agregator}"


        print(message_for_send)

        with open("../excel/shorts", "a", encoding='utf-8') as file:
            file.write(f"{message_for_send}\n--------------------------------\n")

        pass

def get_companies():
    response = DataBaseOperations(None).get_all_from_db('companies', without_sort=True)
    for i in response:
        print(i)

def nbsp():
    with open("./../logs/logs_errors.txt", "r", encoding='utf-8') as file:
        text = file.read()
        print(text, '\n\n\n')
        match = re.findall(r'\xa0', text)
        print('match', match)
        text = text.replace('\xa0', ' ')
        print(text)

# delete_since(param="WHERE DATE(created_at) >'2022-11-13'")

def copy_companies_to_local_db():
    database1 = 'd2tmbiujurbrcr'
    user1 = 'ljgsrnphxwbfsg'
    password1 = '7546fe6db78dc036f71813646989a02f0a37d8afbe4ca1ab5cc6fa38f9125f57'
    host1 = 'ec2-54-220-255-121.eu-west-1.compute.amazonaws.com'
    port1 = '5432'

    con1 = psycopg2.connect(
        database=database1,
        user=user1,
        password=password1,
        host=host1,
        port=port1
    )

    companies = DataBaseOperations(con1).get_all_from_db(
        table_name='companies',
        without_sort=True
    )
    database2 = 'd2tmbiujurbrcr'
    user2 = 'ljgsrnphxwbfsg'
    password2 = '7546fe6db78dc036f71813646989a02f0a37d8afbe4ca1ab5cc6fa38f9125f57'
    host2 = 'ec2-54-220-255-121.eu-west-1.compute.amazonaws.com'
    port2 = '5432'

    con2 = psycopg2.connect(
        database=database2,
        user=user2,
        password=password2,
        host=host2,
        port=port2
    )
    companies_list = []
    for i in companies:
        companies_list.append(i[1])

    DataBaseOperations(con2).write_to_db_companies(companies_list)

    print('the end of copy the companies')

def create_all_bd_clone_fake():
    valid_profession_list = ['marketing', 'ba', 'game', 'product', 'mobile',
                                  'pm', 'sales_manager', 'analyst', 'frontend',
                                  'designer', 'devops', 'hr', 'backend', 'qa', 'junior']
    valid_profession_list.append('no_sort')
    valid_profession_list = ['fullstack',]
    database2 = 'fake'
    user2 = 'ruslan'
    password2 = '12345'
    host2 = 'localhost'
    port2 = '5432'

    con2 = psycopg2.connect(
        database=database2,
        user=user2,
        password=password2,
        host=host2,
        port=port2
    )
    cur = con2.cursor()
    for i in valid_profession_list:
        DataBaseOperations(con2).check_or_create_table(cur=cur, table_name=i)

def write_to_excel_from_proff_and_nosort():
    vacancies_dict = {}
    vacancies_dict['chat_name']=[]
    vacancies_dict['title']=[]
    vacancies_dict['body']=[]
    vacancies_dict['profession']=[]
    vacancies_dict['vacancy']=[]
    vacancies_dict['vacancy_url']=[]
    vacancies_dict['company']=[]
    vacancies_dict['english']=[]
    vacancies_dict['relocation']=[]
    vacancies_dict['job_type']=[]
    vacancies_dict['city']=[]
    vacancies_dict['salary']=[]
    vacancies_dict['experience']=[]
    vacancies_dict['contacts']=[]
    response = DataBaseOperations(con2).get_all_from_db(
        table_name='admin_last_session',
        without_sort=True
    )
    for i in response:
        vacancies_dict['chat_name'].append(i[1])
        vacancies_dict['title'].append(i[2].replace('​', ''))
        vacancies_dict['body'].append(i[3])
        vacancies_dict['profession'].append(i[4])
        vacancies_dict['vacancy'] .append(i[5])
        vacancies_dict['vacancy_url'].append(i[6])
        vacancies_dict['company'].append(i[7])
        vacancies_dict['english'].append(i[8])
        vacancies_dict['relocation'].append(i[9])
        vacancies_dict['job_type'] .append(i[10])
        vacancies_dict['city'].append(i[11])
        vacancies_dict['salary'].append(i[12])
        vacancies_dict['experience'].append(i[13])
        vacancies_dict['contacts'].append(i[14])

    df = pd.DataFrame(
        {
            'chat_name': vacancies_dict['chat_name'],
            'title': vacancies_dict['title'],
            'body': vacancies_dict['body'],
            'profession': vacancies_dict['profession'],
            'vacancy': vacancies_dict['vacancy'],
            'vacancy_url': vacancies_dict['vacancy_url'],
            'company': vacancies_dict['company'],
            'english': vacancies_dict['english'],
            'relocation': vacancies_dict['relocation'],
            'job_type': vacancies_dict['job_type'],
            'city': vacancies_dict['city'],
            'salary': vacancies_dict['salary'],
            'experience': vacancies_dict['experience'],
            'contacts': vacancies_dict['contacts'],
        }
    )

    df.to_excel(f'./../excel/for_checking.xlsx', sheet_name='Sheet1')
    print('got it')

def rewrite_vacancy():
    from patterns.pattern_Alex2809 import vacancy_name
    excel_data_df = pd.read_excel("./../excel/for_checking.xlsx", sheet_name='Sheet1')
    excel_dict = {
            'chat_name': excel_data_df['chat_name'].tolist(),
            'title': excel_data_df['title'].tolist(),
            'body': excel_data_df['body'].tolist(),
            'profession': excel_data_df['profession'].tolist(),
            'vacancy': excel_data_df['vacancy'].tolist(),
            'vacancy_url': excel_data_df['vacancy_url'].tolist(),
            'company': excel_data_df['company'].tolist(),
            'english': excel_data_df['english'].tolist(),
            'relocation': excel_data_df['relocation'].tolist(),
            'job_type': excel_data_df['job_type'].tolist(),
            'city': excel_data_df['city'].tolist(),
            'salary': excel_data_df['salary'].tolist(),
            'experience': excel_data_df['experience'].tolist(),
            'contacts': excel_data_df['contacts'].tolist(),
    }

    for i in excel_dict['body']:
        index = excel_dict['body'].index(i)
        title = excel_dict['title'][index]

        if not i and title:
            text = title
        else:
            text = title + str(i)
        match = re.findall(rf"{vacancy_name}", text)
        print('match: ', match)
        excel_dict['vacancy'][index] = match

    df = pd.DataFrame(
        {
            'chat_name': excel_dict['chat_name'],
            'title': excel_dict['title'],
            'body': excel_dict['body'],
            'profession': excel_dict['profession'],
            'vacancy': excel_dict['vacancy'],
            'vacancy_url': excel_dict['vacancy_url'],
            'company': excel_dict['company'],
            'english': excel_dict['english'],
            'relocation': excel_dict['relocation'],
            'job_type': excel_dict['job_type'],
            'city': excel_dict['city'],
            'salary': excel_dict['salary'],
            'experience': excel_dict['experience'],
            'contacts': excel_dict['contacts'],
        }
    )
    df.to_excel(f'./../excel/for_checking.xlsx', sheet_name='Sheet1')
    print('got it ')



# response = DataBaseOperations(None).get_all_from_db(
#     table_name='marketing',
#     param="WHERE DATE(created_at) >'2022-11-13'",
#     without_sort=True
# )
# for i in response:
#     print(i)
# print(len(response))
# copy_companies_to_local_db()
write_to_excel_from_proff_and_nosort()
rewrite_vacancy()
# create_all_bd_clone_fake()