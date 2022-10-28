
from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
from db_operations.scraping_db import DataBaseOperations
from scraping_telegramchats2 import WriteToDbMessages

def delete_since():
    """
    delete records since time in params in tables in list[]
    """
    for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst', 'mobile', 'qa', 'hr', 'game',
              'ba', 'marketing', 'junior', 'sales_manager', 'no_sort', 'admin_last_session']:
        DataBaseOperations(None).delete_data(table_name=i, param="WHERE created_at > '2022-10-25 06:00:00'")


def write_pattern_to_db():
    from patterns.pattern_Alex2809 import pattern

    for tag in pattern:
        for ma_or_mex in pattern[tag]:
            for value in pattern[tag][ma_or_mex]:
                DataBaseOperations(None).write_pattern_new(table_name=ma_or_mex, tag=tag, value=value)

        pass

def show_all_tables():
    DataBaseOperations(None).output_tables()

def delete_tables(tables_delete=None):
    if not tables_delete:
        tables_delete = ['followers_statistics',]

    for i in tables_delete:
        DataBaseOperations(None).delete_table(i)

def show_all():
    response = DataBaseOperations(None).get_all_from_db('mex', without_sort=True)
    # print(response)
    for i in response:
        print(i)

def append_columns():
    DataBaseOperations(None).append_columns(
        ['marketing', 'ba', 'game', 'product', 'mobile', 'pm', 'sales_manager', 'analyst', 'frontend', 'designer',
         'devops', 'hr', 'backend', 'qa', 'junior'], column='session VARCHAR(15)')
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


# response = DataBaseOperations(None).get_all_from_db('qa', param="WHERE vacancy <> ''")
# for i in range(len(response)-1, len(response)):
#     print('chat_name = ', response[i][1])
#     print('title = ', response[i][2][0:40])
#     print('body = ', response[i][3][0:40])
#     print('profession = ', response[i][4])
#     print('time_of_public = ', response[i][5])
#     print('created_at = ', response[i][6])
#     print('agregator_link = ', response[i][7])
#     print('vacancy = ', response[i][8])
#     print('vacancy_url = ', response[i][9])
#     print('company = ', response[i][10])
#     print('english = ', response[i][11])
#     print('relocation = ', response[i][12])
#     print('job_type = ', response[i][13])
#     print('city = ', response[i][14])
#     print('salary = ', response[i][15])
#     print('experience = ', response[i][16])
#     print('contacts = ', response[i][17])
#     print('session = ', response[i][18])

response = DataBaseOperations(None).get_all_from_db('users', without_sort=True)
for i in response:
    print(i)
