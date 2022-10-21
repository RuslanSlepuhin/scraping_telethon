
from filters.scraping_get_profession_Alex_next_2809 import AlexSort2809
from db_operations.scraping_db import DataBaseOperations

def delete_since():
    """
    delete records since time in params in tables in list[]
    """
    for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst', 'mobile', 'qa', 'hr', 'game',
              'ba', 'marketing', 'junior', 'sales_manager', 'no_sort']:
        DataBaseOperations(None).delete_data(table_name=i, param="WHERE created_at > '2022-10-19 06:00:00'")


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
        tables_delete = ['pattern_product']

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