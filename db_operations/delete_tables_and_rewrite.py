import re

from db_operations.scraping_db import DataBaseOperations


def rewrite_old_pro_db():
    """
    delete old pro db,
    get all messages from 'all_messages'
    check the profession and
    write to pro db

    """
def delete_and_check_status_tables():
    for i in ['backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'fullstack', 'mobile', 'qa', 'hr',
                  'game', 'ba', 'marketing', 'junior', 'middle', 'senior', 'ad', 'sales_manager', 'analyst']:
        DataBaseOperations(con=None).delete_data(table_name=i)
        query = f"""SELECT * FROM {i}"""
        con = DataBaseOperations(con=None).connect_db()
        cur = con.cursor()
        with con:
            cur.execute(query)
            response = cur.fetchall()
        if response:
            print(f'{i} isnt empty')
        else:
            print(f'{i} is empty')




    con = DataBaseOperations(con=None).connect_db()

    cur = con.cursor()

    query = """SELECT * FROM all_messages ORDER BY time_of_public"""
    with con:
        cur.execute(query)
        response = cur.fetchall()

    for i in response:
        print(i)
        result_dict = {
            'chat_name': i[1],
            'title': control_clear(i[2]),
            'body': control_clear(i[3]),
            'time_of_public': i[5],
            'created_at': i[6]
        }
        t = DataBaseOperations(con).push_to_bd(result_dict)
        print(f'\n\n/////////////: {t}\n\n')
        pass

def control_clear(text):
    text = re.sub(r'<[\W\w\d]{1,10}>', '', text)
    return text

delete_and_check_status_tables()
rewrite_old_pro_db()