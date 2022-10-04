import pandas as pd
from filters.scraping_get_profession import Professions

excel_data_df = None

try:
    excel_data_df = pd.read_excel('D://python/тестовый разбор.xlsx', sheet_name='Sheet1')
except Exception as e:
    print(e)

print(excel_data_df)

excel_dict = {
    'code_python': excel_data_df['Code Python'].tolist(),
    'number': excel_data_df['Number'].tolist(),
    'channel': excel_data_df['Channel'].tolist(),
    'title': excel_data_df['Title'].tolist(),
    'body': excel_data_df['Body'].tolist(),
    'fullstack': excel_data_df['Fullstack'].tolist(),
    'front': excel_data_df['Front'].tolist(),
    'back': excel_data_df['Back'].tolist(),
    'pm': excel_data_df['PM'].tolist(),
    'mobile': excel_data_df['Mobile'].tolist(),
    'game': excel_data_df['Game'].tolist(),
    'designer': excel_data_df['Designer'].tolist(),
    'hr': excel_data_df['HR'].tolist(),
    'analyst': excel_data_df['Analyst'].tolist(),
    'qa': excel_data_df['QA'].tolist(),
    'ba': excel_data_df['BA'].tolist(),
    'prdm': excel_data_df['PrdM'].tolist(),
    'devops': excel_data_df['DevOps'].tolist(),
    'marketer': excel_data_df['Marketer'].tolist(),
    'sales': excel_data_df['Sales'].tolist(),
}
print(excel_dict)

print(len(excel_dict['title']))

for i in range(1, len(excel_dict['title'])):
    title = excel_dict['title'][i]
    body = excel_dict['body'][i]
    profession = Professions().sort_by_profession(title, body)
    excel_dict['code_python'][i] = profession['profession']

df = pd.DataFrame(
        {
            'code_python': excel_dict['code_python'],
            'number': excel_dict['number'],
            'channel': excel_dict['channel'],
            'title': excel_dict['title'],
            'body': excel_dict['body'],
            'fullstack': excel_dict['fullstack'],
            'front': excel_dict['front'],
            'back': excel_dict['back'],
            'pm': excel_dict['pm'],
            'game': excel_dict['game'],
            'designer': excel_dict['designer'],
            'hr': excel_dict['hr'],
            'analyst': excel_dict['analyst'],
            'qa': excel_dict['qa'],
            'ba': excel_dict['ba'],
            'prdm': excel_dict['prdm'],
            'devops': excel_dict['devops'],
            'marketer': excel_dict['marketer'],
            'sales': excel_dict['sales'],
        }
    )

df.to_excel('D://python/тестовый разбор2.xlsx', sheet_name='Sheet1')

print('complete')


