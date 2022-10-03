import random
import re
import time
from datetime import datetime
import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from scraping_push_to_channels import PushChannels
from scraping_db import DataBaseOperations

class GeekJobGetInformation:

    def __init__(self, client):
        self.client = client
        self.db_tables = None


    async def get_content(self, count_message_in_one_channel, db_tables=None):
        """
        If DB_tables = 'all', that it will push to all DB include professions.
        If None (default), that will push in all_messages only
        :param count_message_in_one_channel:
        :param db_tables:
        :return:
        """
        self.db_tables = db_tables

        self.count_message_in_one_channel = count_message_in_one_channel

        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")

        link = f'https://geekjob.ru/vacancies?'
        self.browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        self.browser.get(link)
        time.sleep(2)
        button = self.browser.find_element(By.XPATH, "//button[@class='btn btn-small waves-effect']")
        button.click()
        time.sleep(2)
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        await self.get_link_message(self.browser.page_source)

        return 'Compete'


    async def get_link_message(self, raw_content):
        message_dict ={}
        results_dict = {}
        to_write_excel_dict = {
            'title': [],
            'body': [],
            'time_create': [],
            'job_format': [],
            'hiring': [],
            'hiring_link': [],
            'contacts': []
        }
        base_url = 'https://geekjob.ru'
        links = []
        soup = BeautifulSoup(raw_content, 'lxml')
        self.browser.quit()

        list_links = soup.find_all('a', class_='title')
        for i in list_links:
            links.append(i.get('href'))
            print(base_url + i.get('href'))  # собираем все ссылки в list, чтобы получить оттуда полный текст вакансии
            response = requests.get(base_url + i.get('href'))

            soup = BeautifulSoup(response.text, 'lxml')
            try:
                title = soup.find('h1').get_text()
            except:
                title = ''

            try:
                tags = soup.find('div', class_="tags").get_text()
                title += f"\n{tags}"
            except:
                pass

            try:
                title += f"\n{soup.find('div', class_='category').get_text()}"
            except:
                pass

            try:
                title += f"\n{soup.find('div', class_='location').get_text()}"
            except:
                pass

            print('title = ', title)

            try:
                body = self.normalize_text(soup.find('div', id="vacancy-description"))
            except:
                body = ''
            print('body = ', body)

            try:
                hiring = self.clean_company_name(soup.find('h5', class_='company-name').get_text())
                title += f'\nContacts: {hiring.strip()} '
            except:
                hiring = ''
            print('hiring = ', hiring)

            try:
                hiring_link = soup.find('h5', class_="company-name").find('a', class_="link extlink").get('href')
                title += hiring_link.strip() + ' '
            except:
                hiring_link = ''
            print('hiring_link = ', hiring_link)

            try:
                job_format = soup.find('span', class_="jobformat").get_text()
                title += f'\n{job_format}'
            except:
                job_format = ''
            print('job_format = ', job_format)

            try:
                date = soup.find('div', class_="time").get_text()
                date = self.convert_date(date)
            except:
                date = ''
            print('date = ', date)

            try:
                contacts = soup.find('div', id='hrblock').get_text()
                body += f'\n{contacts}'
            except:
                contacts = ''
            print('contacts = ', contacts)

            if hiring_link:
                to_write_excel_dict['title'].append(title)
                to_write_excel_dict['body'].append(body)
                to_write_excel_dict['time_create'].append(date)
                # to_write_excel_dict['job_format'].append(job_format)
                # to_write_excel_dict['hiring'].append(hiring)
                # to_write_excel_dict['hiring_link'].append(hiring_link)
                # to_write_excel_dict['contacts'].append(contacts)

                results_dict['chat_name'] = 'geek_jobs.ru'
                results_dict['title'] = title
                results_dict['body'] = body
                results_dict['time_of_public'] = date

                message_dict['message'] = f'{title}\n{body}'

                if self.db_tables:
                    await PushChannels().push(results_dict, self.client, message_dict)
                    await DataBaseOperations(con=None).write_to_one_table(results_dict)
                else:
                    await DataBaseOperations(con=None).write_to_one_table(results_dict)

                print(f"{self.count_message_in_one_channel} from_channel = geek_jobs.ru'")
                self.count_message_in_one_channel += 1
                print('time_sleep')
                # time.sleep(random.randrange(10, 15))

            pass

        df = pd.DataFrame(
            {
                'title': to_write_excel_dict['title'],
                'body': to_write_excel_dict['body'],
                'time_create': to_write_excel_dict['time_create'],
                # 'job_format': to_write_excel_dict['job_format'],
                # 'hiring': to_write_excel_dict['hiring'],
                # 'hiring_link': to_write_excel_dict['hiring_link'],
                # 'contacts': to_write_excel_dict['contacts']
            }
        )

        df.to_excel('ggg.xlsx', sheet_name='Sheet1')
        print('записал в файл')

        pass

    def normalize_text(self, text):
        text = str(text)
        text = text.replace('<div id="vacancy-description">', '')
        text = text.replace('<br>', f'\n').replace('<br/>', '')
        text = text.replace('<p>', f'\n').replace('</p>', '')
        text = text.replace('<li>', f'\n\t- ').replace('</li>', '')
        text = text.replace('<strong>', '').replace('</strong>', '')
        text = text.replace('<div>', '').replace('</div>', '')
        text = text.replace('<h4>', f'\n').replace('</h4>', '')
        text = text.replace('<ul>', '').replace('</ul>', '')
        text = text.replace('<i>', '').replace('</i>', '')
        text = text.replace('<ol>', '').replace('</ol>', '')
        text = text.replace('\u200b', '')
        text = re.sub(r'<[\W\w\d]{1,10}>', '', text)

        return text

    def convert_date(self, str):
        convert = {
            'января': '1',
            'февраля': '2',
            'марта': '3',
            'апреля': '4',
            'мая': '5',
            'июня': '6',
            'июля': '7',
            'августа': '8',
            'сентября': '9',
            'октября': '10',
            'ноября': '11',
            'декабря': '12',
        }

        month = str.split(' ')[1].strip()
        day = str.split(' ')[0].strip()
        year = datetime.now().strftime('%Y')

        for i in convert:
            if i == month:
                month = convert[i]
                break

        date = datetime(int(year), int(month), int(day), 12, 00)

        return date

    def clean_company_name(self, text):
        text = re.sub('Прямой работодатель', '', text)
        text = re.sub(r'[(]{1} [a-zA-Z0-9\W\.]{1,30} [)]{1}', '', text)
        text = re.sub(r'Аккаунт зарегистрирован с (публичной почты|email) \*@[a-z.]*[, не email компании!]{0,1}', '', text)
        text = text.replace(f'\n', '')
        return text

# GeekJobGetInformation().get_content()