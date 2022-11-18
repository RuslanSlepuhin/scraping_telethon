import asyncio
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
# from bot.scraping_push_to_channels import PushChannels
from db_operations.scraping_db import DataBaseOperations
from patterns.pattern_Alex2809 import cities_pattern, params


class GeekJobGetInformation:

    def __init__(self, bot_dict):
        self.db_tables = None
        self.options = None
        self.page = None
        self.bot = bot_dict['bot']
        self.chat_id = bot_dict['chat_id']
        self.current_message = None


    async def get_content(self, db_tables=None):
        """
        If DB_tables = 'all', that it will push to all DB include professions.
        If None (default), that will push in all_messages only
        :param count_message_in_one_channel:
        :param db_tables:
        :return:
        """
        self.db_tables = db_tables

        self.count_message_in_one_channel = 1

        self.options = Options()
        self.options.add_argument("--headless")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--no-sandbox")

        link = f'https://geekjob.ru/vacancies?'
        response_dict = await self.get_info(link)
        # for self.page in range(1, 48):
        #     link = f'https://geekjob.ru/vacancies/{self.page}'
        #     await self.get_info(link)
        return response_dict

    async def get_info(self, link):
        self.browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
        self.browser.get(link)
        time.sleep(2)
        # button = self.browser.find_element(By.XPATH, "//button[@class='btn btn-small waves-effect']")
        # button.click()
        # time.sleep(2)
        self.browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        response_dict = await self.get_link_message(self.browser.page_source)

        return response_dict

    async def get_link_message(self, raw_content):
        message_dict ={}
        results_dict = {}
        to_write_excel_dict = {
            'chat_name': [],
            'title': [],
            'body': [],
            'vacancy': [],
            'vacancy_url': [],
            'company': [],
            'company_link': [],
            'english': [],
            'relocation': [],
            'job_type': [],
            'city': [],
            'salary': [],
            'experience': [],
            'time_of_public': [],
            'contacts': []
        }
        self.current_message = await self.bot.send_message(self.chat_id, 'https://geekjob.ru is starting', disable_web_page_preview=True)

        base_url = 'https://geekjob.ru'
        links = []
        soup = BeautifulSoup(raw_content, 'lxml')
        self.browser.quit()

        list_links = soup.find_all('a', class_='title')

# --------------------- LOOP -------------------------
        self.current_message = await self.bot.edit_message_text(
            f'{self.current_message.text}\nНайдено {len(list_links)} вакансий',
            self.current_message.chat.id,
            self.current_message.message_id,
            disable_web_page_preview = True
        )
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
                hiring = self.clean_company_name(soup.find('h5', class_='company-name').get_text()).strip()
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

            relocation = ''
            try:
                job_format = soup.find('span', class_="jobformat").get_text()
                title += f'\n{job_format}'
                if 'Релокация' in job_format:
                    relocation = 'релокация'
            except:
                job_format = ''
                relocation = ''
            print('job_format = ', job_format)
            print('relocation = ', relocation)

            try:
                date = soup.find('div', class_="time").get_text()
                date = self.convert_date(date)
            except:
                date = ''
            print('date = ', date)

            try:
                contacts = soup.find('div', id='hrblock').get_text()
                match = re.findall(r'[a-zA-Z]{0,}@[a-zA-Z\.-_]{3,}', contacts)
                if match:
                    contacts = ''
                    for i in match:
                        if i[-1] == '.':
                            i = i[0:-1]
                        contacts += f'{i}, '
                    contacts = contacts[:-2]

                body += f'\n{contacts}'
            except:
                contacts = ''
            print('contacts = ', contacts)

            try:
                vacancy_name = soup.find('h1').get_text()
            except:
                vacancy_name = ''
            print('vacancy_name = ', vacancy_name)

            try:
                city = soup.find('div', class_='location').get_text()
            except:
                city = ''
            print('city = ', city)

            try:
                salary = soup.find('span', class_='salary').get_text()
            except:
                salary = ''
            print('salary = ', salary)

            # ------------------------- search relocation ----------------------------
            relocation = ''
            if re.findall(r'[Рр]елокация', body):
                relocation = 'релокация'

            # ------------------------- search city ----------------------------
            city = ''
            for key in cities_pattern:
                for item in cities_pattern[key]:
                    match = re.findall(rf"{item}", body)
                    if match and key != 'others':
                        for i in match:
                            city += f"{i} "

            # ------------------------- search english ----------------------------
            english = ''
            for item in params['english_level']:
                match = re.findall(rf"{item}", body)
                if match:
                    for i in match:
                        english += f"{i} "

            # ------------------- compose title and body ------------------------------------

            if hiring_link:
                to_write_excel_dict['chat_name'].append('https://geekjob.ru/')
                to_write_excel_dict['title'].append(title)
                to_write_excel_dict['body'].append(body)
                to_write_excel_dict['vacancy'].append(vacancy_name)
                to_write_excel_dict['vacancy_url'].append(str(i))
                to_write_excel_dict['company'].append(hiring)
                to_write_excel_dict['company_link'].append(hiring_link)
                to_write_excel_dict['english'].append('')
                to_write_excel_dict['relocation'].append(relocation)
                to_write_excel_dict['job_type'].append(job_format)
                to_write_excel_dict['city'].append(city)
                to_write_excel_dict['salary'].append(salary)
                to_write_excel_dict['experience'].append('')
                to_write_excel_dict['time_of_public'].append(date)
                to_write_excel_dict['contacts'].append(contacts)
                self.current_message = await self.bot.edit_message_text(
                    f'{self.current_message.text}\n{self.count_message_in_one_channel}. {vacancy_name}\n',
                    self.current_message.chat.id,
                    self.current_message.message_id,
                    disable_web_page_preview=True)
                #
                #
                # results_dict['chat_name'] = 'geek_jobs.ru'
                # results_dict['title'] = title
                # results_dict['body'] = body
                # results_dict['time_of_public'] = date
                # message_dict['message'] = f'{title}\n{body}'

                print(f"{self.count_message_in_one_channel} from_channel = geekjob.ru")
                self.count_message_in_one_channel += 1
                print('time_sleep')
                # time.sleep(random.randrange(10, 15))

        df = pd.DataFrame(
            {
                'title': to_write_excel_dict['title'],
                'body': to_write_excel_dict['body'],
                'vacancy': to_write_excel_dict['vacancy'],
                'vacancy_url': to_write_excel_dict['vacancy_url'],
                'company': to_write_excel_dict['company'],
                'english': to_write_excel_dict['english'],
                'relocation': to_write_excel_dict['relocation'],
                'job_type': to_write_excel_dict['job_type'],
                'city': to_write_excel_dict['city'],
                'salary': to_write_excel_dict['salary'],
                'experience': to_write_excel_dict['experience'],
                'time_of_public': to_write_excel_dict['time_of_public'],
                'contacts': to_write_excel_dict['contacts'],
            }
        )

        try:
            df.to_excel(f'./../excel/geekjob.xlsx', sheet_name='Sheet1')
            print('записал в файл')
        except Exception as e:
            print('Geek не записан в файл ', e)

        self.current_message = await self.bot.send_message(
            self.chat_id,
            f'\nMessages are writting to DB, please wait a few time ...\n'
        )

        return to_write_excel_dict

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

    async def compose_in_one_file(self):
        hiring = []
        link = []
        contacts = []

        for i in range(1, 48):
            excel_data_df = pd.read_excel(f'./../messages/geek{i}.xlsx', sheet_name='Sheet1')

            hiring.extend(excel_data_df['hiring'].tolist())
            link.extend(excel_data_df['hiring_link'].tolist())
            contacts.extend(excel_data_df['contacts'].tolist())

        df = pd.DataFrame(
            {
            'hiring': hiring,
            'access_hash': link,
            'contacts': contacts,
            }
        )
        try:
            df.to_excel(f'all_geek.xlsx', sheet_name='Sheet1')
        except Exception as e:
            print('Geek didnt write to excel: ', e)

    async def write_to_db_table_companies(self):
        excel_data_df = pd.read_excel('all_geek.xlsx', sheet_name='Sheet1')
        companies = excel_data_df['hiring'].tolist()
        links = excel_data_df['access_hash'].tolist()

        companies = set(companies)

        db=DataBaseOperations(con=None)
        db.write_to_db_companies(companies)

# loop = asyncio.new_event_loop()
# loop.run_until_complete(GeekJobGetInformation().get_content(1))

