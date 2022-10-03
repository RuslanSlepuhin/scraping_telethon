import asyncio
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import re


class FindJobGetInformation:

    def __init__(self, client):
        self.client = client

        self.base_url = 'https://finder.vc'
        self.options = Options()
        # self.options.add_argument("--headless")
        # self.options.add_argument("--disable-dev-shm-usage")
        # self.options.add_argument("--no-sandbox")
        self.browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)



    async def get_content(self, count_message_in_one_channel=20):

        self.count_message_in_one_channel = count_message_in_one_channel

        # options = Options()
        # options.add_argument("--headless")
        # options.add_argument("--disable-dev-shm-usage")
        # options.add_argument("--no-sandbox")

        link = self.base_url + '/vacancies?category=1'
        # self.browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
        self.browser.get(link)
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
        links = []
        soup = BeautifulSoup(raw_content, 'lxml')
        # self.browser.quit()

        list_links = soup.find_all('a', class_='vacancy-card vacancy-card_result')
        pass

        for i in list_links:

            links.append(i.get('href'))
            print(self.base_url + i.get('href'))  # собираем все ссылки в list, чтобы получить оттуда полный текст вакансии

            response = requests.get(self.base_url + i.get('href'))

            soup = BeautifulSoup(response.text, 'lxml')
            try:
                title = soup.find('h1', class_='vacancy-info-header__title').get_text()  # Программист 1С
            except:
                title = ''
            print('title = ', title)

            try:
                body = soup.find('div', class_="vacancy-info-body__description").get_text()
            except:
                body = ''
            print('body = ', body)

            try:
                responsibilities = soup.find('ul', class_="vacancy-info-body__list").get_text()
            except:
                responsibilities = ''
            print('responsibilities = ', responsibilities)

            try:
                terms = soup.find('ul', class_="vacancy-info-body__list").get_text()
            except:
                terms = ''
            print('terms = ', terms)

            try:
                hiring = soup.find('a', class_='link').get_text()
            except:
                hiring = ''
            print('hiring = ', hiring)

            # try:
            #     hiring_link = soup.find('a', class_='link').get('href')
            #     title += hiring_link.strip() + ' '
            # except:
            #     hiring_link = ''
            # print('hiring_link = ', hiring_link)

            try:
                time_job = soup.find('div', class_="employment-label__text").get_text()
                # body = f'\nГрафик работы: {time_job}\n' + body
            except:
                time_job = ''
            print('time_job = ', time_job)

            try:
                item = soup.find_all('div', class_="row-text")
                cost = item[0].get_text()
                experience = item[1].get_text()
                # body = f'\nЗарплата: {cost}\nОпыт: {experience}\n' + body
            except:
                cost = ''
                experience= ''
            print('cost = ', cost)
            print('experience = ', experience)

            time_created = soup.find('div', class_='vacancy-info-header__publication-date').get_text()
            print(time_created)


# --------------------- get contacts by click button ----------------------------
            link_vacancy = self.base_url + links[-1]
            print('vacancy_url = ', link_vacancy)
            self.browser.get(link_vacancy)
            time.sleep(2)
            button = self.browser.find_element(By.XPATH,
                                               "//button[@class='vacancy-info-footer__button button button_primary button_bold button_uppercase button_mobile-block']")
            button.click()
            time.sleep(2)
            soup_contacts = BeautifulSoup(self.browser.page_source, 'lxml')
            contacts = soup_contacts.find('div', class_='contacts__item').get_text()
            print('contacts = ', contacts)

# ------------------- collect title and body ------------------------------------
            body = f'' \
                   f'Компания: {hiring}\n' \
                   f'Контакты: {contacts}\n' \
                   f'Заработная плата: {cost}\n' \
                   f'Требуемый опыт: {experience}\n\n' \
                   f'График работы: {time_job}\n\n' \
                   f'Описание вакансии:\n{body}\n\n' \
                   f'Требования: \n{responsibilities}\n\n' \
                   f'Условия: \n{terms}'

            pass

            if contacts:
                to_write_excel_dict['title'].append(title)
                to_write_excel_dict['body'].append(body)
                to_write_excel_dict['time_create'].append(self.convert_date(time_created))
            #     # to_write_excel_dict['job_format'].append(job_format)
            #     # to_write_excel_dict['hiring'].append(hiring)
            #     # to_write_excel_dict['hiring_link'].append(hiring_link)
            #     # to_write_excel_dict['contacts'].append(contacts)
            #
                # results_dict['chat_name'] = 'geek_jobs.ru'
                # results_dict['title'] = title
                # results_dict['body'] = body
                # results_dict['time_of_public'] = time_created
            #
                # message_dict['message'] = f'{title}\n{body}'
            #     # await PushChannels().push(results_dict, self.client, message_dict)
            #
            #     print(f"{self.count_message_in_one_channel} from_channel = geek_jobs.ru'")
            #     self.count_message_in_one_channel += 1
            #     print('time_sleep')
            #     # time.sleep(random.randrange(10, 15))
        #
        #     pass
        self.browser.quit()

        df = pd.DataFrame(
            {
                'title': to_write_excel_dict['title'],
                'body': to_write_excel_dict['body'],
                'time_create': to_write_excel_dict['time_create'],
        #         # 'job_format': to_write_excel_dict['job_format'],
        #         # 'hiring': to_write_excel_dict['hiring'],
        #         # 'hiring_link': to_write_excel_dict['hiring_link'],
        #         # 'contacts': to_write_excel_dict['contacts']
            }
        )

        df.to_excel('finder.vc.xlsx', sheet_name='Sheet1')

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

        return text

    def convert_date(self, date):
        date = date.split(' ')
        if date[1] == 'сегодня':
            date = datetime.now()
        elif re.findall(r'дн[ейя]{1,2}', date[2]):
            date = datetime.now()-timedelta(days=int(date[1]))
        elif re.findall(r'месяц[ева]{0,2}', date[2]):
            date = datetime.now() - timedelta(days=int(date[1]*30))
        return date


    # def convert_date(self, str):
    #     convert = {
    #         'января': '1',
    #         'февраля': '2',
    #         'марта': '3',
    #         'апреля': '4',
    #         'мая': '5',
    #         'июня': '6',
    #         'июля': '7',
    #         'августа': '8',
    #         'сентября': '9',
    #         'октября': '10',
    #         'ноября': '11',
    #         'декабря': '12',
    #     }
    #
    #     month = str.split(' ')[1].strip()
    #     day = str.split(' ')[0].strip()
    #     year = datetime.now().strftime('%Y')
    #
    #     for i in convert:
    #         if i == month:
    #             month = convert[i]
    #             break
    #
    #     date = datetime(int(year), int(month), int(day), 12, 00)
    #
    #     return date

    def clean_company_name(self, text):
        text = re.sub('Прямой работодатель', '', text)
        text = re.sub(r'[(]{1} [a-zA-Z0-9\W\.]{1,30} [)]{1}', '', text)
        text = re.sub(r'Аккаунт зарегистрирован с (публичной почты|email) \*@[a-z.]*[, не email компании!]{0,1}', '', text)
        text = text.replace(f'\n', '')
        return text

print('go')
# task = asyncio.create_task(FindJobGetInformation().get_content())
loop = asyncio.new_event_loop()
loop.run_until_complete(FindJobGetInformation().get_content())
# loop.run_until_complete(FindJobGetInformation().get_content())
