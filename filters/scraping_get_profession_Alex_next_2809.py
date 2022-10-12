"""
It is the new pattern from Alexander
"""

import re
from patterns import pattern_Alex2809
# from db_operations.scraping_db import DataBaseOperations


class AlexSort2809:

    def __init__(self):
        self.pattern_alex = pattern_Alex2809.pattern

        self.result_dict2 = {'vacancy': 0, 'contacts': 0, 'fullstack': 0, 'frontend': 0, 'backend': 0, 'pm': 0,
                             'mobile': 0, 'game': 0, 'designer': 0, 'hr': 0, 'analyst': 0, 'qa': 0, 'ba': 0,
                             'product': 0, 'devops': 0, 'marketing': 0, 'sales_manager': 0, 'junior': 0, 'middle': 0,
                             'senior': 0}

        self.keys_result_dict = ['fullstack', 'frontend', 'qa', 'ba', 'backend', 'pm', 'mobile', 'game', 'designer',
                                 'hr', 'analyst', 'product', 'devops', 'marketing', 'sales_manager']

    def sort_by_profession_by_Alex(self, title, body, companies=None):
        params = {}

        params['company_hiring'] = []
        # search company
        for i in companies:
            match = re.findall(rf'{i}', title+body)
            if match:
                params['company_hiring'] = match[0]
                break
        if not params['company_hiring']:
            p = self.get_company(title, body, companies) #new code
            companies_list = self.clean_company(p)
            params['company_hiring'] = companies_list

        params['english_level'] = self.english_requirements(title, body)
        params['jobs_type'] = self.work_type_fulltime(title, body)
        params['city'] = self.get_city(title, body)
        params['relocation'] = self.get_relocation(title, body)

        profession = []
        profession_dict = {}

        self.tag_alex = ''
        self.tag_alex_anti = ''

# ----------------- Check for used capitalize or don't ------------------
        for i in self.pattern_alex:
            if i not in ['internship', 'remote', 'relocate', 'country', 'city']:
                if i in ['pm', 'game', 'designer', 'hr', 'analyst', 'qa', 'ba', 'product']:
                    capitalize = True
                else:
                    capitalize = False

                message = f'{title}\n{body}'
                if i == 'remote':
                    pass
                self.get_profession(message, capitalize, key=i)

                if i == 'contacts' and self.result_dict2['contacts'] == 0:
                    print('*****************NO CONTACTS!!!!!!!!!')
                    profession = ['no_sort']
                    break
                if i == 'vacancy' and self.result_dict2['vacancy'] == 0:
                    print('*****************NO VACANCY!!!!!!!!!')
                    profession = ['no_sort']
                    break


        for i in self.result_dict2:
            if self.result_dict2[i]:
                profession.append(i)
        pass

#------------------- вывести в консоль result_dict ----------------------
        # for i in self.result_dict2:
        #     print(f'{i}: {self.result_dict2[i]}')

# ---------------- delete not used keys and values as contact and vacancy -------------------
        k=0
        while k<len(profession):
            if profession[k] in ['vacancy', 'contacts']:
                profession.pop(k)
            else:
                k += 1

        profession = set(profession)
        skills = {'middle', 'senior', 'junior'}
        names_profession = {'backend', 'frontend', 'devops', 'pm', 'product', 'designer', 'analyst',
                                    'fullstack', 'mobile', 'qa', 'hr', 'game', 'ba', 'marketing',
                                    'sales_manager'}
        if skills.intersection(profession) and not names_profession.intersection(profession):
            profession = {'no_sort'}

# -------------- collect dict for return it to main code ----------------
        profession_dict['profession'] = profession
        profession_dict['tag'] = self.tag_alex
        profession_dict['anti_tag'] = self.tag_alex_anti
        profession_dict['block'] = False  # заглушки для кода, который вызывает этот класс
        profession_dict['junior'] = 0
        profession_dict['middle'] = 0
        profession_dict['senior'] = 0

        if not profession_dict['profession']:
            profession_dict['profession'] = 'no_sort'

        return {'profession': profession_dict, 'params': params}

    def get_profession(self, message, capitalize, key):
        message_to_check = ''
        link_telegraph = ''
        if key == 'remote':
            pass
# --------------- collect all matches in 'ma' -----------------------
        for word in self.pattern_alex[key]['ma']:

            if not capitalize:
                word = word.lower()
                message_to_check = message.lower()
            else:
                message_to_check = message

            match = re.findall(word, message_to_check)
            if match:
                self.tag_alex += f'TAG {key}={match}\n'
                print(f'TAG {key} = {match}')
                self.result_dict2[key] += len(match)

# -------------- cancel all matches if it excludes words ------------------
        for exclude_word in self.pattern_alex[key]['mex']:
            match = re.findall(exclude_word, message_to_check)
            if match:
                self.tag_alex_anti += f'TAG ANTI {key}={match}\n'
                print(f'ANTI TAG {key} = {match}')
                self.result_dict2[key] = 0

        pass

    def get_company(self, title, body, companies):  # new code
        text = title+body
        company = []

        # companies = DataBaseOperations(con=None).get_all_from_db('companies', without_sort=True)
        for comp in companies:
            el = comp[1]
            match = re.findall(el, text)
            if match:
                company.append(match)

        if not company:
            for param in pattern_Alex2809.params['company_hiring']:
                match = re.findall(rf'{param}', text)
                if match:
                    company.append(match)
        company = self.check_clear_element(company) # transform to string
        return company

    def english_requirements(self, title, body):
        text = title + body
        english = []
        for param in pattern_Alex2809.params['english_level']:
            match = re.findall(rf'{param}', text)
            if match:
                english.append(match)
        english = self.check_clear_element(english) # transform to string
        return english

    def work_type_fulltime(self, title, body):
        text = title+body
        fulltime = []
        for param in pattern_Alex2809.params['jobs_type']:
            match = re.findall(param, text)
            if match:
                fulltime.append(match)
        fulltime = self.check_clear_element(fulltime) # transform to string
        return fulltime

    def get_relocation(self, title, body):
        text = title+body
        relocation = []
        for param in pattern_Alex2809.params['relocation']:
            match = re.findall(param, text)
            if match:
                relocation.append(match)
        relocation = self.check_clear_element(relocation) # transform to string
        return relocation

    def get_city(self, title, body):
        text = title+body
        city = []
        for key in pattern_Alex2809.cities_pattern:
            for param in pattern_Alex2809.cities_pattern[key]:
                match = re.findall(param, text)
                if match:
                    city.append(match)
        city = self.check_clear_element(city) # transform to string
        return city

    def clean_company(self, company_list):
        elements_list = []
        if type(company_list) in [tuple, list, set]:
            for elements in company_list:
                for element in elements:
                    e = re.sub(r'[Кк]омпан[иi][яий][:\-\s]{0,3}|[Cc]ompany[:\-\s]{0,3}|[Р,р]аботодатель[:\-\s]{0,3}', '', str(element))
                    if e:
                        elements_list.append(e)
        else:
            elements_list = company_list
        return elements_list

    def check_clear_element(self, element):
        if element == []:
            return ''
        for i in range(0, 3):
            if type(element) in [list, tuple, set]:
                element = element[0]
            else:
                return element

    def get_content_from_telegraph(self, link_telegraph):
        print('link_telegraph = ', link_telegraph)
        """
        parsing
        """

#  -------------- it reads from file for testing ------------------
# with open('./../file.txt', 'r', encoding='utf-8') as file:
#     text = file.read()
#
# text = text.split(f'\n', 1)
# title = text[0]
# body = text[1]
#
# print(title)
# print(body)
#
# profession = AlexSort2809().sort_by_profession_by_Alex(title, body)
# print('total profession = ', profession)