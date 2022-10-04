"""
It is the new pattern from Alexander
"""

import re
from patterns import pattern_Alex2809


class AlexSort2809:

    def __init__(self):
        self.pattern_alex = pattern_Alex2809.pattern

        self.result_dict2 = {'vacancy': 0, 'contacts': 0, 'fullstack': 0, 'frontend': 0, 'backend': 0, 'pm': 0,
                             'mobile': 0, 'game': 0, 'designer': 0, 'hr': 0, 'analyst': 0, 'qa': 0, 'ba': 0,
                             'product': 0, 'devops': 0, 'marketing': 0, 'sales_manager': 0, 'junior': 0, 'middle': 0,
                             'senior': 0}

        self.keys_result_dict = ['fullstack', 'frontend', 'qa', 'ba', 'backend', 'pm', 'mobile', 'game', 'designer',
                                 'hr', 'analyst', 'product', 'devops', 'marketing', 'sales_manager']

    def sort_by_profession_by_Alex(self, title, body):

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
        for i in self.result_dict2:
            print(f'{i}: {self.result_dict2[i]}')

# ---------------- delete not used keys and values as contact and vacancy -------------------
        k=0
        while k<len(profession):
            if profession[k] in ['vacancy', 'contacts']:
                profession.pop(k)
            else:
                k += 1

        profession = set(profession)

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

        return profession_dict


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
                print(f'TAG ANTI {key} = {match}')
                self.result_dict2[key] = 0

        pass

    def get_content_from_telegraph(self, link_telegraph):
        print('link_telegraph = ', link_telegraph)
        """
        parsing
        """

#  -------------- it reads from file for testing ------------------
# with open('file.txt', 'r', encoding='utf-8') as file:
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