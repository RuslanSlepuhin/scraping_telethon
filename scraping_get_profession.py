import re
from test_text import t_text_body, t_text_title

class Professions:
    # def sort_by_profession(self, title, body):
    #     self.check_dictionary = {
    #         'title': {
    #             'backend': 0,
    #             'frontend': 0,
    #             'devops': 0,
    #             'fullstack': 0,
    #             'mobile': 0,
    #             'pm': 0,
    #             'product': 0,
    #             'designer': 0,
    #             'qa': 0,
    #             'game': 0,
    #             'analyst': 0,
    #             'hr': 0,
    #             'ad': 0,
    #             'backend_language': 0,
    #             'frontend_language': 0,
    #         },
    #         'body': {
    #             'backend': 0,
    #             'frontend': 0,
    #             'devops': 0,
    #             'fullstack': 0,
    #             'mobile': 0,
    #             'pm': 0,
    #             'product': 0,
    #             'designer': 0,
    #             'qa': 0,
    #             'game': 0,
    #             'analyst': 0,
    #             'hr': 0,
    #             'ad': 0,
    #             'backend_language': 0,
    #             'frontend_language': 0,
    #         }
    #     }
    #
    #     counter = 1
    #     counter2 = 1
    #     pattern_ad = r'ищу\s{0,1}работу|opentowork|\bsmm\b|\bcopyright\w{0,3}\b|\btarget\w{0,3}\b|фильм на вечер|\w{0,2}рекоменд\w{2,5}' \
    #                  r'хотим рассказать о новых каналах|#резюме|кадровое\s{0,1}агентство|skillbox|' \
    #                  r'зарабатывать на крипте|\bсекретар\w{0,2}|делопроизводител\w{0,2}'
    #     pattern_backend = r'back\s{0,1}end|б[е,э]к\s{0,1}[е,э]нд[а-я]{0,2}|backend.{0,1}developer|datascientist|datascience'
    #     pattern_frontend = r'front.*end|фронт.*[е,э]нд[а-я]{0,2}\B|vue\.{0,1}js\b|\bangular\b'
    #     pattern_devops = r'dev\s*ops|sde|sre|Site\s{0,1}reliability\s{0,1}engineering'
    #     pattern_backend_mobile = r'android|ios|flutter'
    #     pattern_fullstack = r'full.{0,1}stack'
    #     pattern_designer = r'дизайнер[а-я]{0,2}\B|designer|\bui\s'
    #     pattern_analitic = r'analyst|аналитик[а-я]{0,2}'
    #     pattern_qa = r'qa\b|тестировщик[а-я]{0,2}|qaauto'
    #     pattern_hr = r'hr\s|рекрутер[а-я]{0,2}\B'
    #     pattern_pm = r'project[\W,\s]{0,1}manager|проджект[\W,\s]{0,1}менеджер\w{0,2}|marketing[\W,\s]{0,1}manager'
    #     pattern_product_m = r'product[\W,\s]{0,1}manager|прод[а,у]кт[\W,\s]{0,1}м[е,а]н[е,а]джер\w{0,2}|marketing[\W,\s]{0,1}manager'
    #     pattern_game = r'\Wunity|\Wpipeline|\Wgame[s]{0,1}\W|\Wmatch\W{0,1}3'
    #     # ---------------languages--------------------------------
    #     pattern_backend_languages = r'python[\s,#]|scala[\s,#]|java[\s,#]|linux[\s,#]|haskell[\s,#]|php[\s,#]|server|' \
    #                                 r'\bсервер\w{0,3}\b|c\+\+|\bml\b|\bnode.{0,1}js\b|docker|java\/{0,1}|scala\/{0,1}|cdn|docker|websocket\w{0,1}|django|pandas'
    #     pattern_frontend_languages = r'javascript|html|css|react\s*js|firebase|\bnode.{0,1}js\b|vue\.{0,1}js|aws|amazon\s{0,1}ws|ether\.{0,1}js|web3\.{0,1}js|angular'
    #     # ---------------------------------------------------------
    #
    #     text = [title.lower(), body.lower()]
    #     text_field = ['title', 'body']
    #
    #     k = 0
    #     for item in text:
    #         looking_for = re.findall(pattern_ad, item)
    #         if looking_for:
    #             self.check_dictionary[text_field[k]]['ad'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_backend, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['backend'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_frontend, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['frontend'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_devops, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['devops'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_backend_mobile, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['mobile'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_fullstack, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['fullstack'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_pm, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['pm'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_designer, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['designer'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_analitic, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['analyst'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_qa, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['qa'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_hr, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['hr'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_product_m, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['product'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_game, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['game'] += len(looking_for)
    #
    #         # ----------------------------langueges-------------------------
    #         looking_for = re.findall(pattern_backend_languages, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['backend_language'] += len(looking_for)
    #
    #         looking_for = re.findall(pattern_frontend_languages, item)
    #         if looking_for:
    #             counter += 1
    #             self.check_dictionary[text_field[k]]['frontend_language'] += len(looking_for)
    #         # ----------------------------------------------------------------
    #         else:
    #             counter2 += 1
    #
    #         k += 1
    #
    #     profession = self.analys_profession()
    #
    #     return profession
    #
    #
    # def analys_profession(self):
    #     backend = self.check_dictionary['title']['backend'] + self.check_dictionary['body']['backend']
    #     frontend = self.check_dictionary['title']['frontend'] + self.check_dictionary['body']['frontend']
    #     devops = self.check_dictionary['title']['devops'] + self.check_dictionary['body']['devops']
    #     fullstack = self.check_dictionary['title']['fullstack'] + self.check_dictionary['body']['fullstack']
    #     pm = self.check_dictionary['title']['pm'] + self.check_dictionary['body']['pm']
    #     product = self.check_dictionary['title']['product'] + self.check_dictionary['body']['product']
    #     designer = self.check_dictionary['title']['designer'] + self.check_dictionary['body']['designer']
    #     qa = self.check_dictionary['title']['qa'] + self.check_dictionary['body']['qa']
    #     analyst = self.check_dictionary['title']['analyst'] + self.check_dictionary['body']['analyst']
    #     mobile = self.check_dictionary['title']['mobile'] + self.check_dictionary['body']['mobile']
    #     hr = self.check_dictionary['title']['hr'] + self.check_dictionary['body']['hr']
    #     ad = self.check_dictionary['title']['ad'] + self.check_dictionary['body']['ad']
    #     backend_language = self.check_dictionary['title']['backend_language'] + self.check_dictionary['body'][
    #         'backend_language']
    #     frontend_language = self.check_dictionary['title']['frontend_language'] + self.check_dictionary['body'][
    #         'frontend_language']
    #     game = self.check_dictionary['title']['game'] + self.check_dictionary['body']['game']
    #
    #     profession = []
    #
    #     pro = None
    #     if ((devops and backend and frontend) or (devops and backend and not frontend) or (
    #             devops and frontend and not backend)) and (backend_language > 5 and backend_language > 5):
    #         profession = ['fullstack']
    #
    #     if frontend:
    #         profession.append('frontend')
    #
    #     if backend:
    #         profession.append('backend')
    #
    #     if (frontend and backend) and (frontend_language or backend_language):  # and not fullstack:
    #         if frontend_language / 2 > backend_language:
    #             pro = 'frontend'
    #         else:
    #             pro = 'backend'
    #         if pro not in profession:
    #             profession.append('pro')
    #
    #     # --------------------------------------------------------------
    #
    #     if backend and not frontend:  # and not fullstack:
    #         if backend_language and frontend_language:
    #             if frontend_language / 2 > backend_language:
    #                 pro = 'frontend'
    #             else:
    #                 pro = 'backend'
    #
    #         elif frontend_language and not backend_language:
    #             pro = 'frontend'
    #
    #         else:
    #             pro = 'backend'
    #         profession = [pro]
    #
    #     # --------------------------------------------------------------
    #
    #     if frontend and not backend:  # and not fullstack:
    #         if backend_language and frontend_language:
    #             if backend_language / 2 > frontend_language:
    #                 pro = 'backend'
    #             else:
    #                 pro = 'frontend'
    #
    #         elif backend_language and not frontend_language:
    #             pro = 'backend'
    #
    #         else:
    #             pro = 'frontend'
    #
    #         profession = [pro]
    #
    #     # --------------------------------------------------------------
    #
    #     if fullstack:
    #         profession.append('fullstack')
    #
    #     if qa:
    #         profession.append('qa')
    #
    #     if devops and 'fullstack' not in profession:
    #         profession.append('devops')
    #
    #     if pm:
    #         profession.append('pm')
    #
    #     if product:
    #         profession.append('product')
    #
    #     if mobile:
    #         profession.append('mobile')
    #
    #     if designer:
    #         profession.append('designer')
    #
    #     if analyst:
    #         profession.append('analyst')
    #
    #     if hr:
    #         profession.append('hr')
    #
    #     if game:
    #         profession.append('game')
    #
    #     if ad:
    #         profession = ['ad', ]
    #
    #     if not profession:
    #         if backend_language and frontend_language:
    #             if backend_language > frontend_language:
    #                 pro = 'backend'
    #             else:
    #                 pro = 'frontend'
    #         elif backend_language and not frontend_language:
    #             pro = 'backend'
    #         elif frontend_language and not backend_language:
    #             pro = 'frontend'
    #         if pro:
    #             profession.append(pro)
    #
    #     if not profession:
    #         profession = ['ad', ]
    #
    #     return profession
    #

#     def sort_by_profession2(self, title, body):
#
#         #-------------------------------TAGS---------------------------------------------
#
#         pattern_backend_tags = r'\#[а-я]{0,}\W{0,1}б[е,э]к\W{0,1}[е,э]нд[а-я]{0,2}|\#[a-z]{0,}\W{0,1}back\W{0,1}end\w{0,2}' \
#                                r'|\#python|\#scala|\#linux|\#java|\#ml|\#django|\#docker|\#php|\#websocket|\#pandas'
#         pattern_frontend_tags = r'\#[а-я]{0,}\W{0,1}фронт\W{0,1}[е,э]нд[а-я]{0,2}|\[a-z]{0,}\W{0,1}front\W{0,1}end\w{0,2}' \
#                                 r'|\#javascript|\#html|\#react|\#firebase|\#vue\.{0,1}js|\#ether\.{0,1}js|\#web3\.{0,1}js' \
#                                 r'|\#angular|\#css'
#         pattern_qa_tags = r'\#\w{0,6}\W{0,1}qa\W{0,1}\w{0,5}[^fulst]|\#[а-я]{0,}\W{0,1}тестировщик|\#test'
#         pattern_fullstack_tags = r'\#\w{0,7}\W{0,1}full\W{0,1}stack|\#[а-я]{0,6}\W{0,1}фу[л]{1,2}\W{0,1}ст[э,е]к|\#qa\W{0,1}full|\#stack\W{0,1}qa'
#         pattern_designer_tags = r'\#\w{0,6}\W{0,1}designer|\#ui[-\/_]{0,1}ux|\#ui|\#ux|\#[а-я]{0,6}\W{0,1}дизайнер'
#         pattern_mobile_tags = r'#ios|\#android|\#flutter|\#kotlin|\#mobile|\#swift'
#         pattern_project_manager_tags = r'\#\w{0,6}\W{0,1}project\W{0,1}manager|\#[а-я]{0,6}\W{0,1}продж[е,э]кт\W{0,1}менеджер|\#pm\W'
#         patter_product_manager_tags = r'\#\w{0,6}\W{0,1}product\W{0,1}manager|\#\w{0,6}\W{0,1}prdm|\#\w{0,6}\W{0,1}pm\W'
#         pattern_game_tags = r'\#game|\#unity|\#unreal|\#match\W{0,1}3|\#pipeline'
#         pattern_ba_tags = r'\#[а-я]{0,5}\W{0,1}бизнес\W{0,1}аналитик|\#[а-я]{0,5}\W{0,1}ба\W|\#\w{0,6}\s{0,1}business\W{0,1}analityst|\#\w{0,6}\s{0,1}ba\W' \
#                           r'|\#\w{0,6}\s{0,1}business\W{0,1}analyst'
#         pattern_devops_tags = r'\#\w{0,6}\W{0,1}dev\W{0,1}ops|\#[а-я]{0,6}\W{0,1}дев\W{0,1}опс'
#         pattern_marketing_tags = r'\#\w{0,6}\W{0,1}smm|\#\w{0,6}\W{0,1}copyright[er,ing]|\#\w{0,6}\W{0,1}seo'
#         pattern_analyst_tags = r'\#data\W{0,1}analyst|\#analyst|\#data\W{0,1}scientist|\#business\W{0,1}analyst|\#ba\W'
#         pattern_hr_tags = r'\#hr|\#recruiter|\#human|\#hr\W{0,1}recruiter|\#hr\W{0,1}tech|\#hr\W{0,1}[департамент,department]|\#seo\W{0,1}hr'
#
#         #-------------------------------TITLE BODY---------------------------------------------
#
#         pattern_backend = r'б[е,э]к\W{0,1}[е,э]нд[а-я]{0,2}|back\W{0,1}end\w{0,2}' \
#                                r'|python|scala|linux|#java|ml\W|django|docker|php|websocket|pandas|flask'
#         pattern_frontend = r'фронт\W{0,1}[е,э]нд[а-я]{0,2}|front\W{0,1}end\w{0,2}' \
#                                 r'|javascript|html|react|firebase|vue\.{0,1}js|ether\.{0,1}js|web3\.{0,1}js' \
#                                 r'|angular|css'
#         pattern_qa = r'[^\#]qa\W{0,1}\w{0,5}[^fulst]|тестировщик|\Wtest|тестирование|[^\#]automation'
#         pattern_fullstack = r'full\W{0,1}stack|фу[л]{1,2}\W{0,1}ст[э,е]к|qa\W{0,1}full|stack\W{0,1}qa'
#         pattern_designer = r'designer|\Wui\W{0,1}ux|\Wui|\Wux|дизайнер'
#         pattern_mobile = r'\W{0,1}ios|android|flutter|kotlin|mobile|swift'
#         pattern_project_manager = r'project\W{0,1}manager|продж[е,э]кт\W{0,1}менеджер|\Wpm'
#         patter_product_manager = r'product\W{0,1}manager|\Wprdm'
#         pattern_game = r'\Wgame|\Wunity|\Wunreal|match\W{0,1}3|pipeline'
#         pattern_ba = r'бизнес\W{0,1}аналитик|\Wба|business\W{0,1}analityst|#\w{0,6}\s{0,1}ba|business\W{0,1}analyst'
#         pattern_devops = r'dev\W{0,1}ops|дев\W{0,1}опс'
#         pattern_marketing = r'smm|copyright[er,ing]|\Wseo|маркетинг\W{0,1}менеджер|маркетинг'
#         pattern_analyst = r'data\W{0,1}analyst|\Wanalyst|data\W{0,1}scientist|business\W{0,1}analyst|\Wba\W'
#         pattern_hr = r'hr|recruiter|human|hr\W{0,1}recruiter|hr\W{0,1}tech|hr\W{0,1}[департамент,department]|seo\W{0,1}hr'
#
#         #-------------------------AD---------------------------------
#
#         pattern_ad = r'ищу\W{0,1}работу|opentowork|target|фильм на вечер|рекоменд' \
#                      r'хотим рассказать о новых каналах|#резюме|кадровое\W{0,1}агентство|skillbox|' \
#                      r'зарабатывать на крипте|секретар|делопроизводител|рекомендация'
#
#         profession = []
#         text = title.lower() + body.lower()
#         body = body.lower()
#
#         if re.findall(pattern_ad, text):
#             profession = ['ad',]
#             print(profession)
#             return profession
#
# #----------поиск по тегам и подтверждение в body----------------
#
#         if re.findall(pattern_backend_tags, text):
#             if re.findall(pattern_backend, body):
#                 profession.append('backend')
#             # else:
#             #     profession.append('backend')
#
#         if re.findall(pattern_frontend_tags, text):
#             if re.findall(pattern_frontend, body):
#                 profession.append('frontend')
#             # else:
#             #     profession.append('frontend')
#
#         if re.findall(pattern_qa_tags, text):
#             if re.findall(pattern_qa, body):
#                 profession.append('qa')
#             # else:
#             #     profession.append('qa')
#
#         if re.findall(pattern_fullstack_tags, text):
#             if re.findall(pattern_fullstack, body):
#                 profession.append('fullstack')
#             # else:
#             #     profession.append('fullstack')
#
#         if re.findall(pattern_designer_tags, text):
#             if re.findall(pattern_designer, body):
#                 profession.append('designer')
#             # else:
#             #     profession.append('designer')
#
#         if re.findall(pattern_mobile_tags, text):
#             if re.findall(pattern_mobile, body):
#                 profession.append('mobile')
#             # else:
#             #     profession.append('mobile')
#
#         if re.findall(pattern_project_manager_tags, text):
#             if re.findall(pattern_project_manager, body):
#                 profession.append('pm')
#             # else:
#             #     profession.append('pm')
#
#         if re.findall(patter_product_manager_tags, text):
#             if re.findall(patter_product_manager, body):
#                 profession.append('product')
#             # else:
#             #     profession.append('product')
#
#         if re.findall(pattern_game_tags, text):
#             if re.findall(pattern_game, body):
#                 profession.append('game')
#             # else:
#             #     profession.append('game')
#
#         if re.findall(pattern_ba_tags, text):
#             if re.findall(pattern_ba, body):
#                 profession.append('ba')
#             # else:
#             #     profession.append('ba')
#
#         if re.findall(pattern_devops_tags, text):
#             if re.findall(pattern_devops, body):
#                 profession.append('devops')
#             # else:
#             #     profession.append('devops')
#
#         if re.findall(pattern_marketing_tags, text):
#             if re.findall(pattern_marketing, body):
#                 profession.append('marketing')
#             # else:
#             #     profession.append('marketing')
#
#         if re.findall(pattern_analyst_tags, text):
#             if re.findall(pattern_analyst, body):
#                 profession.append('analyst')
#             # else:
#             #     profession.append('analyst')
#
#         if re.findall(pattern_hr_tags, text):
#             if re.findall(pattern_hr, body):
#                 profession.append('hr')
#             # else:
#             #     profession.append('hr')
#
#         if ('fullstack' in profession and 'backend' in profession) or \
#                 ('fullstack' in profession and 'frontend' in profession) \
#                 or (re.findall(pattern_fullstack, body) and 'fullstack' in profession and 'frontend' in profession):
#             profession = ['fullstack',]
#
#         if 'designer' in profession and 'game' in profession:
#             profession = ['designer', ]
#
#         if ('qa' in profession and 'backend' in profession) or ('qa' in profession and 'frontend' in profession):
#             profession = ['qa', ]
#
#         if ('backend' in profession or 'frontend' in profession) and re.findall(pattern_fullstack, body):
#             profession = ['fullstack', ]
#
#
# #-------------если по тегам нет совпадений, ищем в body по ключ словам---------------
#         if not profession:
#             profession_dict = {
#                 'backend': 0,
#                 'frontend': 0,
#                 'qa': 0,
#                 'fullstack': 0,
#                 'designer': 0,
#                 'mobile': 0,
#                 'pm': 0,
#                 'product': 0,
#                 'game': 0,
#                 'ba': 0,
#                 'devops': 0,
#                 'marketing': 0,
#                 'analyst': 0,
#                 'hr': 0,
#             }
#
#             pattern_dict = {'backend': pattern_backend,
#                             'frontend': pattern_frontend,
#                             'qa': pattern_qa,
#                             'fullstack': pattern_fullstack,
#                             'designer': pattern_designer,
#                             'mobile': pattern_mobile,
#                             'pm': pattern_project_manager,
#                             'product': patter_product_manager,
#                             'game': pattern_game,
#                             'ba': pattern_ba,
#                             'devops': pattern_devops,
#                             'marketing': pattern_marketing,
#                             'analyst': pattern_analyst,
#                             'hr': pattern_hr
#             }
#             max_value = 0
#             for i in pattern_dict:
#                 search = re.findall(pattern_dict[i], body)
#
#                 if search:
#                     profession_dict[i] = len(search)
#                     if len(search) > max_value:
#                         max_value = len(search)
#
#             if max_value>0:
#                 for key in profession_dict:
#                     if profession_dict[key]>=max_value:
#                         profession = [key, ]
#                         break
#
#             if (profession_dict['frontend'] == max_value or profession_dict['backend'] == max_value) \
#                     and profession_dict['fullstack']>0:
#                 profession = ['fullstack', ]
#
#             if (profession_dict['frontend'] == max_value or profession_dict['backend'] == max_value) and profession_dict['qa']>0:
#                 profession = ['qa']
#
#             if profession_dict['game'] == max_value and profession_dict['designer']>0:
#                 profession = ['designer', ]
#
#         if not profession:
#             pass #'пробежаться по тегам отдельно, но насколько это корректно'
#
#         if not profession:
#             profession = ['no_sort',]
#
#         if len(profession)>1:
#             profession = ['no_sort',]
#
#         # print(profession)
#
#
#         return profession
#

    def sort_by_profession3(self, title, body):

        profession_dict = {
            'tags': {'backend': 0,
                    'frontend': 0,
                    'qa': 0,
                    'fullstack': 0,
                    'designer': 0,
                    'mobile': 0,
                    'pm': 0,
                    'product': 0,
                    'game': 0,
                    'ba': 0,
                    'devops': 0,
                    'marketing': 0,
                    'hr': 0,
                    'ad': 0,
                     },
            'body': {'backend': 0,
                    'frontend': 0,
                    'qa': 0,
                    'fullstack': 0,
                    'designer': 0,
                    'mobile': 0,
                    'pm': 0,
                    'product': 0,
                    'game': 0,
                    'ba': 0,
                    'devops': 0,
                    'marketing': 0,
                    'hr': 0,
                    'ad': 0,
                     },
        }

        pattern = {
            'backend': ('backend', 'back end', 'back-end', 'бэкэнд', 'бэкенд', 'бэк-энд', 'бэк-енд', 'бекенд',
                           'python', 'scala', 'linux', 'c\+\+', 'php', 'java', 'django', 'docker', 'linux', 'websocket',
                           'pandas', 'flask', r'\Wrust', 'goland'),
            'frontend': ('frontend', 'front end', 'front-end', 'фронтэнд', 'фронтенд', 'фронт-энд', 'фронт-енд',
                            'фронт энд', 'фронт енд', 'javascript', 'html', 'react', 'firebase', 'vue.js', 'vuejs',
                            'ether.js', 'etherjs', 'web3.js', 'web3js', 'angular', 'css'),
            'qa': (' qa', 'qa ', 'qa-', 'qa/', 'qaauto', 'manual', 'qaengineer', 'qa engineer', 'тестировщик', 'test ',
                   'automation', 'automatic testing', 'автоматизация процессов тестирования', 'тестировщика',
                   'тестированию', 'автоматизация тестирования', 'автотестировании', 'ручном тестировании', 'тестировании', 'auto',
                   'автоматизатора', 'автоматизатор', 'автоматизации тестирования', 'test automation'),
            'fullstack': ('fullstack', 'full stack', 'full-stack', 'fullstack qa', 'фуллстэк', 'фуллстек', 'фулстэк', 'фулстек'),
            'designer': ('designer', 'дизайнер', 'ui/ux', 'ui ', 'uikit'),
            'mobile': ('android', 'ios ', 'flutter', 'kotlin', 'mobile', 'swift', 'андроид'),
            'pm': ('project manager', 'project-manager', 'projectmanager', 'project/manager', 'pm ', 'проджект менеджер', 'проджект-менеджер', 'менеджерпроекта'),
            'product': ('product manager', 'product-manager', 'productmanager', 'продакт менеджер', 'продукт менеджер',
                        'подакт-менеджер', 'продукт-менеджер', 'продактменеджер', 'продуктменеджер', 'business development manager', 'business development'),
            'game': ('game ', r'\Wunity', 'unreal', 'match-3', 'match3', 'pipeline'),
            'ba': ('business analyst', 'бизнес аналитик', 'ba '),
            'devops': ('devops', 'dev ops', 'девопс', 'дев опс'),
            'marketing': ('smm', 'copyrighter', 'seo'),
            'hr': ('hr', 'recruiter', 'human'),
            'ad': ('резюме', 'cv ', 'ищу работу', 'ищуработу', 'opentowork', 'фильм на вечер', 'рекомендую', 'хотим рассказать о новых каналах',
                    'кадровое агентство', 'skillbox', 'зарабатывать на крипте', 'секретар', 'делопроизводител',
                    'онлайн курс', 'образовательная платформа', 'блоге', 'блог', 'меня зовут', 'к курсу', 'курсы', 'со скидкой',
                   'бесплатном марафоне', 'это помогает нам стать лучше для вас', 'получайте больше откликов', '3dartist', '3d artist',
                   'бесплатном интенсиве', 'бесплатный интенсив', 'в онлайн-интенсиве'),
        }

        exclude_fullstack = ('position: senior fullstack', 'position: fullstack', 'position: full-stack', 'вакансия: fullstack',
                    'вакансия: full-stack',
                    'senior full-stack developer', 'senior fullstack developer')

        text = title + body
        text = text.lower()
        profession = []

        text_without_tags = text
        tags = re.findall(r'#[a-zа-я]*\W', text)
        for t in tags:
            text_without_tags = text_without_tags.replace(t, '')

        search_body = []
        for pro in pattern:
            for i in pattern[pro]:
                search_tags = re.findall(f'#{i}', text)
                if i == 'резюме':
                    search_body = []
                elif i == 'cv ':
                    search_body = []
                else:
                    search_body = re.findall(i, text_without_tags)

                if search_tags:
                    profession_dict['tags'][pro] += len(search_tags)
                if search_body:
                    profession_dict['body'][pro] += len(search_body)

        for key in profession_dict:
            print(f'{key} = ', profession_dict[key])

        if profession_dict['tags']['ad'] or profession_dict['body']['ad']:
            return 'ad'

        if profession_dict['tags']['backend'] and profession_dict['body']['backend']:
            profession.append('backend')
        if profession_dict['tags']['frontend'] and profession_dict['body']['frontend']:
            profession.append('frontend')
        if profession_dict['tags']['qa'] and profession_dict['body']['qa']:
            profession.append('qa')
        if profession_dict['tags']['fullstack'] and profession_dict['body']['fullstack']:
            profession.append('fullstack')
        if profession_dict['tags']['designer'] and profession_dict['body']['designer']:
            profession.append('designer')
        if profession_dict['tags']['mobile'] and profession_dict['body']['mobile']:
            profession.append('mobile')
        if profession_dict['tags']['pm'] and profession_dict['body']['pm']:
            profession.append('pm')
        if profession_dict['tags']['product'] and profession_dict['body']['product']:
            profession.append('product')
        if profession_dict['tags']['game'] and profession_dict['body']['game']:
            profession.append('game')
        if profession_dict['tags']['ba'] and profession_dict['body']['ba']:
            profession.append('ba')
        if profession_dict['tags']['devops'] and profession_dict['body']['devops']:
            profession.append('devops')
        if profession_dict['tags']['marketing'] and profession_dict['body']['marketing']:
            profession.append('marketing')
        if profession_dict['tags']['hr'] and profession_dict['body']['hr']:
            profession.append('hr')

        print(profession)

        t = False

        if len(profession)>1 or not profession:
            result_tags = self.find_profession(profession, profession_dict, 'tags')
        else:
            if 'backend' in profession:
                for i in exclude_fullstack:
                    if re.findall(i, text):
                        t = True
                        break
            if t:
                return 'fullstack'
            else:
                return profession[0]

        if result_tags != 'no_sort':
            return result_tags
        else:
            profession = []
            if profession_dict['body']['backend']:
                profession.append('backend')
            if profession_dict['body']['frontend']:
                profession.append('frontend')
            if profession_dict['body']['qa']:
                profession.append('qa')
            if profession_dict['body']['fullstack']:
                profession.append('fullstack')
            if profession_dict['body']['designer']:
                profession.append('designer')
            if profession_dict['body']['mobile']:
                profession.append('mobile')
            if profession_dict['body']['pm']:
                profession.append('pm')
            if profession_dict['body']['product']:
                profession.append('product')
            if profession_dict['body']['game']:
                profession.append('game')
            if profession_dict['body']['ba']:
                profession.append('ba')
            if profession_dict['body']['devops']:
                profession.append('devops')
            if profession_dict['body']['marketing']:
                profession.append('marketing')
            if profession_dict['body']['hr']:
                profession.append('hr')

            print(profession)

            result_body = self.find_profession(profession, profession_dict, 'body')

            if result_body == 'backend' and re.findall(r'[^\#]qa', text[0:30]):
                return 'qa'
            if re.findall(r'[^\#]java|[^\#]python|[^\#]php|[^\#]backend|[^\#]scala', text[0:40]):
                return 'backend'

            max_value = 0
            key = ''
            if result_body == 'no_sort':
                for item in profession_dict['tags']:
                    if profession_dict['tags'][item] > max_value:
                        max_value = profession_dict['tags'][item]
                        key = item
                if key:
                    return key

        return result_body


    def find_profession(self, profession, profession_dict, field):

        profession_final = ''

        if 'fullstack' in profession:
            return 'fullstack'

        if 'devops' in profession:
            if 'backend' in profession and 'frontend' in profession:
                if profession_dict[field]['backend']/2 > profession_dict[field]['devops'] and profession_dict[field]['backend']>profession_dict[field]['frontend']:
                    return 'backend'
                elif profession_dict[field]['frontend']>profession_dict[field]['backend']:
                    return 'frontend'

            elif 'backend' in profession and profession_dict[field]['backend']/2 > profession_dict[field]['devops']:
                return 'backend'

            elif 'frontend' in profession and profession_dict[field]['frontend']/2 > profession_dict[field]['devops']:
                return 'frontend'

            elif 'qa' in profession:
                if profession_dict[field]['qa'] / 2 > profession_dict[field]['devops']:
                    return 'qa'

            else:
                return 'devops'
            return 'devops'

        if 'qa' in profession:
            if 'backend' in profession and 'frontend' in profession:
                if profession_dict[field]['backend'] / 2 > profession_dict[field]['qa'] and profession_dict[field]['backend'] > profession_dict[field]['frontend']:
                    return 'backend'
                elif profession_dict[field]['frontend'] /2 > profession_dict[field]['qa']:
                    return 'frontend'

            elif 'backend' in profession and profession_dict[field]['backend']/2 > profession_dict[field]['qa']:
                return 'backend'

            elif 'frontend' in profession and profession_dict[field]['frontend']/2 > profession_dict[field]['qa']:
                return 'frontend'

            else:
                return 'qa'
            return 'qa'

        if 'mobile' in profession:
            return 'mobile'

        if 'game' in profession:
            return 'game'

        if 'backend' in profession and 'frontend' in profession:
            if profession_dict['tags']['backend'] + profession_dict['body']['backend'] > profession_dict['tags']['frontend'] + profession_dict['body']['frontend']:
                return 'backend'
            else:
                return 'frontend'

        if 'backend' in profession:
            return 'backend'

        if 'frontend' in profession:
            return 'frontend'

        max_value = 0
        for i in ['pm', 'product', 'ba', 'marketing', 'hr']:
            if profession_dict[field][i]>max_value:
                max_value = profession_dict[field][i]
                profession_final = i
        if profession_final:
            return profession_final
        else:
            return 'no_sort'


profession = Professions().sort_by_profession3(t_text_title, t_text_body)
print('profession = ', profession)