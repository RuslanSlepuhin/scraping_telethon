import datetime
import random
import re
import time
import telebot
import configparser
# import scraping_telegramchats


from telebot import types
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

config = configparser.ConfigParser()
config.read("config.ini")
no_sorted_channel = config['My_channels']['no_sorted_channel']
backend_channel = config['My_channels']['backend_channel']
frontend_channel = config['My_channels']['frontend_channel']
devops_channel = config['My_channels']['devops_channel']
fullstack_channel = config['My_channels']['fullstack_channel']
pm_channel = config['My_channels']['pm_channel']
product_channel = config['My_channels']['product_channel']
designer_channel = config['My_channels']['designer_channel']
analyst_channel = config['My_channels']['analyst_channel']
qa_channel = config['My_channels']['qa_channel']
hr_channel = config['My_channels']['hr_channel']
mobile_channel = config['My_channels']['mobile_channel']
game_channel = config['My_channels']['game_channel']
test_channel = config['My_channels']['test_channel']
ba_channel = config['My_channels']['ba_channel']
marketing_channel = config['My_channels']['marketing_channel']
agregator_channel = config['My_channels']['agregator_channel']
junior_channel = config['My_channels']['junior_channel']
middle_channel = config['My_channels']['middle_channel']
senior_channel = config['My_channels']['senior_channel']

ruslan_channel = config['My_channels']['ruslan_channel']

bot = telebot.TeleBot("5484849364:AAF0fPhis-GLuQNsSdR7EbmnYa0QjTXpGdE")
print('bot is working...')


def main():
    @bot.message_handler(commands=['start'])
    def welcome_user(message):
        bot.send_message(message.chat.id, f'Привет, <b>{message.from_user.first_name}</b>!', parse_mode='html')

    @bot.message_handler(commands=['scrape'])
    def scrape(message):
        bot.send_message(message.chat.id, 'Начался инвайт')
        # scraping_telegramchats.main()



    @bot.message_handler(content_types=['text'])
    def some_text(message):

        channels = []
        text = message.text

        numbers_message = text.split("/", 1)[0]  # ver old
        text = text.replace(f'{numbers_message}/', '')  # ver old

        for i in range(0, int(numbers_message)):
            channel = text.split("/", 1)[0]  # ver old
            channels.append(channel)
            text = text.replace(f'{channel}/', '')

        message_to_send = text

        message_for_print = message_to_send.replace(f'\n', '')[0:40]
        print(f"message = {message_for_print}")

        if 'middle' in channels and 'senior' in channels:
            channels.remove('senior')  # чтобы не задваивалось отправление в один и тот же канал middle/senior


        contacts = search_contacts(message_to_send)
        print(contacts)
        pass

        for channel in channels:
            markup = collect_online_keyboard({'names': ['откликнуться', ], 'callbacks': ['calling', ]})
            try:
                match channel:
                    case 'backend':
                        bot.send_message(backend_channel, message_to_send, reply_markup=markup)  #+

                    case 'frontend':
                        bot.send_message(frontend_channel, message_to_send, reply_markup=markup)  #+

                    case 'devops':
                        bot.send_message(devops_channel, message_to_send, reply_markup=markup)  #+

                    case 'fullstack':
                        bot.send_message(fullstack_channel, message_to_send, reply_markup=markup)  #+

                    case 'mobile':
                        bot.send_message(mobile_channel, message_to_send, reply_markup=markup)  #+

                    case 'pm':
                        bot.send_message(pm_channel, message_to_send, reply_markup=markup)  #+

                    case 'product':
                        bot.send_message(pm_channel, message_to_send, reply_markup=markup)  #+

                    case 'designer':
                        bot.send_message(designer_channel, message_to_send, reply_markup=markup)  #+

                    case 'qa':
                        bot.send_message(qa_channel, message_to_send, reply_markup=markup)  #+

                    case 'analyst':
                        bot.send_message(analyst_channel, message_to_send, reply_markup=markup)  #+

                    case 'hr':
                        bot.send_message(hr_channel, message_to_send, reply_markup=markup)  #+

                    case 'game':
                        bot.send_message(game_channel, message_to_send, reply_markup=markup)  #+

                    case 'ba':
                        bot.send_message(ba_channel, message_to_send, reply_markup=markup)  #+

                    case 'marketing':
                        bot.send_message(marketing_channel, message_to_send, reply_markup=markup)  # +

                    case 'ad':
                        markup = None
                        bot.send_message(no_sorted_channel, f'{channel}\n\n' + message_to_send, reply_markup=markup)

                    case 'no_sort':
                        markup = None
                        bot.send_message(no_sorted_channel, f'{channel}\n\n' + message_to_send, reply_markup=markup)

                    case 'junior':
                        bot.send_message(junior_channel, message_to_send, reply_markup=markup)

                    case 'middle':
                        pass

                    case 'sales_manager':
                        pass

                    case 'senior':
                        pass

                    case 'sales_manager':
                        pass  # because that profession was not in plane

            except Exception as e:
                print('FUCKING ERROR', e)

            print(f"{datetime.datetime.now().strftime('%H:%M')} channel = {channel}")
            time.sleep(1)

        agregator_links = ''
        for i in channels:
            if i in ['no_sort', 'ad']:
                agregator_links = ''
                break
            else:
                agregator_links += f"\n\nБольше вакансий {i.upper()} " \
                           f"по ссылке {config['Links'][i]}"

        if agregator_links:
            try:
                bot.send_message(agregator_channel, message_to_send + agregator_links, parse_mode='html')  # old
                print(f"{datetime.datetime.now().strftime('%H:%M')} channel = agregator")
                time.sleep(1)
            except Exception as e:
                print("MOTHER FUCKER ERROR", e)
        print()

        bot.delete_message(message.chat.id, message.message_id)
        # time.sleep(random.randrange(3,7))

        @bot.callback_query_handler(func=lambda call: True)
        def send_inline(call):
            if call.data == 'calling':
                bot.answer_callback_query(
                    call.id,
                    text='You have pressed 😉', show_alert=True)

    bot.polling()


def search_contacts(message_to_send):
    """ Search contacts on message and return in main code """
    pattern_contacts = r'https:\/\/t\.me\/|@[a-zA-Z|\-_\d]{1,}|www\.|http:\/\/'
    contacts = re.findall(pattern_contacts, message_to_send)

    return contacts

def control_clear(text):
    text = re.sub(r'<[\W\w\d]{1,10}>', '', text)
    return text


def collect_online_keyboard(online_menu_dict, row_width=2):
    n=0
    markup = InlineKeyboardMarkup(row_width=row_width)
    while n<len(online_menu_dict['names']):
        markup.add(InlineKeyboardButton(online_menu_dict['names'][n], callback_data=online_menu_dict['callbacks'][n]))
        n += 1

    return markup



main()