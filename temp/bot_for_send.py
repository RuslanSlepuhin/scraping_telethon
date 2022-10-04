import telebot
import configparser

config = configparser.ConfigParser()
config.read("config.ini")
no_sorted_channel = config['My_channels']['no_sorted_channel']
backend_channel = config['My_channels']['backend_channel']
frontend_channel =config['My_channels']['frontend_channel']
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

bot = telebot.TeleBot("5484849364:AAF0fPhis-GLuQNsSdR7EbmnYa0QjTXpGdE")



@bot.message_handler(commands=['start'])
def welcome_user(message):
    bot.send_message(
        message.chat.id,
        f'Привет, <b>{message.from_user.first_name}</b> !\n\n'
        f'Введите текст, я его отправлю в канал', parse_mode='html'
    )
    data = f'{message.from_user.id}\n{message.from_user.username}'
    bot.send_message(137336064, data)

@bot.message_handler(content_types=['text'])
def some_text(message):
    if message.text != 'invite':
        profession = message.text.split("/", 1)[0]
        message_to_send = message.text.replace(f'{profession}/', '')

        bot.send_message(no_sorted_channel, f'{profession}\n\n{message_to_send}')

        match profession:
            case 'backend':
                bot.send_message(backend_channel, message_to_send)  #+
            case 'frontend':
                bot.send_message(frontend_channel, message_to_send)  #+
            case 'devops':
                bot.send_message(devops_channel, message_to_send)  #+
            case 'fullstack':
                bot.send_message(fullstack_channel, message_to_send)  #+
            case 'mobile':
                bot.send_message(mobile_channel, message_to_send)  #+
            case 'pm':
                bot.send_message(pm_channel, message_to_send)  #+
            case 'product':
                bot.send_message(product_channel, message_to_send)  #+
            case 'designer':
                bot.send_message(designer_channel, message_to_send)  #+
            case 'qa':
                bot.send_message(qa_channel, message_to_send)  #+
            case 'analyst':
                 bot.send_message(analyst_channel, message_to_send)  #+
            case 'hr':
                bot.send_message(hr_channel, message_to_send)  #+
            case 'game':
                bot.send_message(game_channel, message_to_send)  #+
            case 'ad':
                bot.send_message(no_sorted_channel, f'{profession}\n\n'+message_to_send)
            case 'ba':
                bot.send_message(no_sorted_channel, f'{profession}\n\n'+message_to_send)  #+
            case 'marketing':
                bot.send_message(no_sorted_channel, f'{profession}\n\n'+message_to_send)  # +
            case 'no_sort':
                bot.send_message(no_sorted_channel, f'{profession}\n\n' + message_to_send)
        print(profession)


def send_invite(message, invite_text):
    list_id = [137336064, 758905227, 97129286, 556128576]  #Руслан, Наташа, Александр, Женя
    for user in list_id:
        try:
            bot.send_message(user, invite_text)
        except Exception as e:
            bot.send_message(message.chat.id, f'Didn\'t send invite to user_id {user}: {e}')
    bot.send_message(message.chat.id, 'Sending invite-link completed successfully')

bot.polling()




