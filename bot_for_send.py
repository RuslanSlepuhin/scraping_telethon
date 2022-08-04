import telebot

# bot = telebot.TeleBot("5484849364:AAF0fPhis-GLuQNsSdR7EbmnYa0QjTXpGdE", parse_mode=None)
# bot = telebot.TeleBot("5484849364:AAF0fPhis-GLuQNsSdR7EbmnYa0QjTXpGdE")

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
        bot.send_message(-1001345406077, message.text)
    else:
        profession = message.text.split("/", 1)
        message_to_send = message.text.replace(profession)[1:-1]
        # match profession:
        #     case:
        pass






def send_invite(message, invite_text):
    list_id = [137336064, 758905227, 97129286, 556128576]  #Руслан, Наташа, Александр, Женя
    for user in list_id:
        try:
            bot.send_message(user, invite_text)
        except Exception as e:
            bot.send_message(message.chat.id, f'Didn\'t send invite to user_id {user}: {e}')
    bot.send_message(message.chat.id, 'Sending invite-link completed successfully')

bot.polling()




