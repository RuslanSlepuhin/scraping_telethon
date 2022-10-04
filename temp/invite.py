import configparser
from telethon.sync import TelegramClient
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.types import InputPeerChannel ,InputUser
from telethon.tl.functions.contacts import ResolveUsernameRequest
from telethon.tl.functions.channels import GetParticipantsRequest
from telethon.tl.types import ChannelParticipantsSearch

config = configparser.ConfigParser()
config.read("config.ini")

# ad_channel = config['My_channels']['ad_channel']
# backend_channel = config['My_channels']['backend_channel']
# frontend_channel =config['My_channels']['frontend_channel']
# devops_channel = config['My_channels']['devops_channel']
# fullstack_channel = config['My_channels']['fullstack_channel']
# pm_channel = config['My_channels']['pm_channel']
# product_channel = config['My_channels']['product_channel']
# designer_channel = config['My_channels']['designer_channel']
# analyst_channel = config['My_channels']['analyst_channel']
# qa_channel = config['My_channels']['qa_channel']
# hr_channel = config['My_channels']['hr_channel']
# alexandr_channel = config['My_channels']['alexandr_channel']
bot = config['My_channels']['bot']

api_id = int(config['TelegramRuslan']['api_id'])
api_hash = config['TelegramRuslan']['api_hash']
username = config['TelegramRuslan']['username']
phone = '+375296449690'  #телеграм Руслан

ruslan_channel = config['My_channels']['ruslan_channel']


client = TelegramClient(phone, api_id, api_hash)
client.connect()


async def invite(all_participant, channel_to_send):
    # url = 'https://t.me/z_developer_channel'
    # chann = await client.get_entity(url)

# ---------сбор инфы о канале куда отправлять, но можно и без этого всего, просто отправить на id канала ------------

    # channel_test = await client.get_entity(-1001345406077)
    # channel_id = channel_test.id
    # channel_access_hash = channel_test.access_hash
    # chanal_test=InputPeerChannel(channel_id, channel_access_hash)  #channel_access_hash


# -------------------получить пользователей группы --------------------------------

    # offset_user = 0  # номер участника, с которого начинается считывание
    # limit_user = 100  # максимальное число записей, передаваемых за один раз
    #
    # all_participants = []  # список всех участников канала
    # filter_user = ChannelParticipantsSearch('')
    #
    # try:
    #     while True:
    #         participants = await client(GetParticipantsRequest(chann, filter_user, offset_user, limit_user, hash=0))
    #         if not participants.users:
    #             break
    #         all_participants.extend(participants.users)
    #         offset_user += len(participants.users)
    # except Exception as e:
    #     print(e)

    # user = all_participants[0]  #client(ResolveUsernameRequest())
    #
    # user=InputUser(user.id,user.access_hash)

    all_participants = [137336064, 758905227, 97129286, 556128576]


#-------------------------отправить в нужный канал всех полученных юзеров --------------------------
    await client(InviteToChannelRequest(-1001427569857, all_participants))
    print('Added users', [i for i in all_participants])


def start():
    with client:
        client.loop.run_until_complete(invite(-1001427569857, [137336064, 758905227, 97129286, 556128576]))

start()
