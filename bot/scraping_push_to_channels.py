import asyncio
import configparser
from db_operations.scraping_db import DataBaseOperations

config = configparser.ConfigParser()
config.read("./settings/config.ini")

bot = config['My_channels']['bot']

class PushChannels:
    async def push(self, results_dict, client, i, bot=bot):
        block = False

        # print('PUSH_TO_DB')
        response_dict = DataBaseOperations(con=None).push_to_bd(results_dict)
        channels = response_dict.keys()

        if 'block' in channels:
            block = response_dict['block']

        channel_list = []
        if not block:
            message = ''
            length = 0
            for channel in channels:
                if not response_dict[channel]:
                    message = message + f'{channel}/'
                    channel_list.append(channel)
                    length += 1

            if message:
                await client.send_message(entity=bot, message=f"{length}/{message}{i['message']}")  #!!!!!!!!
                await asyncio.sleep(2)
                for i in channel_list:
                    print(f"pushed to channel = {i}\n")

        else:
            pass
        pass