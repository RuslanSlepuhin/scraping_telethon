import asyncio
import random
import time


class ShowProgress:

    def __init__(self, bot_dict):
        self.percent = 0
        self.bot = bot_dict['bot']
        self.chat_id = bot_dict['chat_id']


    async def show_the_progress(self, message, current_number, end_number):
        check = current_number * 100 // end_number
        if check > self.percent:
            quantity = check // 5
            self.percent = check
            self.message = await self.bot.edit_message_text(
                f"progress {'|' * quantity} {self.percent}%", message.chat.id, message.message_id)
        await asyncio.sleep(0.5)