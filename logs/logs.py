from datetime import datetime


class Logs:

    async def write_log(self, text, session):
        operation_time = datetime.now().strftime('%d-%m-%y %H:%M:%S')
        with open(f'./logs/logs_{session}') as f:
            f.write(f'{operation_time} | text\n')