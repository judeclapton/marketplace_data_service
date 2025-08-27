import os
import requests
from logger import Logger
from dotenv import load_dotenv


class ApiClient:
    def __init__(self, name='ApiClient'):
        load_dotenv()

        self.api_url = os.getenv('API_URL')
        self.logger = Logger(name).get_logger()

    def get_data(self, date):
        if not isinstance(date, str):
            date = date.isoformat()

        params = {'date': date}
        self.logger.info(f'Запрос данных с API за {date}')

        try:
            response = requests.get(self.api_url, params=params)
            self.logger.info(f'Ответ от API: статус {response.status_code}')

            if response.status_code == 200:
                try:
                    data = response.json()
                    self.logger.info(f'Успешно получено {len(data)} записей')
                    return data
                except ValueError:
                    self.logger.error(f'Данных за {date} нет')
                    return []
            else:
                self.logger.warning(f'Ошибка при получении данных: {response.text}')
                return []
        except requests.exceptions.RequestException as ex:
            self.logger.error(f'Ошибка соединения с API: {ex}')
            return []
