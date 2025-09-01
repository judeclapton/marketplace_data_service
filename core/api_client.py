import os
import requests
from core.logger import Logger


class ApiClient:
    def __init__(self, name='ApiClient'):
        self.api_url = os.getenv('API_URL')
        self.logger = Logger(name).get_logger()

    def get_data(self, date):
        params = {'date': date}
        self.logger.info(f'Запрос данных с API за {date}')

        try:
            response = requests.get(self.api_url, params=params)

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
