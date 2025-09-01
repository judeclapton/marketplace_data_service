import os
import requests
from core.logger import Logger


class ApiClient:
    def __init__(self, name='ApiClient'):
        self._api_url = os.getenv('API_URL')
        self._logger = Logger(name).get_logger()

    def get_data(self, date):
        params = {'date': date}
        self._logger.info(f'Запрос данных с API за {date}')

        try:
            response = requests.get(self._api_url, params=params)

            if response.status_code == 200:
                try:
                    data = response.json()
                    self._logger.info(f'Успешно получено {len(data)} записей')
                    return data
                except ValueError:
                    self._logger.error(f'Данных за {date} нет')
                    return []
            else:
                self._logger.warning(f'Ошибка при получении данных: {response.text}')
                return []
        except requests.exceptions.RequestException as ex:
            self._logger.error(f'Ошибка соединения с API: {ex}')
            return []
