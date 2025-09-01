import os
from datetime import datetime
from dotenv import load_dotenv
from core.api_client import ApiClient
from loaders.daily_data_loader import DailyDataLoader
from core.data_parsing import DataParsing
from core.db_client import DBClient
from loaders.historical_data_loader import HistoricalDataLoader
from core.logger import Logger


class App:
    def __init__(self, historical_data_needed=False):
        load_dotenv(dotenv_path=os.path.join('config', '.env'))

        self._project_name = os.path.basename(os.getcwd())
        self._logger = Logger('App').get_logger()
        self._api_client = ApiClient()
        self._parser = DataParsing()
        self._db_client = DBClient()
        self._historical_data_needed = historical_data_needed

        if self._historical_data_needed:
            self._historical_loader = HistoricalDataLoader(self._api_client, self._db_client, self._parser)
        else:
            self._daily_loader = DailyDataLoader(self._api_client, self._db_client, self._parser)

    def run(self):
        start_time = datetime.now()
        self._logger.info(f'Запуск работы «{self._project_name}»')
        try:
            self._db_client.create_table()
            if self._historical_data_needed:
                self._logger.info('Запуск загрузки исторических данных')
                self._historical_loader.load()
            else:
                self._logger.info('Запуск загрузки данных за предыдущий день')
                self._daily_loader.load()
        except Exception as ex:
            self._logger.error(f'Возникла ошибка: {ex}. Завершение работы')
        finally:
            self._db_client.close_connection()
            self._logger.info(f'Завершение работы «{self._project_name}»')
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self._logger.info(f'Загрузка завершена за {duration:.2f} секунд')
            self._logger.info('=' * 60 + '\n')
