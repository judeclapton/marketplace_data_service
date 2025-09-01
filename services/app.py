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

        self.project_name = os.path.basename(os.getcwd())
        self.logger = Logger('App').get_logger()
        self.api_client = ApiClient()
        self.parser = DataParsing()
        self.db_client = DBClient()
        self.historical_data_needed = historical_data_needed

        if self.historical_data_needed:
            self.historical_loader = HistoricalDataLoader(self.api_client, self.db_client, self.parser)
        else:
            self.daily_loader = DailyDataLoader(self.api_client, self.db_client, self.parser)

    def run(self):
        start_time = datetime.now()
        self.logger.info(f'Запуск работы «{self.project_name}»')
        try:
            self.db_client.create_table()
            if self.historical_data_needed:
                self.logger.info('Запуск загрузки исторических данных')
                self.historical_loader.load()
            else:
                self.logger.info('Запуск загрузки данных за предыдущий день')
                self.daily_loader.load()
        except Exception as ex:
            self.logger.error(f'Возникла ошибка: {ex}. Завершение работы')
        finally:
            self.db_client.close_connection()
            self.logger.info(f'Завершение работы «{self.project_name}»')
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self.logger.info(f'Загрузка завершена за {duration:.2f} секунд')
            self.logger.info('=' * 60 + '\n')
