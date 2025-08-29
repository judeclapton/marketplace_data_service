import os
from dotenv import load_dotenv
from api_client import ApiClient
from daily_data_loader import DailyDataLoader
from data_parsing import DataParsing
from db_client import DBClient
from historical_data_loader import HistoricalDataLoader
from logger import Logger


class App:
    def __init__(self, historical_data_needed=False):
        load_dotenv()

        self.project_name = os.path.basename(os.getcwd())
        self.logger = Logger('App').get_logger()
        self.api_client = ApiClient()
        self.parser = DataParsing()
        self.db_client = DBClient()
        self.historical_data_needed = historical_data_needed

        if self.historical_data_needed:
            self.historical_loader = HistoricalDataLoader(self.api_client, self.db_client, self.parser)
        self.daily_loader = DailyDataLoader(self.api_client, self.db_client, self.parser)

    def run(self):
        self.logger.info(f'Запуск работы «{self.project_name}»')
        try:
            self.db_client.create_table()
            if self.historical_data_needed:
                self.historical_loader.load()
            self.daily_loader.load()
        except Exception as ex:
            self.logger.error(f'Возникла ошибка: {ex}. Завершение работы')
        finally:
            self.db_client.close_connection()
            self.logger.info(f'Завершение работы «{self.project_name}»')
            self.logger.info('=' * 60 + '\n')
