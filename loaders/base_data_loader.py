from abc import ABC, abstractmethod
from datetime import timedelta
from core.logger import Logger


class BaseDataLoader(ABC):
    def __init__(self, api_client, db_client, parser, name):
        self._logger = Logger(name).get_logger()
        self._api_client = api_client
        self._parser = parser
        self._db_client = db_client

    def _fetch_date(self, date):
        return self._api_client.get_data(date)

    def _load_range(self, start_date, end_date):
        current_date = start_date
        while current_date <= end_date:
            self._logger.info(f'Обработка даты: {current_date}')
            data = self._fetch_date(current_date)
            if data:
                valid_data = self._parser.process(data)
                if valid_data:
                    self._db_client.insert_data(valid_data)
            else:
                self._logger.info(f'Данных за {current_date} нет')
            current_date += timedelta(days=1)

    @abstractmethod
    def load(self):
        raise NotImplementedError
