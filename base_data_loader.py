from datetime import timedelta
from logger import Logger


class _BaseDataLoader:
    def __init__(self, api_client, db_client, parser, name):
        self.logger = Logger(name).get_logger()
        self.api_client = api_client
        self.parser = parser
        self.db_client = db_client
        self.db_client.create_table()

    def _fetch_date(self, date):
        return self.api_client.get_data(date)

    def _load_range(self, start_date, end_date):
        current_date = start_date
        while current_date <= end_date:
            self.logger.info(f'Обработка даты: {current_date}')
            data = self._fetch_date(current_date)
            if data:
                valid_data = self.parser.process(data)
                if valid_data:
                    self.db_client.insert_data(valid_data)
            else:
                self.logger.info(f'Данных за {current_date} нет')
            current_date += timedelta(days=1)

    def load(self):
        raise NotImplementedError
