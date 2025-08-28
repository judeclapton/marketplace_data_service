from datetime import timedelta

from logger import Logger


class _BaseDataLoader:
    def __init__(self, api_client, name='DataLoader'):
        self.logger = Logger(name).get_logger()
        self.api_client = api_client

    def _fetch_date(self, date):
        self.logger.info(f'Запрос данных за {date}')
        return self.api_client.get_data(date)

    def _load_range(self, start_date, end_date):
        current_date = start_date
        while current_date <= end_date:
            self.logger.info(f'Обработка даты: {current_date}')
            data = self._fetch_date(current_date)
            if data:
                self.logger.info(f'Получено {len(data)} записей за {current_date}')
                # TODO: тут будет загрузка данных в БД
            else:
                self.logger.info(f'Данных за {current_date} нет')
            current_date += timedelta(days=1)
