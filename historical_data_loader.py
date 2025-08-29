import os
from datetime import date, timedelta
import requests
from base_data_loader import _BaseDataLoader


class HistoricalDataLoader(_BaseDataLoader):
    def __init__(self, api_client, db_client, parser,
                 from_date=date(2020, 1, 1), name='HistoricalDataLoader'):
        super().__init__(api_client, db_client, parser, name)
        self.from_date = from_date
        self.api_url = os.getenv('API_URL')

    def _has_data(self, date):
        url = f'{self.api_client.api_url}?date={date}'
        return requests.get(url).text != 'Информация за более ранние периоды отсутствует'

    def _get_first_date(self):
        self.logger.info('Поиск первой даты с данными')
        low = self.from_date
        step = timedelta(days=365)
        high = low

        while not self._has_data(high):
            high += step

        while low < high:
            mid = low + (high - low) // 2
            if self._has_data(mid):
                high = mid
            else:
                low = mid + timedelta(days=1)

        self.logger.info(f'Первая доступная дата с данными: {low}')
        return low

    def load(self):
        start_date = self._get_first_date()
        end_date = date.today() - timedelta(days=1)
        self._load_range(start_date, end_date)
