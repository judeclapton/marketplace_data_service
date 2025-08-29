from datetime import date, timedelta

from base_data_loader import _BaseDataLoader


class DailyDataLoader(_BaseDataLoader):
    def __init__(self, api_client, parser, name='DailyDataLoader'):
        super().__init__(api_client, parser, name)

    def load(self):
        target_date = date.today() - timedelta(days=1)
        self._load_range(target_date, target_date)
