from core.logger import Logger
import pandas as pd


class DataParsing:
    def __init__(self, name='DataParsing'):
        self._logger = Logger(name).get_logger()

    @staticmethod
    def _preprocess(record):
        record['purchase_datetime'] = pd.to_datetime(record['purchase_datetime'])
        record['purchase_datetime'] = record['purchase_datetime'] + pd.to_timedelta(
            record['purchase_time_as_seconds_from_midnight'], unit='s')
        del record['purchase_time_as_seconds_from_midnight']

    def _validate_record(self, record):
        req_fields = ['client_id', 'gender', 'purchase_datetime', 'purchase_time_as_seconds_from_midnight',
                      'product_id', 'quantity', 'price_per_item',
                      'discount_per_item', 'total_price']
        for field in req_fields:
            if field not in record:
                self._logger.warning(f'Пропущено или пустое поле {field} в записи: {record}')
                return False

        if record['gender'] not in ['F', 'M']:
            self._logger.warning(f'Недопустимое значение gender: {record["gender"]} в записи: {record}')
            return False

	if record['quantity'] == 0 and record['total_price'] == 0:
            self._logger.warning(f'Нулевые значения количества и итоговой цены в записи: {record}')
            return False

        numeric_fields = ['quantity', 'price_per_item', 'discount_per_item', 'total_price']
        for field in numeric_fields:
            if record[field] < 0:
                self._logger.warning(f'Отрицательное значение в поле {field}: {record[field]} в записи: {record}')
                return False

        try:
            self._preprocess(record)
        except Exception as ex:
            self._logger.error(f'Ошибка при преобразовании даты/времени: {ex} в записи: {record}')
            return False

        return True

    def process(self, input_data):
        output_data = []
        for record in input_data:
            if self._validate_record(record):
                valid_record = {
                    'client_id': record['client_id'],
                    'gender': record['gender'],
                    'purchase_datetime': record['purchase_datetime'],
                    'product_id': record['product_id'],
                    'quantity': record['quantity'],
                    'price_per_item': record['price_per_item'],
                    'discount_per_item': record['discount_per_item'],
                    'total_price': record['total_price']
                }
                output_data.append(valid_record)
        self._logger.info(f'Успешно обработано {len(output_data)} записей из {len(input_data)}')
        return output_data
