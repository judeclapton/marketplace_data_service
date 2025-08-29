import os
import psycopg2
from logger import Logger
from psycopg2.extras import execute_values


class DBClient:
    def __init__(self, name='DBClient'):
        self.logger = Logger(name).get_logger()
        self.connection = psycopg2.connect(
            user=os.getenv('PG_USER'), password=os.getenv('PG_PASSWORD'),
            host=os.getenv('PG_HOST'), port=os.getenv('PG_PORT'), dbname=os.getenv('PG_DBNAME')
        )
        self.cursor = self.connection.cursor()
        self.table = 'purchases'

    def create_table(self):
        query = f'''
        CREATE TABLE IF NOT EXISTS {self.table} (
            client_id INT,
            gender VARCHAR(1),
            purchase_datetime TIMESTAMP,
            product_id INT,
            quantity INT,
            price_per_item FLOAT,
            discount_per_item FLOAT,
            total_price FLOAT,
            CONSTRAINT unique_purchase UNIQUE (client_id, purchase_datetime, product_id)
        )
        '''
        self.cursor.execute(query)
        self.connection.commit()
        self.logger.info(f'Таблица {self.table} создана или уже существует')

    def _get_table_size(self):
        self.cursor.execute(f'SELECT COUNT(*) FROM {self.table}')
        return self.cursor.fetchone()[0]

    def insert_data(self, data):
        if not data:
            self.logger.warning('Данных для вставки не найдено')
            return

        values = [(
            row['client_id'], row['gender'], row['purchase_datetime'],
            row['product_id'], row['quantity'], row['price_per_item'],
            row['discount_per_item'], row['total_price']
        ) for row in data]

        query = f'''
        INSERT INTO {self.table} (client_id, gender, purchase_datetime, 
        product_id, quantity, price_per_item, discount_per_item, total_price)
        VALUES %s 
        ON CONFLICT (client_id, purchase_datetime, product_id) DO NOTHING
        '''

        try:
            start_table_size = self._get_table_size()

            psycopg2.extras.execute_values(self.cursor, query, values)
            self.connection.commit()

            now_table_size = self._get_table_size()
            new_rows_count = now_table_size - start_table_size

            self.logger.info(f'В таблицу {self.table} добавлено {new_rows_count} новых строк')
        except Exception as ex:
            self.connection.rollback()
            self.logger.error(f'Ошибка при добавлении данных: {ex}')

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
        self.logger.info('Соединение с базой данных закрыто')
