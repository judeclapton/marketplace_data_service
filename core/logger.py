import logging
import os
from datetime import datetime


class Logger:
    def __init__(self, name):
        self.logger = None
        self.name = name
        self.directory = '_logs'

        self._logger_configure()

    def _logger_configure(self):
        os.makedirs(self.directory, exist_ok=True)
        file_name = datetime.now().strftime('%Y-%m-%d.txt')
        path = os.path.join(self.directory, file_name)

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            handler = logging.FileHandler(path, encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def get_logger(self):
        return self.logger
