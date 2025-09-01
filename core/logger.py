import logging
import os
from datetime import datetime


class Logger:
    def __init__(self, name):
        self._logger = None
        self._name = name
        self._directory = '_logs'

        self._logger_configure()

    def _logger_configure(self):
        os.makedirs(self._directory, exist_ok=True)
        file_name = datetime.now().strftime('%Y-%m-%d.txt')
        path = os.path.join(self._directory, file_name)

        self._logger = logging.getLogger(self._name)
        self._logger.setLevel(logging.INFO)

        if not self._logger.handlers:
            handler = logging.FileHandler(path, encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s %(levelname)s [%(name)s]: %(message)s')
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)

    def get_logger(self):
        return self._logger
