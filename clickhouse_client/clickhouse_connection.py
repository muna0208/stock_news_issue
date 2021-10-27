import clickhouse_driver
import os
import pandas as pd
import logging

logger = logging.getLogger('clickhouse connection')


class ClickHouseConnection:
    def __init__(self,
                 host='',
                 user='default',
                 password='',
                 database='',
                 settings={'use_numpy': True}
                 ):
        self.database = database
        self.client = None
        try:
            self.client = clickhouse_driver.Client(host=host, user=user, password=password, database=database, settings=settings)
        except Exception as e:
            logger.error(f'{e}')

    def get_client(self):
        return self.client