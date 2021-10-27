from clickhouse_client.clickhouse_connection import ClickHouseConnection
from clickhouse_client.clickhouse_schema import ClickHouseSchema
import pandas as pd
import logging
import os
import gc
from config import get_clickhouse_config
import sys
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('clickhouse_reader')


class ClickHouseReader:
    def __init__(self, **argv):
        self.connection = ClickHouseConnection(**argv)
        self.client = self.connection.get_client() 
        self.database = None
        if 'database' in argv:
            self.database = argv['database']
        else:
            logger.error(f'database error {self.database}')
        self.schema = ClickHouseSchema(database=self.database)

    def read_financial(self, table_name, cond):
        df = self.client.query_dataframe(f'SELECT * FROM {table_name} {cond}')
        return df
    
