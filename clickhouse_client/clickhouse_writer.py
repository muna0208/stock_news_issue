from clickhouse_client.clickhouse_connection import ClickHouseConnection
from clickhouse_client.clickhouse_schema import ClickHouseSchema
import pandas as pd
import logging
import os
import re
import gc
from config import get_clickhouse_config
import sys
from logging.handlers import RotatingFileHandler


logger = logging.getLogger('clickhouse_writer')


class ClickHouseWriter:
    def __init__(self, **argv):
        self.connection = ClickHouseConnection(**argv)
        self.client = self.connection.get_client() 
        self.database = None
        if 'database' in argv:
            self.database = argv['database']
        else:
            logger.error(f'database error {self.database}')
        self.schema = ClickHouseSchema(**argv)


    def write_clickhouse(self, table_name, data, chunksize=10000):
        try:
            self.schema.get_columns(table_name)
            field_types = self.schema.get_schema(table_name)
        except Exception as e:
            # 테이블 생성
            logger.debug(f'get_columns 에러: {e}')
            self.schema.create_table(table_name)
            self.schema.get_columns(table_name)
            field_types = self.schema.get_schema(table_name)

        
        names = []
        for row in field_types:
            names += [row[0]]

        
        try:
            logger.debug(f"INSERT INTO {table_name} ({','.join(names)}) VALUES")
            for idx in range(0, data.shape[0], chunksize): 
                n = self.client.insert_dataframe(f"INSERT INTO {table_name} VALUES", data.iloc[idx:idx+chunksize, :][names])
                logger.debug(f'insert rows: {n}')
                if n == 0:
                    logger.error(f'0 rows written: {table_name}')
        except Exception as e:
            logger.error(f'write clickhouse Error: {e}')
    

