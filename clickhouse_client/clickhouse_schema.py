import clickhouse_driver
from clickhouse_sqlalchemy import Table, make_session, get_declarative_base, types, engines
import pandas as pd
import datetime
import csv
import re
import logging
from config import get_clickhouse_config



logger = logging.getLogger('clickhouse schema')

class ClickHouseSchema:
    def __init__(self,
                 host='',
                 user='default',
                 password='',
                 database='',
                 settings={'use_numpy': True}
                 ):
        self.database = database
        self.client = clickhouse_driver.Client(host=host, database=database, settings=settings)


    def create_table(self, table_name):
        query = f"DROP TABLE IF EXISTS {table_name}"
        self.client.execute(query)

        if 'issue_score' == table_name:
            query = f'''
                CREATE TABLE issue_score
                (
                    WRITE_DT String,
                    STOCK String,
                    ISSUE Float64,
                    CMP_CD String
                )
                ENGINE = ReplacingMergeTree()
                PRIMARY KEY STOCK
                ORDER BY STOCK
                SETTINGS index_granularity = 8192;
            '''

        logger.info(f'{query}')
        self.client.execute(query)


    def get_columns(self, table_name):
        query = 'SELECT * FROM {} LIMIT 0'.format(table_name)
        _, cols = self.client.execute(query, with_column_types=True)
        return [x[0] for x in cols]

    def get_schema(self, table_name):
        return self.client.execute('DESC {}'.format(table_name))


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)
    schema = ClickHouseSchema(host='10.1.42.7', 
                            database='somemoney_data', 
                            user='default', 
                            password='')
    schema.create_table('issue_score')

