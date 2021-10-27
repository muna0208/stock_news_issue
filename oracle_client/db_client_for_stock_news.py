import os
import sys
import copy
import logging
from datetime import timedelta, datetime

from oracle_client.db_client import MariaDBClient, OracleClient
from config import get_oracle_config

_SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

ISSUE_STOCK_TABLE = 'nv_issue_score'
logger = logging.getLogger('oracle client')


class DBClientForIssueStock(OracleClient):
    '''뉴스기반 종목별 이슈점수 DB(Oracle DB)용 클라이언트'''
    
    def __init__(self, max_retry=3, batch=False, autocommit=True):
        config = get_oracle_config('./db.config')

        if not autocommit:
            config['autocommit'] = False
        super(DBClientForIssueStock, self).__init__(config, max_retry)

    # for nnd module
    def get_daily_issue_stocks(self, day):
        # day: iso-date format (YYYY-mm-dd)
        query = """select * from %s 
                   where WRITE_DT = '%s' 
                """ % (ISSUE_STOCK_TABLE, day)
        rows = self.execute(query)
        logger.debug(query)
        return list(rows)
    
    # for nnd module
    def get_issue_stocks_by_date(self, start_day, end_day):
        # start_day, end_day: iso-date format (YYYY-mm-dd)
        query = """select * from %s 
                   where WRITE_DT >= '%s'
                   and WRITE_DT <= '%s'
                """ % (ISSUE_STOCK_TABLE, start_day, end_day)
        rows = self.execute(query)
        return list(rows)


if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

    db = DBClientForIssueStock()

    rows = db.get_daily_issue_stocks('2021-08-31')
    #rows = db.get_issue_stocks_by_date('2020-12-21', '2020-12-24')

    for row in rows:
        print(row)
