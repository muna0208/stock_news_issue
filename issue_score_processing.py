import os
import sys
import copy
import logging
from datetime import timedelta, datetime

import pandas as pd

from oracle_client.db_client_for_stock_news import DBClientForIssueStock
from clickhouse_client.clickhouse_reader import ClickHouseReader
from clickhouse_client.clickhouse_writer import ClickHouseWriter
from config import get_clickhouse_config

import json_patch

_SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
logger = logging.getLogger('issue_score_processing')
conf = get_clickhouse_config('./db.config')


def get_issue_score(day=None):
    # day: iso-date format (YYYY-mm-dd)

    if not day:
        day = datetime.today().strftime('%Y-%m-%d')

    # oracle nv_issue_score 정보를 가져옴
    db = DBClientForIssueStock()
    rows = db.get_daily_issue_stocks(day)

    df = pd.DataFrame(rows)
    logger.debug(df)
    return df


def get_stock_master():
    reader = ClickHouseReader(host=conf['ClickHouse']['host'], 
                              database=conf['ClickHouse']['db_web_service_data'], 
                              user=conf['ClickHouse']['user'], 
                              password=conf['ClickHouse']['password'])

    # clickhouse stock_master 최근 date 정보를 가져옴
    df = reader.read_financial(table_name='stock_master', cond=" WHERE date=(SELECT date FROM web_service_data.stock_master ORDER BY date desc limit 1)")
    logger.debug(df)

    return df


def match_issue_score(issue_score_df, stock_master_df):
    # issue_scoer_df 전처리 - 띄어쓰기 제거
    isd = issue_score_df.copy()

    # 테스트 코드
    # isd = isd.append({'WRITE_DT':'2021-10-20', 'STOCK':'동화약품', 'ISSUE':'13.1313'}, ignore_index=True)
    # isd = isd.append({'WRITE_DT':'2021-10-20', 'STOCK':'우리은행', 'ISSUE':'14.1414'}, ignore_index=True)
    # isd = isd.append({'WRITE_DT':'2021-10-20', 'STOCK':'KR모터스', 'ISSUE':'12.1212'}, ignore_index=True)

    isd['NAME'] = isd['STOCK'].str.replace(" ","")
    isd.set_index('NAME', inplace=True)

    # stock_master 전처리 - 띄어쓰기 제거
    smd = stock_master_df[['CMP_NM_KOR','CMP_CD','analysis_filter','date']].copy()
    smd['NAME'] = smd['CMP_NM_KOR'].str.replace(" ","")
    smd.set_index('NAME', inplace=True)

    # issue_scoer_df, stock_master Join
    join_df = isd.join(smd)
    logger.debug('### join_df ###')
    logger.debug(join_df)

    # CMP_CD 결측값 체크
    nan_cnt = join_df['CMP_CD'].isnull().sum()
    if nan_cnt > 0:
        logger.debug(f'결측값 {nan_cnt}개 발생')
        join_df = check_nan(isd, smd, join_df)
    else:
        logger.debug(f'결측값 없음')

    # analysis_filter값이 1인것만 추출
    join_df = join_df.loc[join_df['analysis_filter'] == '1']
    result = join_df[['WRITE_DT', 'STOCK', 'ISSUE', 'CMP_CD']].copy()
    logger.debug('### result ###')
    logger.debug(result)

    return result


def check_nan(isd, smd, join_df):
    logger.debug('### check_nan ###')
    logger.debug(join_df.loc[join_df['CMP_CD'].isnull()])

    nan_stock_list = join_df.loc[join_df['CMP_CD'].isnull(),'STOCK']

    reader = ClickHouseReader(host=conf['ClickHouse']['host'], 
                              database=conf['ClickHouse']['db_web_service_data'], 
                              user=conf['ClickHouse']['user'], 
                              password=conf['ClickHouse']['password'])
    
    for stock in nan_stock_list:
        df = reader.read_financial(table_name='stock_master', cond=f" WHERE CMP_NM_KOR == '{stock}' ORDER BY date desc LIMIT 1")
        if not df.empty:
            join_df.loc[join_df['STOCK']==stock, 'CMP_CD'] = df['CMP_CD'][0]
            join_df.loc[join_df['STOCK']==stock, 'analysis_filter'] = df['analysis_filter'][0]


    nan_cnt = join_df['CMP_CD'].isnull().sum()
    if nan_cnt > 0:
        logger.debug('#############################################################################################################################')
        logger.debug(f'예외 결측값 {nan_cnt}개 발생')
        logger.debug(join_df.loc[join_df['CMP_CD'].isnull()])
        logger.debug('#############################################################################################################################')
    else:
        logger.debug(f'예외 결측값 없음')
    
    return join_df


def write_clickhouse(issue_score_match):
    try:
        writer = ClickHouseWriter(host=conf['ClickHouse']['host'], 
                                    database=conf['ClickHouse']['db_somemoney_data'], 
                                    user=conf['ClickHouse']['user'], 
                                    password=conf['ClickHouse']['password'])

        writer.write_clickhouse('issue_score', issue_score_match, chunksize=eval(conf['ClickHouse']['chunksize']))
        logger.debug("완료!!")

    except Exception as e:
        logger.error(f'error {e}')
    
    return 





if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

    issue_score_df = get_issue_score()
    stock_master_df = get_stock_master()

    issue_score_match = match_issue_score(issue_score_df, stock_master_df)
    write_clickhouse(issue_score_match)