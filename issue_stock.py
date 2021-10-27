import os
import sys
import copy
import logging
from datetime import timedelta, datetime

import pandas as pd

from db_client_for_stock_news import DBClientForIssueStock
import json_patch

_SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))


def get_issue_score(stock_name, day=None):
    # day: iso-date format (YYYY-mm-dd)

    if not day:
        day = datetime.today().strftime('%Y-%m-%d')

    db = DBClientForIssueStock()
    rows = db.get_daily_issue_stocks(day)
    # row examples:
    #    {'WRITE_DT': '2020-12-24', 'STOCK': '삼성전자', 'ISSUE': 7.0688}
    #    {'WRITE_DT': '2020-12-24', 'STOCK': '삼성증권', 'ISSUE': 5.0295}
    # 주의할 점: 해당일에 뉴스가 있는 종목들에 대해서만 결과가 존재

    df = pd.DataFrame(rows)
    df.set_index('STOCK', inplace=True)
    # TODO: 뉴스 있는 종목만 나오므로 전체 순위 구하는 것은 수정해야함
    df['total_rank'] = df['ISSUE'].rank(ascending=False)

    # TODO: 코스피/코스닥 필터링 후 market_rank 구하기


    # TODO: market_rank 및 decile 정보 추가하기
    try:
        item = df.loc[stock_name]
        return {
            'name': '이슈분석',
            #'score': (100.0 + item['ISSUE']) / 2, # 점수 정규화
            'score': item['ISSUE'], # 뉴스분석단에서 점수를 이미 정규화함
            'total_rank': round(item['total_rank']),
        }
    except:
        return {
            'name': '이슈분석',
            'score': 50,
            'total_rank': round(len(df)/2), # TODO: market_size/2
        }


def get_issue_stocks(day=None, top_n=3):
    # day: iso-date format (YYYY-mm-dd)

    if not day:
        day = datetime.today().strftime('%Y-%m-%d')

    db = DBClientForIssueStock()
    rows = db.get_daily_issue_stocks(day)

    top_n = sorted(rows, key=lambda x: x['ISSUE'], reverse=True)[:top_n]

    # TODO: stock_code 추가하기
    return [{
        'date': x['WRITE_DT'],
        'stock_name': x['STOCK'],
        #'score': (100.0 + x['ISSUE']) / 2, # 점수 정규화
        'score': item['ISSUE'], # 뉴스분석단에서 점수를 이미 정규화함
    } for x in top_n]


if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

    #stocks = get_issue_stocks('2020-12-01')
    stocks = get_issue_stocks()
    json_patch.print_json(stocks)

    issue_info = get_issue_score('삼성전자')
    json_patch.print_json(issue_info)
