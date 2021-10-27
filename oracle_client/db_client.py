#!/usr/bin/env python3
# -*- coding: utf8 -*-

import os
import sys
import time
import logging
import re
from datetime import datetime

import pymysql
import cx_Oracle

import json_patch


class DBClient(object):
    '''MariaDBClient 및 OracleDBClient 의 부모 class'''

    def __init__(self, db_conf, max_retry=3):
        self.config = db_conf
        self.max_retry = max_retry
        self.conn = None
        self._connect()

    def __del__(self):
        self._close()

    def _connect(self):
        raise Exception('_connect() method not implemented')

    def _close(self):
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None

    def _reconnect(self):
        self._close()
        self._connect()

    # select, show 등 데이터를 받아오는 쿼리를 처리하는 함수
    def _get(self, query):
        raise Exception('_get() method not implemented')

    # insert, update, delete 등 데이터를 받아올 필요가 없는 쿼리를 처리하는 함수
    def _set(self, query):
        raise Exception('_set() method not implemented')

    # query 실행 함수 (밖으로 보이는 유일한 함수)
    def execute(self, query):
        # query의 첫번째 명령어
        q_cmd = query.strip().split()[0].lower()

        if q_cmd in ('select', 'show'):
            return self._get(query)
        elif q_cmd in ('insert', 'update', 'delete', 'merge', 'rename'):
            return self._set(query)
        else:
            logging.warn('[execute] unknown type query ::: '+query)
            return self._get(query)

    # clob 데이터가 포함된 쿼리를 만들때, 문자열이 너무 길어 발생하는 오류를 피하기 위한 함수
    def str_to_clob(self, text):
        text = text.replace("'", "`")
        word_list = re.split('\s', text)
        # 쿼리내의 문자열이 너무 길지 않도록 50단어씩 끊어서 처리
        conv_words_to_clob = lambda i: "to_clob('%s')" % ' '.join(word_list[i:i+50])
        chunk_list = map(conv_words_to_clob, range(0, len(word_list), 50))
        return ' || '.join(chunk_list)

    # scrapy item 인스턴스(혹은 dict)로부터 insert query를 생성해주는 유틸리티 함수
    def make_insert_query(self, item, table_name, clob_fields=None):
        def conv_value2str(k, v):
            if clob_fields and k in clob_fields:
                return self.str_to_clob(v)
            return self.value2str(v)

        keys = sorted(item.keys())
        values = map(lambda k: conv_value2str(k, item[k]), keys)
        return 'INSERT INTO %s ( %s ) VALUES ( %s )' % (table_name, ', '.join(keys), ', '.join(values))

    # 변수 타입에 따라 sql 쿼리의 문자열 값으로 변환해주는 유틸리티 함수
    def value2str(self, v):
        if v is None:
            return "NULL"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, str):
            return "'%s'" % (v.replace("'", "`"))
        # JJKIM : python >= 3, NameError: name 'unicode' is not defined
        # if isinstance(v, unicode):
        #    return "'%s'" % (v.encode("utf8").replace("'", "`"))
        if isinstance(v, datetime):
            return datetime.strftime(v, "'%Y-%m-%d %H:%M:%S'")
        else:
            raise Exception('[make_insert_query] unknown value type ::: %s, %s' % (str(type(v)), str(v)))


class MariaDBClient(DBClient):
    '''Maria DB 용 클라이언트'''

    # for new connection
    def _connect(self):
        self.conn = None
        try:
            self.conn = pymysql.connect(**self.config)
        except:
            logging.error('db connection failed ::: ' + json_patch.dump_json(self.config))
            raise Exception('db connection failed')

    # for select, show, ...
    def _get(self, query):
        for retry in range(self.max_retry):
            with self.conn.cursor(pymysql.cursors.DictCursor) as cur:
                try:
                    cur.execute(query)
                    return cur.fetchall()
                except pymysql.err.OperationalError as e:
                    # reconnect and try again
                    logging.warn('[OperationalError] ' + str(e))
                except Exception as e:
                    # no retry
                    logging.error('[SelectError] ' + str(e))
                    return []

            # reconnect and try again
            self._reconnect()

        # when failed up to max_retry
        logging.error('too many retry during select')
        return []

    # for insert, update, ...
    def _set(self, query):
        for retry in range(self.max_retry):
            with self.conn.cursor() as cur:
                try:
                    cur.execute(query)
                    return True
                except pymysql.err.OperationalError as e:
                    # reconnect and try again
                    logging.warn('[OperationalError] ' + str(e))
                except pymysql.err.IntegrityError as e:
                    # no retry
                    logging.warn('[IntegrityError] ' + str(e))
                    return False
                except Exception as e:
                    # no retry
                    logging.error('[ExecuteError] ' + str(e))
                    return False

            # reconnect and try again
            self._reconnect()

        # when failed up to max_retry
        logging.error('too many retry during execute')
        return False
    
    # update 쿼리
    def make_merge_query(self, item, table_name, key_fields, clob_fields=None):
        def conv_value2str(k, v):
            if clob_fields and k in clob_fields:
                return self.str_to_clob(v)
            return self.value2str(v)

        update_fields = map(lambda x: x not in key_fields, sorted(item.keys()))
        query = self.make_insert_query(item, table_name, clob_fields)
        query += ' on duplicate key update '
        query += ', '.join(map(lambda x: x+' = '+conv_value2str(x, item[x]), update_fields))
        return query


class OracleClient(DBClient):
    '''Oracle DB 용 클라이언트'''

    # for new connection
    def _connect(self):
        self.conn = None
        os.putenv('NLS_LANG', 'AMERICAN_AMERICA.AL32UTF8')
        dsn_tns = cx_Oracle.makedsn(self.config['ip'], self.config['port'], self.config['db'])
        for retry in range(self.max_retry):
            try:
                self.conn = cx_Oracle.connect(self.config['user'], self.config['password'], dsn_tns, threaded = True)
                break
            except Exception as e:
                try:
                    dsn_tns = cx_Oracle.makedsn(self.config['ip'], self.config['port'], service_name=self.config['db'])
                    self.conn = cx_Oracle.connect(self.config['user'], self.config['password'], dsn_tns, threaded = True)
                    break
                except:
                    logging.error('db connection failed ::: ' + json_patch.dump_json(self.config))
                    logging.error(str(e))
                    raise Exception('db connection failed')
            time.sleep(600)

    def _kv_to_dict(self, cols, row):
        output = {}
        for k, v in zip(cols, row):
            if isinstance(v, (type(cx_Oracle.LOB), type(cx_Oracle.CLOB))):
                output[k] = v.read()
            else:
                output[k] = v
        return output

    # for select, show, ...
    def _get(self, query):
        for retry in range(self.max_retry):
            try:
                cur = self.conn.cursor()
                cur.execute(query)
                rows = cur.fetchall()
                desc = cur.description
                results = map(lambda row: self._kv_to_dict(map(lambda x: x[0], desc), row), rows) 
                #cols = map(lambda x: x[0], cur.description)
                cur.close()
                return results
                #return map(lambda row: self._kv_to_dict(cols, row), rows)
                #results = map(lambda row: self._kv_to_dict(map(lambda x: x[0], cur.description), row), rows)
                #cols = map(lambda x: x[0], cur.description)
                #####
                #json_patch.print_json(cols)
                #json_patch.print_json(rows)
                #####
                #results = map(lambda row: self._kv_to_dict(map(lambda x: x[0], cur.description), row), rows)
                #cur.close()
                #return results
                #return map(lambda row: dict(zip(cols, row)), rows)
                #return map(lambda row: self._kv_to_dict(cols, row), rows)
            # TODO: 재시도 해야할 오류 타입 파악
            #except cx_Oracle.DatabaseError as e:
            #    # reconnect and try again
            #    logging.warn('[DatabaseError] ' + str(e))
            #    cur.close()
            #    self._reconnect()
            except Exception as e:
                # no retry
                logging.error('[SelectError] ' + str(e))
                logging.error(query)
                cur.close()
                return []

        # when failed up to max_retry
        logging.error('too many retry during select')
        return []

    # for insert, update, ...
    def _set(self, query):
        for retry in range(self.max_retry):
            try:
                cur = self.conn.cursor()
                query = query.replace('`','\'\'')
                cur.execute(query)
                cur.close()
                self.conn.commit()
                return True
            # TODO: 재시도 해야할 오류 타입 파악
            #except cx_Oracle.DatabaseError as e:
            #    # reconnect and try again
            #    logging.warn('[DatabaseError] ' + str(e))
            #    cur.close()
            #    self._reconnect()
            except Exception as e:
                # no retry
                logging.error('[ExecuteError] ' + str(e))
                logging.error(query)
                cur.close()
                return False

        # when failed up to max_retry
        logging.error('too many retry during execute')
        return False
    
    # update 쿼리
    def make_merge_query(self, item, table_name, key_fields, clob_fields=None):
        def conv_value2str(k, v):
            if clob_fields and k in clob_fields:
                return self.str_to_clob(v)
            return self.value2str(v)

        update_fields = list(filter(lambda x: x not in key_fields, sorted(item.keys())))
        key_values = map(lambda x: self.value2str(item[x]), key_fields)
        update_values = map(lambda x: conv_value2str(x, item[x]), update_fields)
        query = '''
            merge into %s using dual
            on ( %s )
            when matched then update set %s
        ''' % (
            table_name,
            ' and '.join(map(lambda x: '%s = %s' % x, zip(key_fields, key_values))),
            ', '.join(map(lambda x: '%s = %s' % x, zip(update_fields, update_values))),
        )
        return query

    # update 쿼리
    def make_merge_query_bak(self, item, table_name, key_fields, clob_fields=None):
        def conv_value2str(k, v):
            if clob_fields and k in clob_fields:
                return self.str_to_clob(v)
            return self.value2str(v)

        update_fields = list(filter(lambda x: x not in key_fields, sorted(item.keys())))
        key_values = map(lambda x: self.value2str(item[x]), key_fields)
        update_values = map(lambda x: conv_value2str(x, item[x]), update_fields)
        query = '''
            merge into %s using dual
            on ( %s )
            when matched then update set %s
            when not matched then insert ( %s ) values ( %s )
        ''' % (
            table_name,
            ' and '.join(map(lambda x: '%s = %s' % x, zip(key_fields, key_values))),
            ', '.join(map(lambda x: '%s = %s' % x, zip(update_fields, update_values))),
            ', '.join(key_fields+update_fields),
            ', '.join(list(key_values)+list(update_values)),
        )
        return query

def _test_mariadb_client():

    news_db_config = {
        'host': '10.1.51.31',  # news_db (MariaDB)
        'port': 3306,
        'db': 'test',
        'charset': 'utf8',
        'user': 'news',
        'passwd': 'news2016',
        'autocommit': True,
    }

    db_client = MariaDBClient(news_db_config)

    """
    query = '''insert into test.crawl_news_list (source_nm, sid1_code, press_nm, title, article_link, article_status, sid1_name, reg_date, sid2_code, create_date, article_id, sid2_name, updt_date) values ("naver", 102, "대전일보", "당론밀려 의원소신 뒷전(上－정당 민주화)", "http://news.naver.com/main/read.nhn?mode=LPOD&mid=sec&oid=089&aid=0000002067", "R", "사회", "2016-04-27 11:21:08", 256, "1990-01-01 10:55:00", "199001010890000002067", "지역", "2016-04-27 11:21:08")'''
    print '#', query
    rv = db_client.execute(query)
    print rv

    query = '''select * from crawl_news_list limit 10'''
    #query = '''select * from news_dev.crawl_news_list limit 10'''
    #query = '''select * from news_dev.crawl_reply_info where create_date >= "2018-11-13" limit 10'''
    print '#', query
    rows = db_client.execute(query)
    json_patch.print_json(rows)

    query = '''delete from test.crawl_news_list where article_id = "199001010890000002067"'''
    print '#', query
    rv = db_client.execute(query)
    print rv
    """

    query = '''select * from crawl_news_list limit 10'''
    print('#', query)
    rows = db_client.execute(query)
    json_patch.print_json(rows)


def _test_oracle_client():

    dhsvc_db_config = {
        'ip': '10.1.51.32',  # dhsvc_db (Oracle DB)
        'port': 1521,
        'db': 'ASPDB2',
        'charset': 'utf8',
        'user': 'DHSVC',
        'password': 'DHSVC2018',
    }
    
    tagged_db_config = {
        'ip': '10.1.51.33',  # tagged news db (Oracle DB)
        'port': 1521,
        'db': 'aspdb3',
        'charset': 'utf8',
        'user': 'stock',
        'password': 'stock2018',
    }

    db_client = OracleClient(tagged_db_config)
    #query = '''select * from nv_news_item_map'''
    query = '''update nv_news_item_map_backup_test set DUP_ARTICLE_ID_CONTENTS = '201906030110003564512',
    SIM_SCORE_CONTENTS = 0.481 where ARTICLE_ID = '201906030180004394163' and STOCK_ITEM = '기아차' '''
    rows = db_client.execute(query)
    #json_patch.print_json(rows)
    for i, r in enumerate(rows):
        if i == 5:
            break
        print(r)
    """
    query = '''insert into xx_test_brand (CATEGORY_ID, BRAND_ID, BRAND_NM, BRAND_COLOR, BRAND_CI_IMG, MAIN_YN, FAMILY_YN, USE_YN, DESCR, RGST_DT, RGSTR_ID, UPD_DT, UPDR_ID) values ('CA00010', 'BR00005', '하이트', null, null, 'N', 'N', 'Y', '하이트', '20181114', null, '20181114', null)'''
    print '#', query
    rv = db_client.execute(query)
    print rv

    query = '''select * from xx_test_brand where rownum <= 10'''
    #query = '''select * from dhsvc.xx_test_brand where xxx = 1 and rownum <= 10'''
    print '#', query
    rows = db_client.execute(query)
    json_patch.print_json(rows)

    query = '''delete from xx_test_brand where brand_nm = '하이트' '''
    print '#', query
    rv = db_client.execute(query)
    print rv

    query = '''select * from xx_test_brand where rownum <= 10'''
    print '#', query
    rows = db_client.execute(query)
    json_patch.print_json(rows)
    """


if __name__ == "__main__":
  
    os.putenv('NLS_LANG', '.UTF8') 
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.DEBUG)

    #_test_mariadb_client()
    _test_oracle_client()
