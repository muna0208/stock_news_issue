import configparser
import logging
import urllib.parse

logger = logging.getLogger('config')


def get_oracle_config(file_name):
    cfg = configparser.ConfigParser()
    try:
        cfg.read(file_name)
        config = {
            'ip': cfg['OracleDB']['ip'],
            'port': cfg['OracleDB']['port'],
            'db': cfg['OracleDB']['db'],
            'charset': cfg['OracleDB']['charset'],
            'user': cfg['OracleDB']['user'],
            'password': cfg['OracleDB']['password'],
            'autocommit': cfg['OracleDB']['autocommit'],
        }
    except Exception as e:
        logger.error(f'config {e}')
    return config


def get_clickhouse_config(file_name):
    cfg = configparser.ConfigParser()
    try:
        cfg.read(file_name)
    except Exception as e:
        logger.error(f'config {e}')
    return cfg