def _load_stockMaster_for_s1(self):
    # stock master 정보를 가져옴
    # 이슈 점수를 조회하기 위해 종목명이 필요한데, 이에 대한 전처리

    # stock master 테이블은 clickhouse의 web_servie_data.stock_master 사용
    # 매칭할 때 Sales Filter 사용 X

    # 전체, 시장별 종목 코드들을 가져옴
    stock_master = self.table_manager['dataframe_db'].select('StockMaster')
    # 기업 이름이 중복인 경우 처리
    # 이슈 점수 테이블에는 종목 코드 없이 종목 명만 있기 때문에, 종목 명에대한 처리 필요
    # stock master의 기업 이름에서 공백 제거하여 매칭
    stock_master['CMP_NM_KOR'] = stock_master['CMP_NM_KOR'].str.replace(' ', '')
    duplicated_stock = stock_master[stock_master.duplicated('CMP_NM_KOR', keep=False)]
    stock_master.drop_duplicates('CMP_NM_KOR', keep=False, inplace=True)
    stock_master = pd.concat([stock_master, duplicated_stock], copy=False)

    return stock_master
