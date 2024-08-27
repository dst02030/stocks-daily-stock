import logging
import os
import ta
import numpy as np
import pandas as pd

from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler

from datetime import datetime, timedelta, date
from src.utils import get_jinja_yaml_conf, create_db_engine, Clickhouse_client, Postgres_connect
from tqdm.auto import tqdm

def main():
    os.chdir(os.path.dirname(__file__))
    conf = get_jinja_yaml_conf('./conf/logging.yml', './conf/data.yml')
    tqdm.pandas()
    today = datetime.now().date()

    # logger 설정
    stream = logging.StreamHandler()
    # stream.setLevel(logging.DEBUG)
    logger = logging.getLogger('main')
    logging.basicConfig(level=eval(conf['logging']['level']),
        format=conf['logging']['format'],
        handlers = [TimedRotatingFileHandler(filename =  conf['logging']['file_name'],
                                    when=conf['logging']['when'],
                                    interval=conf['logging']['interval'],
                                    backupCount=conf['logging']['backupCount']), 
                                    stream]
                    )

    # DB 설정
    engine = create_db_engine(os.environ)
    postgres_conn = Postgres_connect(engine)
    click_conn = Clickhouse_client(user_name = os.environ['CLICK_USER'], password = os.environ['CLICK_PW'])
    
    # full_save 모드 확인 및 DB 최신 날짜 가져오기
    full_save = True if click_conn.get_count('stocks', 'daily_stock') == 0 else os.environ['full_save'].lower() == 'true'

    logger.info(f'save mode is: {full_save}')


    # 마켓 지표 가져오기
    return_cols = ['수익률'] + [f'수익률{day}일' for day in conf['agg_days']]
    market_indicator = click_conn.get_table('stocks', 'daily_market', 
                            columns = ['기준일자', '지수명', '시가총액', '거래대금'] + return_cols,
                              where = ["`지수명` IN ('코스피', '코스닥')"],
                              orderby_cols = ['기준일자', '계열구분', '지수명']
                                           ).rename(columns = {col: f'시장{col}' for col in return_cols}).rename(columns = {'지수명': '시장구분', '시가총액': '시장시가총액', '거래대금': '시장거래대금'})
    
    market_indicator['기준일자'] = market_indicator['기준일자'].dt.date
    market_indicator['시장구분'] = market_indicator['시장구분'].map(lambda x: 'KOSPI' if x == '코스피' else 'KOSDAQ')


    market_dates = market_indicator['기준일자'].sort_values().unique()

    # 처리할 날짜 설정
    latest_stock_date = pd.to_datetime(
                            click_conn.get_maxmin_col(conf['daily_stock']['database'], conf['daily_stock']['table'], 
                                column = '기준일자', is_min = False)[0]
        ).date()
    
    upload_date = market_dates[0] if full_save else latest_stock_date + timedelta(days = 1)

    if upload_date > market_dates[-1]:
        logger.info("Latest stock information is uploaded already.")
        return 
    
    start_idx = np.where(market_dates >= upload_date)[0][0] - max(conf['agg_days'])
    start_date = market_dates[0] if start_idx < 0 else market_dates[start_idx] 
    end_date = market_dates[-1]


    
    
    # 회사/주가정보 가져오기
    stock_info = postgres_conn.get_data(conf['sto_info']['database'], conf['sto_info']['table'], 
                          columns = conf['sto_info']['columns'],
                          where = [
                                  "증권구분 = '주권'", 
                                   "주식종류 = '보통주'", 
                                   "액면가 != '무액면'",
                                   "시장구분 IN ('KOSPI', 'KOSDAQ')",
                                   f"상장일 <= '{end_date}'",
                                  f"기준일자 >= '{start_date}'"],
                            orderby_cols = ['기준일자'])
    
    stock_price = postgres_conn.get_data(conf['sto_stocks']['database'], conf['sto_stocks']['table'], 
                          columns = conf['sto_stocks']['columns'],
                          where = [f"종목코드 IN ('{"','".join(stock_info['종목코드'].unique())}')",
                                  f"기준일자 <= '{end_date}'",
                                  f"기준일자 >= '{start_date}'"])
    
    stock_price['시총순위'] = stock_price.groupby('기준일자')['시가총액'].rank()
    stock_price['시총순위백분율'] = stock_price.groupby('기준일자')['시가총액'].rank(pct = True)
    stock_price['거래대금순위'] = stock_price.groupby('기준일자')['거래대금'].rank()
    stock_price['거래대금순위백분율'] = stock_price.groupby('기준일자')['거래대금'].rank(pct = True)
    
    # full_save = False일 때
    if not full_save:
        face_value_history = click_conn.get_table(conf['daily_stock']['database'], conf['daily_stock']['table'],
                                                columns = ['종목코드', '수정액면가'],
                                                orderby_cols = ['기준일자 DESC'],
                                                post_sql = ' LIMIT 1 BY `종목코드`')
        
        last_face_value = stock_info.drop_duplicates('종목코드', keep = 'last')[['종목코드', '액면가']]
        face_value_history['종목코드'] = face_value_history['종목코드'].map(lambda x: x[1:])
        merge_face_value = last_face_value.merge(face_value_history, how = 'left').astype({'액면가': 'Int64', '수정액면가': 'Int64'})
        full_save_idx = (merge_face_value['액면가'] != merge_face_value['수정액면가']) | (merge_face_value['수정액면가'].isnull())
        full_save_stocks = merge_face_value.loc[full_save_idx, '종목코드'].tolist()
        """
        full_save가 필요한 주식 조건
        1. 업로드 대상 테이블에 주가 정보가 없을 때
        2. 주식의 최근 액면가가 변경됐을 때
        """ 
        if len(full_save_stocks) > 0:
            stock_clause = f"""
                                ('{"', '".join([code for code in full_save_stocks])}')
                                """
            
            full_stock_info = postgres_conn.get_data(conf['sto_info']['database'], conf['sto_info']['table'], 
                              columns = conf['sto_info']['columns'],
                              where = ["증권구분 = '주권'", 
                                       "주식종류 = '보통주'", 
                                       "액면가 != '무액면'",
                                       "시장구분 IN ('KOSPI', 'KOSDAQ')",
                                      f"단축코드 IN {stock_clause}"],
                                orderby_cols = ['기준일자'])
        
            full_stock_price = postgres_conn.get_data(conf['sto_stocks']['database'], conf['sto_stocks']['table'], 
                          columns = conf['sto_stocks']['columns'],
                          where = [f"종목코드 IN {stock_clause}",
                                  f"기준일자 < '{upload_date}'"])

            full_stock_history = click_conn.get_table(conf['daily_stock']['database'], conf['daily_stock']['table'],
                        columns = ['기준일자', '종목코드', '시총순위', '시총순위백분율', '거래대금순위', '거래대금순위백분율'],
                        where = [f"RIGHT(`종목코드`, 6) IN {stock_clause}",
                                f"`기준일자` < '{upload_date}'"])
    
            full_stock_price = full_stock_price.merge(full_stock_history, how = 'inner')
        
            stock_info = pd.concat([stock_info, full_stock_info]).drop_duplicates(keep = 'last')
            stock_price = pd.concat([stock_price, full_stock_price]).drop_duplicates(keep = 'last')
    
    
    # 회사/주가 정보 합치기
    merge_info = market_indicator.merge(stock_info, on = ['기준일자', '시장구분'], how = 'inner')
    merge_info = merge_info.merge(stock_price, on = ['기준일자', '종목코드'], how = 'left').rename(columns={'등락률': '수익률', '대비': '전일대비'})
    merge_info.sort_values(['종목코드', '기준일자'], inplace = True)
    merge_info = merge_info.merge(merge_info.groupby('종목코드')['액면가'].last().reset_index().rename(columns = {'액면가': '수정액면가'}), on = '종목코드', how = 'left')

    # 전체 시장 정보
    total_mkt_amount = market_indicator.groupby('기준일자')[['시장시가총액', '시장거래대금']].sum().rename(columns = {'시장시가총액': '전체시가총액', '시장거래대금': '전체거래대금'}).reset_index()
    merge_info = merge_info.merge(total_mkt_amount, how = 'left')
    
    # 최근 회사 액면가에 맞춰 수정칼럼 추가
    val_chg =  merge_info['수정액면가'].astype('Int64') / merge_info['액면가'].astype('Int64')
    merge_info['수정시가'] = merge_info['시가'] * val_chg
    merge_info['수정고가'] = merge_info['고가'] * val_chg
    merge_info['수정저가'] = merge_info['저가'] * val_chg
    merge_info['수정종가'] = merge_info['종가'] * val_chg
    merge_info['수정전일대비'] = merge_info['전일대비'] * val_chg
    merge_info['수정거래량'] = merge_info['거래량'] / val_chg
    merge_info['수정상장주식수'] = merge_info['상장주식수'] / val_chg
    merge_info["시장대비_수익률"] = merge_info["수익률"] - merge_info["시장수익률"]
    merge_info["거래대금시장시총비율"] = merge_info["거래대금"] / merge_info["시장시가총액"] * 100
    merge_info["거래대금시장거래대금비율"] = merge_info["거래대금"] / merge_info["시장거래대금"] * 100
    merge_info["거래대금전체시총비율"] = merge_info["거래대금"] / merge_info["전체시가총액"] * 100
    merge_info["거래대금전체거래대금비율"] = merge_info["거래대금"] / merge_info["전체거래대금"] * 100
    merge_info['상장경과일'] = merge_info['상장일'].map(lambda x: (today - x).days)
        
    
    # 이평선 관련
    group_info = merge_info.groupby('종목코드')
    for day in conf['agg_days']:
        logger.info(f"aggregating process of {day}days starts!")
        ## 종가
        # 이평
        merge_info[f'종가_이평{day}일'] = group_info['수정종가'].rolling(window=day).mean().reset_index(level = 0).iloc[:, -1]
        # 괴리율
        merge_info[f'종가_이평{day}일_괴리율'] = merge_info['수정종가'] / merge_info[f'종가_이평{day}일'] * 100
    
        ## 거래량
        # 이평
        merge_info[f'거래량_이평{day}일'] = group_info['수정거래량'].rolling(window=day).mean().reset_index(level = 0).iloc[:, -1]
        # 합
        merge_info[f'거래량_합{day}일'] = group_info['수정거래량'].rolling(window=day).sum().reset_index(level = 0).iloc[:, -1]
        
        ## 시총이평
        merge_info[f'시총_이평{day}일'] = group_info['시가총액'].rolling(window=day).mean().reset_index(level = 0).iloc[:, -1]
        # 거래대금이평
        merge_info[f'거래대금_이평{day}일'] = group_info['거래대금'].rolling(window=day).mean().reset_index(level = 0).iloc[:, -1]
        # 거래대금 합
        merge_info[f'거래대금_합{day}일'] = group_info['거래대금'].rolling(window=day).sum().reset_index(level = 0).iloc[:, -1]
        
        ## 수익률
        # 이평
        merge_info[f"수익률{day}일"] = group_info['수익률'].rolling(day).progress_apply(lambda x: ((1+x/100).prod() - 1) * 100, raw = True).reset_index(level = 0).iloc[:, -1]
        # 시장대비
        merge_info[f"시장대비_수익률{day}일"] = merge_info[f"수익률{day}일"] - merge_info[f"시장수익률{day}일"]
        # 변동성
        merge_info[f'수익률_변동성{day}일'] = group_info['수익률'].rolling(window=day).std().reset_index(level = 0).iloc[:, -1]
        # 이평 연율화
        merge_info[f'수익률{day}일_연율화'] = ((1 + merge_info[f"수익률{day}일"] / 100) ** (240 / 720) - 1) * 100
        # 변동성 연율화
        merge_info[f'수익률_변동성{day}일_연율화'] = merge_info[f'수익률_변동성{day}일'] / np.sqrt(240)
        # sr 연율화
        merge_info[f'SR_{day}일_연율화'] =  merge_info[f'수익률{day}일_연율화'] / merge_info[f'수익률_변동성{day}일_연율화']
    
    # n일 최고/최저가
    for day in conf['high_low_days']:
        merge_info[f'최고가{day}일'] = group_info['수정고가'].rolling(window=day).max().reset_index(level = 0).iloc[:, -1]
        merge_info[f'최저가{day}일'] = group_info['수정저가'].rolling(window=day).min().reset_index(level = 0).iloc[:, -1]
        # 괴리율
        merge_info[f'최고가{day}일_괴리율'] = merge_info['수정종가'] / merge_info[f'최고가{day}일'] * 100
        merge_info[f'최저가{day}일_괴리율'] = merge_info['수정종가'] / merge_info[f'최저가{day}일'] * 100
    
    
    # 거래량
    merge_info[f'거래량1일_증가율'] = merge_info.apply(lambda x: np.nan if x['거래량_이평20일'] == 0 else x['수정거래량'] / x['거래량_이평20일'] * 100, axis = 1)
    merge_info[f'거래량5일_증가율'] = merge_info.apply(lambda x: np.nan if x['거래량_이평20일'] == 0 else x['거래량_이평5일'] / x['거래량_이평20일'] * 100, axis = 1)
    
    # 거래대금시총비율
    merge_info[f'거래대금시총비율_1일'] = merge_info[f'거래대금'] / merge_info[f'시가총액'] * 100
    merge_info[f'거래대금시총비율_5일'] = merge_info[f'거래대금_이평5일'] / merge_info[f'시총_이평5일'] * 100

    # 거래대금증가율
    merge_info['전일거래대금'] = group_info['거래대금'].shift(1)
    merge_info[f'거래대금_전일대비_증가율'] = merge_info.apply(lambda x: np.nan if x['전일거래대금'] == 0 else x['거래대금'] / x['전일거래대금'] * 100, axis = 1)
    merge_info[f'거래대금_5일이평대비_증가율'] = merge_info.apply(lambda x: np.nan if x['거래대금_이평5일'] == 0 else x['거래대금'] / x['거래대금_이평5일'] * 100, axis = 1)

    
    
    # 기술적 지표 생성
    merge_info['MACD'] = group_info.progress_apply(lambda x: ta.trend.macd(close = x['수정종가'], window_slow = 26, window_fast = 12)).reset_index(level = 0).iloc[:, -1]
    merge_info['MACD_signal'] = group_info.progress_apply(lambda x: ta.trend.macd_signal(close = x['수정종가'], window_slow = 26, window_fast = 12, window_sign = 9)).reset_index(level = 0).iloc[:, -1]
    merge_info['MACD_diff_signal'] = merge_info['MACD'] - merge_info['MACD_signal']
    merge_info['RSI'] = group_info.progress_apply(lambda x: ta.momentum.rsi(close = x['수정종가'], window = 14)).reset_index(level = 0).iloc[:, -1]
    
    # Stochastic
    merge_info['fastK'] = group_info.progress_apply(lambda x: ta.momentum.stoch(high = x['수정고가'], low = x['수정저가'], close = x['수정종가'], window=14, smooth_window=1)).reset_index(level = 0).iloc[:, -1]
    merge_info['fastD'] = merge_info.groupby('종목코드')['fastK'].rolling(window=3).mean().reset_index(level = 0).iloc[:, -1]
    merge_info['slowK'] = merge_info['fastD'].copy()
    merge_info['slowD'] = merge_info.groupby('종목코드')['slowK'].rolling(window=3).mean().reset_index(level = 0).iloc[:, -1]
    
    
    # 볼린저밴드
    merge_info['mavg'] = group_info.progress_apply(lambda x: ta.volatility.bollinger_mavg(close = x['수정종가'], window = 20)).reset_index(level = 0).iloc[:, -1]
    merge_info['up'] = group_info.progress_apply(lambda x: ta.volatility.bollinger_hband(close = x['수정종가'], window = 20, window_dev = 2)).reset_index(level = 0).iloc[:, -1]
    merge_info['dn'] = group_info.progress_apply(lambda x: ta.volatility.bollinger_lband(close = x['수정종가'], window = 20, window_dev = 2)).reset_index(level = 0).iloc[:, -1]
    
    # 골든 데드
    for cross_name, cross_cols in conf['tech_signal'].items():
        merge_info[cross_name] = 0
        left = merge_info.groupby('종목코드')[cross_cols[0]]
        right = merge_info.groupby('종목코드')[cross_cols[1]]
        merge_info.loc[(left.shift(1) <= right.shift(1)) & (left.shift(0) > right.shift(0)), cross_name] = 1 
        merge_info.loc[(left.shift(1) >= right.shift(1)) & (left.shift(0) < right.shift(0)), cross_name] = -1
        merge_info.loc[merge_info[cross_cols[0]].isnull() | merge_info[cross_cols[1]].isnull(), cross_name] = np.nan
    
    merge_info['Bband_Cross'] = 0
    left = merge_info.groupby('종목코드')['수정종가']
    right = merge_info.groupby('종목코드')['up']
    merge_info.loc[(left.shift(1) <= right.shift(1)) & (left.shift(0) > right.shift(0)) & (merge_info['up'] > merge_info['mavg'] * 1.15), 'Bband_Cross'] = 1
    right = merge_info.groupby('종목코드')['dn']
    merge_info.loc[(left.shift(1) >= right.shift(1)) & (left.shift(0) < right.shift(0)) & (merge_info['dn'] > merge_info['mavg'] * 1.15), 'Bband_Cross'] = -1
    merge_info.loc[merge_info['up'].isnull() | merge_info['dn'].isnull() | merge_info['mavg'].isnull(), 'Bband_Cross'] = np.nan

        
    
    
    # 저장할 데이터 기간 filter
    if not full_save:
        merge_info = pd.concat([merge_info.loc[merge_info['종목코드'].isin(full_save_stocks), :], 
                                merge_info[merge_info['기준일자'] >= upload_date]]).drop_duplicates(keep = 'first')

     # 그외 전처리
    merge_info['종목코드'] = 'A' + merge_info['종목코드']
    merge_info['_ts'] = os.environ['_ts']
    
    # 데이터 업로드
    click_conn.df_insert(merge_info, conf['daily_stock']['database'], conf['daily_stock']['table'])
    
        
if __name__ == "__main__":
    main()