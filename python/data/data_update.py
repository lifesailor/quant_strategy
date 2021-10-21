import sys
import tqdm
import numpy as np
import os
import pandas as pd

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util.logger import Logger

import sqlalchemy as sqla

import pdblp



data_path = os.path.abspath('./excel_data/')
def update_data(df, name, engine, keys=None, delete=True, date=None):
    """
    데이터(dataframe) 업데이트 하는 함수

    parameters
    -------------------
    df : DataFrame 형태의 데이터
    name : update할 대상 table 이름
    engine : update할 대상 table이 있는 데이터베이스 connection
    keys : primary keys. key를 바탕으로 기존 데이터 삭제 type : list이면 해당 key들 모두 참조, str이면 해당 key에 대해서만 참조
            ex) keys = 'tdate' -> 해당 데이터가 존재하는 모든 열들을 삭제후 새로운 데이터로 채워넣음
    delete : True이면 기존 데이터 삭제하고 새로운 데이터로 update
    """

    pbar = tqdm.trange(len(df) // 1000 + 1)
    pbar.set_description(f'append data in {name}')

    if delete:
        if date is not None:
            from_date = pd.to_datetime(str(df[date].sort_values().iloc[0])).strftime('%Y%m%d')
            sql = f"delete from {name} where {date} >= '{from_date}'"
        else:
            if type(keys) == str:
                key_data = np.unique(df[keys])
                key_data = "', '".join(key_data)
                sql = f"delete from {name} where {keys} in ('{key_data}')"

            else:
                s = 0
                for key in keys:
                    key_data = np.unique(df[key])
                    key_data = "', '".join(key_data)
                    try:
                        sql += f" and {key} in ('{key_data}')"
                    except NameError:
                        sql = f"delete from {name} where {key} in ('{key_data}')"

                    s += 1
            # print(sql)
        engine.execute(sql)
    for i in pbar:
        df.iloc[i * 1000:1000 + i * 1000].to_sql(name, engine, index=False, if_exists='append')


def parse_data(excel, sheet_name, td, field_name='PX_LAST'):
    a = excel.parse(sheet_name, skiprows=4)

    commodity = a.iloc[:, :15].set_index("Unnamed: 0").loc[td:]

    em = a.iloc[:, 16:27].set_index("Unnamed: 16").loc[td:]

    dm = a.iloc[:, 28:].set_index("Unnamed: 28").loc[td:]

    alls = [commodity, em, dm]
    field_list = ['fut1return-com', 'fut1return-em', 'FutGenratio1']
    all_data = []
    for origin, data in zip(field_list, alls):
        data = data.stack().to_frame().reset_index()

        data.columns = ['TDATE', 'NAME', 'VALUE']

        data['FIELD'] = field_name
        #         data['ORIGIN'] = origin
        data['TDATE'] = pd.to_datetime(data['TDATE'].values).strftime('%Y%m%d')
        data['TICKER'] = data['NAME'].str.upper()
        data = data[['TDATE', 'TICKER', 'FIELD', 'VALUE']]
        all_data.append(data)
    return all_data


def get_data_from_api(field, info, con, start_date, end_date):
    field_tickers = list(set(info[info['field'] == field]['ticker']))
    # 선물데이터는 따로 엑셀로 처리
    except_tickers = info.set_index('origin').loc[['fut1return-com', 'fut1return-em', 'FutGenratio1']]['ticker']
    tickers = []
    for i in field_tickers:
        if i not in except_tickers.values:
            tickers.append(i)

    data = con.bdh(tickers, field, start_date, end_date)
    data = data.stack().stack().reset_index()
    data.columns = ['TDATE', 'FIELD', 'TICKER', 'VALUE']
    data = data[['TDATE', 'TICKER', 'FIELD', 'VALUE']]
    data['TDATE'] = pd.to_datetime(data['TDATE'].values).strftime('%Y%m%d')
    return data

def carry_update(start_date, end_date, engine, con):
    carry_info = pd.read_sql('select * from carry_info', engine)
    tickers1 = carry_info['ticker1'].to_list()  # 근월물
    tickers2 = carry_info['ticker2'].to_list()  # 차월물

    # 해당 기간의 두가지 ticker 찾기(FUT_CUR_GEN_TICKER)
    first_ticker = con.bdh(list(set(tickers1)), 'FUT_CUR_GEN_TICKER', start_date, end_date)
    first_ticker = first_ticker.T.reset_index().drop('field', 1).set_index('ticker').T
    first_ticker = first_ticker.reindex(columns=tickers1)

    second_ticker = con.bdh(list(set(tickers2)), 'FUT_CUR_GEN_TICKER', start_date, end_date)
    second_ticker = second_ticker.T.reset_index().drop('field', 1).set_index('ticker').T
    second_ticker = second_ticker.reindex(columns=tickers2)

    # 위에서 도출된 모든 TICKER들을 모아서 만기일 구함(LAST_TRADEABLE_DT)
    all_tickers = pd.concat([first_ticker, second_ticker], 1).stack().dropna().reset_index()

    all_tickers[0] = all_tickers.apply(lambda x: "{} {}".format(x[0], x['ticker'].split(' ')[-1]), 1)

    last_trade = con.ref(list(set(all_tickers[0])), 'LAST_TRADEABLE_DT')

    last_trade = last_trade.set_index('ticker').value

    # 두가지 ticker에 대한 PX_LAST구함
    first_price = con.bdh(list(set(tickers1)), 'PX_LAST', start_date, end_date)
    first_price = first_price.T.reset_index().drop('field', 1).set_index('ticker').T
    first_price = first_price.reindex(columns=tickers1)

    second_price = con.bdh(list(set(tickers2)), 'PX_LAST', start_date, end_date)
    second_price = second_price.T.reset_index().drop('field', 1).set_index('ticker').T
    second_price = second_price.reindex(columns=tickers2)

    # 날짜로 pivoting
    first = all_tickers.set_index('ticker').loc[tickers1].reset_index()

    first['ld'] = [last_trade.loc[x] for x in first[0]]

    first['ld'] = pd.to_datetime(first['ld'].values)

    first = first.drop_duplicates().pivot(values='ld', index='date', columns='ticker').reindex(columns=tickers1)

    # 날짜로 pivoting
    second = all_tickers.set_index('ticker').loc[tickers2].reset_index()

    second['ld'] = [last_trade.loc[x] for x in second[0]]

    second['ld'] = pd.to_datetime(second['ld'].values)

    second = second.drop_duplicates().pivot(values='ld', index='date', columns='ticker').reindex(columns=tickers2)

    # 날짜 맞추기
    bdate = pd.date_range(first_price.index[0], first_price.index[-1], freq='B')

    first = first.reindex(bdate)
    second = second.reindex(bdate)

    date_diff = (second.values - first.values).astype('timedelta64[D]')

    date_diff = pd.DataFrame(date_diff.astype(int))

    date_diff = date_diff[date_diff != 0]

    # 날짜 맞춘 가격데이터
    t = first_price.reindex(bdate).ffill()

    v = second_price.reindex(bdate).ffill()
    # 캐리 계산
    carry = pd.DataFrame((t.values - v.values) / v.values / date_diff.values * 10000, index=t.index, columns=v.columns)

    first_ticker_db = first_ticker.reset_index().melt('date').dropna()
    first_ticker_db.columns = ['TDATE', 'TICKER', 'VALUE']
    first_ticker_db['FIELD'] = 'FUT_CUR_GEN_TICKER'
    first_ticker_db = first_ticker_db[['TDATE', 'TICKER', 'FIELD', 'VALUE']]

    second_ticker_db = second_ticker.reset_index().melt('date').dropna()
    second_ticker_db.columns = ['TDATE', 'TICKER', 'VALUE']
    second_ticker_db['FIELD'] = 'FUT_CUR_GEN_TICKER'
    second_ticker_db = second_ticker_db[['TDATE', 'TICKER', 'FIELD', 'VALUE']]

    trade_dt_db = last_trade.reset_index()
    trade_dt_db.columns = ['TICKER', 'VALUE']
    trade_dt_db['FIELD'] = 'LAST_TRADEABLE_DT'
    trade_dt_db = trade_dt_db[['TICKER', 'FIELD', 'VALUE']]
    trade_dt_db['VALUE'] = pd.to_datetime(trade_dt_db['VALUE'].values).strftime('%Y-%m-%d')

    first_price_db = first_price.reset_index().melt('date').dropna()
    first_price_db.columns = ['TDATE', 'TICKER', 'VALUE']
    first_price_db['FIELD'] = 'PX_LAST'
    first_price_db = first_price_db[['TDATE', 'TICKER', 'FIELD', 'VALUE']]

    second_price_db = second_price.reset_index().melt('date').dropna()
    second_price_db.columns = ['TDATE', 'TICKER', 'VALUE']
    second_price_db['FIELD'] = 'PX_LAST'
    second_price_db = second_price_db[['TDATE', 'TICKER', 'FIELD', 'VALUE']]

    ticker_db = first_ticker_db.append(second_ticker_db)

    price_db = first_price_db.append(second_price_db)

    carry.columns = carry_info.set_index('ticker2')['db_ticker'].loc[carry.columns]

    carry_db = carry.reset_index().melt('index').dropna()
    carry_db.columns = ['TDATE', 'TICKER', 'VALUE']
    carry_db['FIELD'] = 'CARRY'
    carry_db = carry_db[['TDATE', 'TICKER', 'FIELD', 'VALUE']]
    carry_db['TDATE'] = pd.to_datetime(carry_db['TDATE'].values).strftime('%Y%m%d')
    price_db['TDATE'] = pd.to_datetime(price_db['TDATE'].values).strftime('%Y%m%d')
    ticker_db['TDATE'] = pd.to_datetime(ticker_db['TDATE'].values).strftime('%Y%m%d')

    update_data(price_db, 'carry_prices', engine, date='TDATE')

    update_data(ticker_db, 'carry_tickers', engine, date='TDATE')

    update_data(trade_dt_db, 'CARRY_LAST_DT', engine, keys=['TICKER', 'FIELD', 'VALUE'])
    return carry_db


class UpdateData:
    def __init__(self, start_date = None, bbg_api_ip = None):
        self.engine = sqla.create_engine("oracle://HAEMA:hippocampus!@roboinvest.cpyxwn3oujsg.ap-northeast-2.rds.amazonaws.com:1521/ORCL")
        if start_date:
            self.start_date = pd.to_datetime(start_date).strftime('%Y%m%d')
        if bbg_api_ip:
            self.bbg_api_ip = bbg_api_ip
        else:
            self.bbg_api_ip = '172.16.128.76'
        self.logger = Logger("UPDATE DATA")

    def get_info(self, source = 'BLOOM'):
        self.logger.info("FETCH INFO FOR {} FROM DATABASE".format(source))
        info = pd.read_sql('select * from INFO_{}'.format(source), self.engine)
        # 데이터 업데이트 해야하는 날짜 구함
        if self.start_date is None:
            td = pd.read_sql('select max(tdate) from grp_{} group by ticker'.format(source), self.engine)

            td = pd.to_datetime(td.min().astype(int).astype(str)[0])

            td = pd.to_datetime(td) - pd.tseries.offsets.BDay(3)
        else:
            td = self.start_date
        # 데이터 종류
        return info, td

    def update_bbg_data(self):
        self.logger.info("START UPDATE BLOOMBERG DATA")
        file = 'grp_bbg_fut_data.xlsx'
        file_path = os.path.join(data_path, file)
        info, td = self.get_info('BLOOM')

        try:
            con = pdblp.BCon(host=self.bbg_api_ip, debug=False, port=8194, timeout=5000)
            con.start()
        except:
            self.logger.info('BLOOMBERG API 접속실패 HOST : {}'.format(self.bbg_api_ip))
            raise TimeoutError

        self.logger.info("BLOOMBERG API 접속성공 HOST : {}".format(self.bbg_api_ip))
        # 현재 업데이트해야하는 데이터
        excel = pd.ExcelFile(file_path)
        # 엑셀에서 선물데이터 뽑기
        sheet_names = excel.sheet_names[::2]
        fields = ['PX_LAST', 'PX_LAST', 'PX_LAST', 'CHG_PCT_1D']
        self.logger.info("선물지수 데이터 업데이트 FROM EXCEL")
        fut_data = []
        for sheet_name, field in zip(sheet_names, fields):
            fut_data += parse_data(excel, sheet_name, td, field)

        fut_data = pd.concat(fut_data).drop_duplicates()

        end_date = fut_data['TDATE'].max()
        # api 로 얻는 데이터
        self.logger.info("데이터 업데이트 FROM API")
        api_px_last_data = get_data_from_api('PX_LAST', info, con,
                                             start_date=td.strftime("%Y%m%d"),
                                             end_date=pd.to_datetime(end_date).strftime('%Y%m%d'))

        api_vol_data = get_data_from_api('HIST CALL IMP VOL', info, con,
                                         start_date=td.strftime("%Y%m%d"),
                                         end_date=pd.to_datetime(end_date).strftime('%Y%m%d'))
        # carry데이터 by api either
        self.logger.info("CARRY 데이터 업데이트 FROM API")
        carry_data = carry_update(start_date=td.strftime("%Y%m%d"),
                                  end_date=pd.to_datetime(end_date).strftime('%Y%m%d'),
                                  engine=self.engine,
                                  con = con)

        bbg_data = pd.concat([fut_data, api_px_last_data, api_vol_data, carry_data])
        update_data(bbg_data, 'GRP_BLOOM', self.engine, keys=['TDATE', 'TICKER', 'FIELD'])

    def update_datastream_data(self):
        self.logger.info("START UPDATE DATASTREAM DATA")
        file = 'grp_datastream_data.xlsm'
        file_path = os.path.join(data_path, file)
        info, td = self.get_info('DS')

        excel = pd.ExcelFile(file_path)

        DM_origin = ['ERR', 'EPS', 'EPS1', 'DPS1', 'DPS']
        EM_origin = ['EPS-em', 'EPS1-em', 'DPS1-em', 'DPS-em']

        DM_fields = info.set_index('origin').loc[DM_origin]['field'].unique()

        EM_fields = info.set_index('origin').loc[EM_origin]['field'].unique()

        info2 = excel.parse('REQUEST_TABLE', skiprows=4)

        info2 = info2[['Series Lookup', 'Datatype/Expressions']].dropna()

        info2.columns = ['tickers', 'field']

        DM = info2['tickers'].unique()[0]
        EM = info2['tickers'].unique()[1]

        DM_columns = [x.split('@:')[-1] for x in DM.split(',')]

        EM_columns = [x.split('@:')[-1] for x in EM.split(',')]

        self.logger.info("DM DATA 업데이트")

        dm = excel.parse('IBES_DM', skiprows=1).dropna(how='all')

        dm = dm.ffill().set_index(['Unnamed: 0', 'Name']).T

        dm_ = dm.T.reset_index().drop("Unnamed: 0", 1).set_index('Name')

        # 보통 필드
        dm_data = {}
        for i in DM_fields:
            data = dm[i]
            data.columns = DM_columns
            dm_data[i] = data
        self.logger.info("EMERGING DATA 업데이트")
        em = excel.parse('IBES_EM', skiprows=1).dropna(how='all')

        em = em.dropna(how='all').ffill().set_index(['Unnamed: 0', 'Name']).T

        # 보통 필드
        em_data = {}
        for i in EM_fields:
            data = em[i].iloc[:, :len(EM_columns)]
            data.columns = EM_columns
            em_data[i] = data
        # dm 데이터 처리
        dm_data = pd.concat(dm_data, 1).loc[td:]

        dm_data = dm_data.stack().stack().reset_index()

        dm_data.columns = ['TDATE', 'TICKER', 'FIELD', 'VALUE']

        dm_data['TDATE'] = pd.to_datetime(dm_data['TDATE'].values).strftime('%Y%m%d')
        # em 데이터 처리
        em_data = pd.concat(em_data, 1).loc[td:]

        em_data = em_data.stack().stack().reset_index()

        em_data.columns = ['TDATE', 'TICKER', 'FIELD', 'VALUE']

        em_data['TDATE'] = pd.to_datetime(em_data['TDATE'].values).strftime('%Y%m%d')

        info_ = excel.parse('Univ')

        price_columns = info_.T.reset_index().T[8].dropna().values

        price_info = pd.DataFrame(price_columns.reshape(-1, 1), columns=['TICKER'])

        dm_grp_tickers = list(info.set_index('ticker').loc[DM_columns]['grp_ticker'].unique())
        em_grp_tickers = list(info.set_index('ticker').loc[EM_columns]['grp_ticker'].unique())
        grp_tickers = dm_grp_tickers + em_grp_tickers

        dm_name = 'priceindex'
        em_name = 'priceindex-em'
        self.logger.info("RAW INDEX 업데이트")
        price_info['GRP_TICKER'] = grp_tickers

        price_info['FIELD'] = 'PI'

        price_info['ORIGIN'] = price_info.apply(lambda x: dm_name if x['GRP_TICKER'] in dm_grp_tickers else em_name, 1)

        price_info.to_csv('D:hedge_new/GRP_DS_PRICE.csv', index=False)

        prices = excel.parse("price")

        prices = prices.iloc[1:]
        prices = prices.set_index('Unnamed: 0')

        prices.columns = price_info['TICKER']

        prices = prices.loc[td:]

        prices.index = pd.to_datetime(prices.index).strftime('%Y%m%d')

        prices = prices.stack().reset_index()

        prices.columns = ['TDATE', 'TICKER', 'VALUE']

        prices['FIELD'] = 'PI'

        prices = prices[['TDATE', 'TICKER', 'FIELD', 'VALUE']]

        ds_data = pd.concat([prices, dm_data, em_data])
        update_data(ds_data, 'GRP_DS', self.engine, keys=['TDATE', 'TICKER', 'FIELD'])

    def update_factset_data(self):
        self.logger.info("START UPDATE FACTSET DATA")

        file = "grp_factset_data.xlsx"

        file_path = os.path.join(data_path, file)

        info, td = self.get_info('FP')

        excel = pd.ExcelFile(file_path)

        price = excel.parse(3, skiprows=1)

        price = price.iloc[1:]

        price.set_index('Unnamed: 0', inplace=True)

        growthvalue = pd.concat([price.loc[:, 'SPG':'SGG'], price.loc[:, 'SPV':'SGV']], 1)

        columns = growthvalue.iloc[0].values

        growthvalue = growthvalue.iloc[1:]

        growthvalue.columns = columns

        growthvalue.index = pd.to_datetime(growthvalue.index)

        growthvalue = growthvalue.loc[td:]

        growthvalue.index = pd.to_datetime(growthvalue.index).strftime('%Y%m%d')
        growthvalue = growthvalue.stack().reset_index()
        growthvalue.columns = ['TDATE', 'TICKER', 'VALUE']

        growthvalue['FIELD'] = 'FG_PRICE'

        ROA = excel.parse(4, skiprows=1)

        ROA = ROA.iloc[1:]

        ROA = ROA.loc[:, :'SG']

        columns = ROA.iloc[0, 1:].values

        ROA = ROA.set_index('Unnamed: 0')
        ROA.columns = columns

        ROA = ROA.reset_index().iloc[1:]

        ROA = ROA.dropna(how='all').set_index('Unnamed: 0').loc[td:]

        ROA.index = pd.to_datetime(ROA.index).strftime('%Y%m%d')

        ROA = ROA.stack().reset_index()
        ROA.columns = ['TDATE', 'TICKER', 'VALUE']

        ROA['FIELD'] = 'FMA_ROA'

        ICR = excel.parse(7, skiprows=1)
        ICR = ICR.iloc[1:]
        ICR = ICR.loc[:, :'SG']

        columns = ICR.iloc[0, 1:].values

        ICR = ICR.set_index('Unnamed: 0')
        ICR.columns = columns

        ICR = ICR.reset_index().iloc[1:]

        ICR = ICR.dropna(how='all').set_index('Unnamed: 0').loc[td:]

        ICR.index = pd.to_datetime(ICR.index).strftime('%Y%m%d')

        ICR = ICR.stack().reset_index()
        ICR.columns = ['TDATE', 'TICKER', 'VALUE']

        ICR['FIELD'] = 'FMA_EBIT_INT_EXP'

        factset_data = pd.concat([growthvalue, ROA, ICR])
        update_data(factset_data, 'GRP_FP', self.engine, keys=['TDATE', 'TICKER', 'FIELD'])

    def update_ceic_data(self):
        self.logger.info("START UPDATE CEIC DATA")

        file = 'grp_ceic_data.xlsx'
        file_path = os.path.join(data_path, file)
        # data stream 데이터 information
        info, td = self.get_info('CEIC')
        # 현재 업데이트해야하는 데이터
        excel = pd.ExcelFile(file_path)

        data = excel.parse('Indc')

        data = data.dropna(how='all').T

        tickers = data.loc['Unnamed: 1']

        data = data.iloc[2:]
        data.columns = tickers

        data = data[tickers.dropna()].dropna(how='all')

        data.index = pd.to_datetime(data.index)

        data = data.resample('BM').last()

        data = data.loc[td:]

        data = data.stack().reset_index()

        data.columns = ['TDATE', 'TICKER', "VALUE"]

        data['FIELD'] = [info.set_index('ticker').loc[x]['field'] for x in data['TICKER']]

        data = data[['TDATE', 'TICKER', 'FIELD', 'VALUE']]

        update_data(data, 'GRP_CEIC', self.engine, keys=['TDATE', 'TICKER', 'FIELD'])


if __name__ == '__main__':
    updater = UpdateData()
    updater.update_bbg_data()
    updater.update_datastream_data()
    updater.update_factset_data()
    updater.update_ceic_data()

    # parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # parser.add_argument('-n', help ='result_file name', default = 'model')
    # parser.add_argument('-s', help='start_date YYYYMMDD, default: end_date - 10 days', default=False,
    #                     required=False)
    # parser.add_argument('-e', help='end_date YYYYMMDD, default: today', default=False,
    #                     required=False)
    # parser.add_argument('-d', help='database to use and update, default : local', default='local')
    # parser.add_argument('-c', help='num of cores, default : 4', default=4)
    #
    # args = vars(parser.parse_args())
    # print(args)
    # # args = {'s': '20190605', 'e': '20191207', 'd': 'local', 'c': 6, 't': 'Q', 'n': 'quality'}
    # # print(args)
    # if args['e']:
    #     end_date = args['e']
    # else:
    #     end_date = pd.datetime.today().strftime('%Y%m%d')
    #
    # if args['s']:
    #     start_date = args['s']
    # else:
    #     start_date = (pd.datetime.today() - pd.tseries.offsets.BDay(3)).strftime('%Y%m%d')
    # #
    # # start_date = '20190924'
    # # end_date = '20200121'
    #
    # con = pdblp.BCon(host='172.16.128.89', debug=False, port=8194, timeout=5000)
    # con.start()
