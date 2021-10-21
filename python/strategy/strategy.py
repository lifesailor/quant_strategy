import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import tqdm

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from util.logger import Logger
from util.folder import Folder

base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
database_path = os.path.join(data_path, 'database')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')
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

class Strategy:
    GRP_DB_ADDRESS = "oracle://HAEMA:hippocampus!@roboinvest.cpyxwn3oujsg.ap-northeast-2.rds.amazonaws.com:1521/ORCL"

    # QUERY
    PAST_QUERY = "SELECT * FROM GRP_PAST"

    def __init__(self, strategy_name, asset_type):
        self.strategy_name = strategy_name
        self.asset_type = asset_type

        # logger
        self.logger = Logger.set_logger(strategy_name)
        self.logger.info('[STEP 0] START LOGGING ' + strategy_name.upper())

        # folder
        Folder.make_folder(base_path)
        Folder.make_folder(data_path)
        Folder.make_folder(database_path)
        Folder.make_folder(result_path)

        # 1. data
        self.engine = None
        self.raw_index = None
        self.ret = None
        self.index = None

        self.raw_index_recent = None
        self.ret_recent = None
        self.index_recent = None

        # 2. rebalance period
        self.ts_freq = None
        self.cs_freq = None
        self.cs_monitor = 1
        self.cs_ex = 0

        self.rebalance_weekday = None

        # 3. signal
        self.CSRV = None
        self.TSRV = None

        # 4. parameter
        self.cs_strategy_type = None
        self.assetvol = None
        self.strategyvol = None
        self.factorvol = None
        self.factorsd = None
        self.assetsd = None
        self.statsd = None
        self.min_vol = None
        self.volband = None

        # 5. porfolio
        self.std = None
        self.adjusted_std = None

        self.TS_position = None
        self.CS_position = None
        
        # 6. backtest result
        self.TS_summary = None
        self.TS_result = None
        self.TS_result_tr = None
        self.TS_port = None

        self.CS_summary = None
        self.CS_result = None
        self.CS_result_tr = None
        self.CS_port = None
    def load_portfolio(self):
        self.TS_position = pd.read_sql("SELECT * FROM GRP_TS_POSITION where strategy = '{}'".format(self.strategy_name.upper()), self.engine)
        self.CS_position = pd.read_sql("SELECT * FROM GRP_CS_POSITION where strategy = '{}'".format(self.strategy_name.upper()), self.engine)
        self.TS_position = self.TS_position.pivot('tdate', 'asset', 'position')
        self.CS_position = self.CS_position.pivot('tdate', 'asset', 'position')
        self.TS_position.index = pd.to_datetime(self.TS_position.index)
        self.CS_position.index = pd.to_datetime(self.CS_position.index)

        self.TS_position.index.name = 'tdate'
        self.CS_position.index.name = 'tdate'

    def _connect_database(self):
        self.engine = create_engine(Strategy.GRP_DB_ADDRESS)

    def _load_strategy_data(self, table='datastream', origin='EPS'):
        if self.engine is None:
            self._connect_database()

        if table == 'datastream' or table == 'ds' or table == 'DS':
            query = "select * from grp_ds a inner join (select * from info_ds where origin = :origin) b on UPPER(a.ticker) = UPPER(b.ticker) and a.field = b.field order by tdate asc"
            param = {'origin': origin}
            ds = self.engine.execute(query, param)
            rows = [row for row in ds]
            columns = ds.keys()
            df = pd.DataFrame(rows, columns=columns)

        if table == 'factset' or table == 'fs' or table == "FS" or table == 'fp' or table == 'FP':
            query = "select * from grp_fp a inner join (select * from info_fp where origin = :origin) b on UPPER(a.ticker) = UPPER(b.ticker) and a.field = b.field order by tdate asc"
            param = {'origin': origin}
            fs = self.engine.execute(query, param)
            rows = [row for row in fs]
            columns = fs.keys()
            df = pd.DataFrame(rows, columns=columns)

        if table == 'bloomberg' or table == 'bloom' or table == "BB":
            query = "select * from grp_bloom a inner join (select * from info_bloom where origin = :origin) b on UPPER(a.ticker) = UPPER(b.ticker) and a.field = b.field order by tdate asc"
            param = {'origin': origin}
            bb = self.engine.execute(query, param)
            rows = [row for row in bb]
            columns = bb.keys()
            df = pd.DataFrame(rows, columns=columns)

        if table == 'CEIC':
            query = "select * from grp_ceic a inner join (select * from info_ceic where origin = :origin) b on UPPER(a.ticker) = UPPER(b.ticker) and a.field = b.field order by tdate asc"
            param = {'origin': origin}
            cc = self.engine.execute(query, param)
            rows = [row for row in cc]
            columns = cc.keys()
            df = pd.DataFrame(rows, columns=columns)

        df = df[['tdate', 'grp_ticker', 'value']]
        df.columns = ['tdate', 'ticker', 'value']
        end_day = df['tdate'].max()
        df = df[df['tdate'] < end_day]
        df.drop_duplicates(inplace=True)
        df_pivot = df.pivot(index='tdate', columns='ticker', values='value')
        df_pivot.index = pd.to_datetime(df_pivot.index)
        df_pivot = df_pivot.astype(np.float32)
        df_pivot.fillna(inplace=True, method='ffill')
        return df_pivot

    def set_rebalance_period(self, ts_freq='week', cs_freq='week', rebalance_weekday=1):
        self.logger.info('[STEP 2] SET REBALANCE PERIOD')
        self.ts_freq = ts_freq
        self.cs_freq = cs_freq
        self.rebalance_weekday = rebalance_weekday

    def set_portfolio_parameter(self,
                                cs_strategy_type='vol',
                                cs_monitor=1,
                                cs_ex=0,
                                assetvol=0.02, strategyvol=0.02, factorvol=0.02, factorsd=260, assetsd=90,
                                statsd=90, min_vol=0.15, volband=0.05):
        self.logger.info('[STEP 4] SET PORTFOLIO PARAMETER')
        self.cs_ex = cs_ex
        self.cs_strategy_type = cs_strategy_type
        self.cs_monitor = cs_monitor
        self.assetvol = assetvol
        self.strategyvol = strategyvol
        self.factorvol = factorvol
        self.factorsd = factorsd
        self.assetsd = assetsd
        self.statsd = statsd
        self.min_vol = min_vol
        self.volband = volband

    def make_portfolio(self, save_db=False, save_file=True):
        """
        factor=function(TSRV,CSRV,Ret,TSweek=FALSE, CSLS="notional",TSWGT=1,CSWGT=1,ex=0,CSweek=1,monitor=1,IR=0,RB1=RBP, rpname=name1,BETA=betamat)

        :param save_db:
        :param save_file:
        :return:
        """
        self.logger.info('[STEP 5] MAKE PORTFOLIO')

        self.logger.info('[STEP 5 - 1] CALCULATE VOLATILITY')
        self.std = (self.ret.rolling(window=self.assetsd).std() * np.sqrt(260)).iloc[self.assetsd - 1:]
        self.adjusted_std = self._adjust_vol_by_volband()

        self.logger.info('[STEP 5 - 2] MAKE TS POSITION')
        self.TSRV.index.name = 'tdate'
        self._make_ts_position()

        self.logger.info('[STEP 5 - 3] MAKE CS POSITION')
        self.CSRV.index.name = "tdate"
        self._make_cs_position()

        self.logger.info('[STEP 5 - 4] SAVE POSITION')
        self.TS_position.index.name = 'tdate'
        self.CS_position.index.name = 'tdate'

        if save_db:
            ts_td = pd.read_sql(
                "SELECT max(tdate) FROM GRP_TS_POSITION WHERE strategy = '{}'".format(self.strategy_name.upper()), self.engine)
            ts_td = pd.to_datetime(ts_td.values[0][0]) - pd.tseries.offsets.BDay(10)
            ts_data = self._convert_data_for_db(self.TS_position[ts_td:])

            cs_td = pd.read_sql(
                "SELECT max(tdate) FROM GRP_CS_POSITION WHERE strategy = '{}'".format(self.strategy_name.upper()), self.engine)
            cs_td = pd.to_datetime(cs_td.values[0][0]) - pd.tseries.offsets.BDay(10)
            cs_data = self._convert_data_for_db(self.CS_position[cs_td:])

            update_data(ts_data, 'GRP_TS_POSITION', self.engine, keys = ["TDATE", "STRATEGY", "ASSET"])
            update_data(cs_data, 'GRP_CS_POSITION', self.engine, keys=["TDATE", "STRATEGY", "ASSET"])

        if save_file:
            self.TS_position.to_csv(os.path.join(result_path, self.strategy_name + "_ts_position.csv"))
            self.CS_position.to_csv(os.path.join(result_path, self.strategy_name + "_cs_position.csv"))

    def _convert_data_for_db(self, position_data):


        position_data.index = pd.to_datetime(position_data.index).strftime('%Y%m%d')

        position_data = position_data.stack().reset_index()

        position_data.columns = ['TDATE', 'ASSET', 'POSITION']

        position_data['STRATEGY'] = self.strategy_name

        position_data = position_data[['TDATE', 'STRATEGY', 'ASSET', 'POSITION']]

        return position_data


    def _adjust_vol_by_volband(self):
        adjusted_std = self.std.copy()

        for i in range(1, self.std.shape[0]):
            vol_exceed_asset_count = 0

            for c in range(len(adjusted_std.columns)):
                if not pd.isnull(self.std.iloc[i, c]):
                    if pd.isnull(adjusted_std.iloc[i - 1, c]):
                        adjusted_std.iloc[i - 1, c] = self.std.iloc[i, c]

                    if abs(self.std.iloc[i, c] - adjusted_std.iloc[i - 1, c]) > self.volband * adjusted_std.iloc[i - 1, c]:
                        vol_exceed_asset_count = vol_exceed_asset_count + 1

            for c in range(len(self.std.columns)):
                if vol_exceed_asset_count > 0:
                    for c in range(len(self.std.columns)):
                        adjusted_std.iloc[i, c] = self.std.iloc[i, c]
                else:
                    adjusted_std.iloc[i, c] = adjusted_std.iloc[i - 1, c]

        if self.min_vol is not None:
            adjusted_std[adjusted_std < self.min_vol] = self.min_vol
        return adjusted_std

    def _make_ts_position(self):
        self.logger.info('[STEP 5 - 2 - 1] ALIGN TS POSITION WITH REBALANCE DAY')
        self._align_and_adjust_ts_position()

        self.logger.info('[STEP 5 - 2 - 2] TARGET VOL CONTROL to TS POSITION')
        VCweight = self.assetvol / self.adjusted_std        # Target vol index for Time Series signal
        VCTSpos = (VCweight * self.TSRV).loc[self.TSRV.index[0]:]
        VCTSpos.replace([np.inf, -np.inf], np.nan, inplace=True)    # Vol Control Time Series Position

        self.logger.info('[STEP 5 - 2 - 3] STRATEGY LEVEL VOL CONTROL to TS POSITION')
        strategy = ((self.ret * VCTSpos.shift(1)).sum(axis=1)).loc[self.TSRV.index[0]:]
        strategyrisk_expanding = (strategy.expanding(min_periods=self.statsd).std() * np.sqrt(260)).iloc[self.statsd:]
        strategyrisk_rolling = (strategy.rolling(window=self.statsd).std() * np.sqrt(260)).iloc[self.statsd:]
        strategyrisk = (strategyrisk_expanding + strategyrisk_rolling) / 2.
        bufferrisk = self._adjust_ts_risk_by_strategy_volband(strategyrisk)
        statlev = self.strategyvol / bufferrisk.iloc[:, 0]

        self.logger.info('[STEP 5 - 2 - 4] MAKE FINAL TS POSITION')
        self.TS_position = VCTSpos.multiply(statlev, axis='index').iloc[self.statsd:]
        self.TS_position.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.TS_position.fillna(0, inplace=True)

    def _make_cs_position(self):
        if self.cs_strategy_type == "vol":
            self.logger.info('[STEP 5 - 3 - 1] TARGET VOL CONTROL to CS POSITION')
            VCweight = self.assetvol / self.adjusted_std
            VCCSpos = (self.CSRV * VCweight).loc[self.CSRV.index[0]:]
            self.CSRV = VCCSpos

            self.logger.info('[STEP 5 - 3 - 2] ALIGN CS POSITION WITH REBALANCE DAY')
            self._align_and_adjust_cs_position()

        elif self.cs_strategy_type == 'notional':
            self.logger.info('[STEP 5 - 3 - 1] NO TARGET VOL CONTROL to CS POSITION')
            self.CSRV = self.CSRV

            self.logger.info('[STEP 5 - 3 - 2] ALIGN CS POSITION WITH REBALANCE DAY')
            self._align_and_adjust_cs_position()
        else:
            pass

        self.logger.info('[STEP 5 - 3 - 3] STRATEGY LEVEL VOL CONTROL to CS POSITION')
        Strategy = (self.ret * self.CSRV.shift(1)).dropna(how='all').sum(axis=1)

        if self.cs_ex == 1:
            Strategyrisk = (strategy.expanding(min_periods=self.statsd).std() * np.sqrt(260)).iloc[self.statsd:]
        else:
            Strategyrisk = (Strategy.rolling(window=self.statsd).std() * np.sqrt(260)).iloc[self.statsd:]
        buffer_risk = self._adjust_cs_risk_by_strategy_volband(Strategyrisk)
        statlev = self.strategyvol / buffer_risk.iloc[:, 0]

        self.logger.info('[STEP 5 - 3 - 4] MAKE FINAL CS POSITION')
        self.CS_position = self.CSRV.multiply(statlev, axis='index').iloc[self.statsd:]
        self.CS_position.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.CS_position.fillna(0, inplace=True)

    def _align_and_adjust_ts_position(self):
        if self.ts_freq == 'week':
            for i in (range(1, self.TSRV.shape[0])):
                if self.TSRV.iloc[i].name.weekday() == self.rebalance_weekday:
                    pass
                else:
                    self.TSRV.iloc[i] = self.TSRV.iloc[i - 1]
        elif self.ts_freq == 'month':
            month_end = []
            for i in range(1, self.TSRV.shape[0]):
                if self.TSRV.iloc[i].name.month != self.TSRV.iloc[i - 1].name.month:
                    month_end.append(i - 1)

            # month end rebalancing
            for i in range(1, self.TSRV.shape[0]):
                if i in month_end:
                    pass
                else:
                    self.TSRV.iloc[i] = self.TSRV.iloc[i - 1]
        else:
            pass

    def _align_and_adjust_cs_position(self):
        if self.cs_freq == 'week':
            for i in range(1, self.CSRV.shape[0]):
                if self.CSRV.iloc[i].name.weekday() == self.rebalance_weekday:
                    self.CSRV.iloc[i] = self.CSRV.iloc[i] * self.strategyvol / \
                                        self._calculate_ex_ante_vol(to_date=self.CSRV.iloc[i].name,
                                                                    weights=self.CSRV.iloc[i])
                else:
                    self.CSRV.iloc[i] = self.CSRV.iloc[i - 1]
        elif self.cs_freq == 'month':
            month_end = []
            for i in range(1, self.CSRV.shape[0]):
                if self.CSRV.iloc[i].name.month != self.CSRV.iloc[i - 1].name.month:
                    month_end.append(i - 1)

            for i in range(1, self.CSRV.shape[0]):
                if i in month_end:
                    self.CSRV.iloc[i] = self.CSRV.iloc[i] * self.strategyvol / \
                                        self._calculate_ex_ante_vol(to_date=self.CSRV.iloc[i].name,
                                                                    weights=self.CSRV.iloc[i])
                else:
                    self.CSRV.iloc[i] = self.CSRV.iloc[i - 1]
        else:
            pass

    def _calculate_ex_ante_vol(self, to_date, weights, lookback=130, shrink_corr=0.9):
        to_date_loc = self.ret.index.get_loc(to_date)
        if to_date_loc > lookback:
            from_date_loc = to_date_loc - lookback
        else:
            from_date_loc = 0

        corr = self.ret.iloc[from_date_loc:to_date_loc].corr()  # calculate corr apply shrinkage
        corr = corr * shrink_corr + np.diag(np.ones(len(self.ret.columns))) * (1. - shrink_corr)
        std = self.ret.iloc[from_date_loc:to_date_loc].std()
        cov = pd.DataFrame(np.diag(std).dot(corr).dot(np.diag(std)), index=self.ret.columns, columns=self.ret.columns)
        return np.sqrt(weights.dot(cov).dot(weights)) * np.sqrt(260)

    def _adjust_ts_risk_by_strategy_volband(self, risk):
        adjusted_risk = risk.copy().to_frame()

        for i in range(1, adjusted_risk.shape[0]):
            if abs(adjusted_risk.iloc[i, 0] - adjusted_risk.iloc[i - 1, 0]) > self.volband * self.strategyvol:
                pass
            else:
                adjusted_risk.iloc[i, 0] = adjusted_risk.iloc[i - 1, 0]
        return adjusted_risk

    def _adjust_cs_risk_by_strategy_volband(self, risk):
        adjusted_risk = risk.copy().to_frame()

        if self.cs_monitor == 1:
            for i in range(1, adjusted_risk.shape[0]):
                if (abs(adjusted_risk.iloc[i, 0] - adjusted_risk.iloc[i - 1, 0]) > self.volband * self.strategyvol) and (adjusted_risk.iloc[i].name.weekday() == self.rebalance_weekday):
                    pass
                else:
                    adjusted_risk.iloc[i, 0] = adjusted_risk.iloc[i - 1, 0]
        else:
            month_end = []
            for i in range(1,  adjusted_risk.shape[0]):
                if adjusted_risk.iloc[i].name.month != adjusted_risk.iloc[i - 1].name.month:
                    month_end.append(i - 1)

            for i in range(1, adjusted_risk.shape[0]):
                if (abs(adjusted_risk.iloc[i, 0] - adjusted_risk.iloc[i - 1, 0]) > self.volband * self.strategyvol) and i in month_end:
                    pass
                else:
                    adjusted_risk.iloc[i, 0] = adjusted_risk.iloc[i - 1, 0]
        return adjusted_risk

    def rename_columns(self, data, column_dictionary):
        new_column_dictionary = {}
        for key, value in column_dictionary.items():
            new_column_dictionary[key.upper()] = value.upper()
        try:
            data.index = pd.to_datetime(data.index, format='%Y%m%d')
        except:
            data.index = pd.to_datetime(data.index, format='%Y-%m-%d')
        data.rename(columns=new_column_dictionary, inplace=True)
        data.index = pd.to_datetime(data.index, format='%Y%m%d')
        data = data.astype(np.float32)
        return data


class CommodityStrategy(Strategy):
    BLOOM_COMMODITY_COLUMNS_RECENT = {
        'GC1 Comdty': 'GC',
        'CL1 Comdty': 'CL',
        'NG1 Comdty': 'NG',
        'HG1 Comdty': 'HG',
        'C 1 Comdty': 'C',
        'S 1 Comdty': 'S',
        'SI1 Comdty': 'SI',
        'SB1 Comdty': 'SB',
        'XBW1 Comdty': 'XBW',
        'SM1 Comdty': 'SM',
        'BO1 Comdty': 'BO',
        'W 1 Comdty': 'W',
        'KC1 Comdty': 'KC',
        'CT1 Comdty': 'CT'}
    BLOOM_COMMODITY_COLUMNS_NEXT = {
        'GC2 Comdty': 'GC',
        'CL2 Comdty': 'CL',
        'NG2 Comdty': 'NG',
        'HG2 Comdty': 'HG',
        'C 2 Comdty': 'C',
        'S 2 Comdty': 'S',
        'SI2 Comdty': 'SI',
        'SB2 Comdty': 'SB',
        'XBW2 Comdty': 'XBW',
        'SM2 Comdty': 'SM',
        'BO2 Comdty': 'BO',
        'W 2 Comdty': 'W',
        'KC2 Comdty': 'KC',
        'CT2 Comdty': 'CT'}
    BLOOM_COMMODITY_COLUMNS = {
        'GC1 n:04_0_n comdty': 'GC',
        'CL1 r:04_0_n comdty': 'CL',
        'NG1 r:04_0_n comdty': 'NG',
        'HG1 n:04_0_n comdty': 'HG',
        'C 1 n:04_0_n comdty': 'C',
        'S 1 n:04_0_n comdty': 'S',
        'SI1 n:04_0_n comdty': 'SI',
        'SB1 r:04_0_n comdty': 'SB',
        'XBW1 r:04_0_n comdty': 'XBW',
        'SM1 n:04_0_n comdty': 'SM',
        'BO1 n:04_0_n comdty': 'BO',
        'W 1 n:04_0_n comdty': 'W',
        'KC1 n:04_0_n comdty': 'KC',
        'CT1 n:04_0_n comdty': 'CT'}
    BLOOM_COMMODITY_RETURN_COLUMNS = {
        'GC1 n:04_0_r comdty': 'GC',
        'CL1 r:04_0_r comdty': 'CL',
        'NG1 r:04_0_r comdty': 'NG',
        'HG1 n:04_0_r comdty': 'HG',
        'C 1 n:04_0_r comdty': 'C',
        'S 1 n:04_0_r comdty': 'S',
        'SI1 n:04_0_r comdty': 'SI',
        'SB1 r:04_0_r comdty': 'SB',
        'XBW1 r:04_0_r comdty': 'XBW',
        'SM1 n:04_0_r comdty': 'SM',
        'BO1 n:04_0_r comdty': 'BO',
        'W 1 n:04_0_r comdty': 'W',
        'KC1 n:04_0_r comdty': 'KC',
        'CT1 n:04_0_r comdty': 'CT'}
    BLOOM_COMMODITY_WEATHER_GROUP = ['C', 'S', 'SB', 'SM', 'W', 'KC', 'CT']
    BLOOM_COMMODITY_NOTWEATHER_GROUP = ['GC', 'CL', 'NG', 'HG', 'SI', 'XBW', 'BO']

    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_index_and_return(self, from_db=True, save_file=True):
        self.logger.info("[STEP 1] LOAD DATA")

        if from_db:
            self.logger.info("[STEP 1 - 1] CONNECT TO BLOOM DATABASE")
            self._connect_database()

            self.logger.info("[STEP 1 - 2] GET DATA FROM BLOOM DATABASE")
            commdity_columns = list(CommodityStrategy.BLOOM_COMMODITY_COLUMNS.keys())
            commdity_columns = [column.upper() for column in commdity_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(commdity_columns))
            bloom = self.engine.execute(query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom = pd.DataFrame(rows, columns=columns)

            df_bloom_pivot = df_bloom.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index = self.rename_columns(df_bloom_pivot, CommodityStrategy.BLOOM_COMMODITY_COLUMNS)
            self.ret = self.raw_index.pct_change(periods=1).iloc[1:]

            self.logger.info("[STEP 1 - 3] CHANGE BLOOM DATA INTO STANDARD FORMAT")
            commdity_columns = list(CommodityStrategy.BLOOM_COMMODITY_COLUMNS_RECENT.keys())
            commdity_columns = [column.upper() for column in commdity_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(commdity_columns))
            bloom = self.engine.execute(query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom_recent = pd.DataFrame(rows, columns=columns)

            df_bloom_pivot = df_bloom_recent.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index_recent = self.rename_columns(df_bloom_pivot, CommodityStrategy.BLOOM_COMMODITY_COLUMNS_RECENT)
            self.ret_recent = self.raw_index_recent.pct_change(periods=1).iloc[1:]

            self.logger.info("[STEP 1 - 3] CHANGE BLOOM DATA INTO STANDARD FORMAT")
            commdity_columns = list(CommodityStrategy.BLOOM_COMMODITY_COLUMNS_NEXT.keys())
            commdity_columns = [column.upper() for column in commdity_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(commdity_columns))
            bloom = self.engine.execute(query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom_next = pd.DataFrame(rows, columns=columns)

            df_bloom_pivot = df_bloom_next.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index_next = self.rename_columns(df_bloom_pivot, CommodityStrategy.BLOOM_COMMODITY_COLUMNS_NEXT)
            self.ret_next = self.raw_index_next.pct_change(periods=1).iloc[1:]

            for i in range(1, self.raw_index.shape[0] - 10):
                for j, column in enumerate(self.raw_index.columns):
                    tday_front_diff = abs(
                        self.raw_index_recent.loc[self.raw_index.index[i], column] - self.raw_index.loc[self.raw_index.index[i], column])
                    tday_next_diff = abs(
                        self.raw_index_next.loc[self.raw_index.index[i], column] - self.raw_index.loc[self.raw_index.index[i], column])
                    yday_front_diff = abs(
                        self.raw_index_recent.loc[self.raw_index.index[i-1], column] - self.raw_index.loc[self.raw_index.index[i-1], column])
                    yday_next_diff = abs(
                        self.raw_index_next.loc[self.raw_index.index[i-1], column] - self.raw_index.loc[self.raw_index.index[i-1], column])

                    if (tday_next_diff < tday_front_diff) & (yday_next_diff >= yday_front_diff):
                        self.ret.loc[self.raw_index.index[i], column] = self.ret_next.loc[self.raw_index.index[i], column]
                    else:
                        self.ret.loc[self.raw_index.index[i], column] = self.ret.loc[self.raw_index.index[i], column]

            # self.logger.info("[STEP 1 - 2] GET DATA FROM BLOOM DATABASE")
            # commdity_columns = list(CommodityStrategy.BLOOM_COMMODITY_RETURN_COLUMNS.keys())
            # commdity_columns = [column.upper() for column in commdity_columns]
            # query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {}".format(tuple(commdity_columns))
            # bloom = self.engine.execute(query)
            # rows = [row for row in bloom]
            # columns = bloom.keys()
            # df_bloom = pd.DataFrame(rows, columns=columns)
            #
            # df_bloom_pivot = df_bloom.pivot(index='tdate', columns='ticker', values='value')
            # df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            # self.ret = self.rename_columns(df_bloom_pivot, CommodityStrategy.BLOOM_COMMODITY_RETURN_COLUMNS) / 100
            self.index = (1 + self.ret).cumprod()

            if save_file:
                self.logger.info("[STEP 1 - 4] SAVE RETURN DATA INTO CSV FORMAT")
                self.raw_index.to_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA.csv"))
                self.raw_index_recent.to_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA_RECENT.csv"))
                self.ret.to_csv(os.path.join(database_path, self.asset_type + ".csv"))
        else:
            self.logger.info("[STEP 1 - 1] LOAD DATA FROM FILE")
            self.raw_index = pd.read_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA.csv"),
                                         header=0,
                                         index_col=0,
                                         parse_dates=True)
            self.raw_index_recent = pd.read_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA_RECENT.csv"),
                                                header=0,
                                                index_col=0,
                                                parse_dates=True)
            self.ret = pd.read_csv(os.path.join(database_path, self.asset_type + ".csv"),
                                   header=0,
                                   index_col=0,
                                   parse_dates=True)
            self.index = (1. + self.ret).cumprod()
        self.raw_index.columns.name = 'ticker'
        self.index.columns.name = 'ticker'
        self.ret.columns.name = 'ticker'
        self.raw_index.index.name = 'tdate'
        self.index.index.name = 'tdate'
        self.ret.index.name = 'tdate'


class EquityStrategy(Strategy):
    BLOOM_EQUITY_COLUMNS_NEXT = {
        'ES2 Index': 'SPX',
        'QC2 Index': 'OMX',
        'CF2 Index': 'CAC',
        'EO2 Index': 'AEX',
        'GX2 Index': 'DAX',
        'HI2 Index': 'HSI',
        'IB2 Index': 'IBEX',
        'NI2 Index': 'NKY',
        'PT2 Index': 'TSX',
        'QZ2 Index': 'SG',
        'SM2 Index': 'SMI',
        'ST2 Index': 'MIB',
        'XP2 Index': 'AS51',
        'Z 2 Index': 'FTSE'}
    BLOOM_EQUITY_COLUMNS_RECENT = {
        'ES1 Index': 'SPX',
        'QC1 Index': 'OMX',
        'CF1 Index': 'CAC',
        'EO1 Index': 'AEX',
        'GX1 Index': 'DAX',
        'HI1 Index': 'HSI',
        'IB1 Index': 'IBEX',
        'NI1 Index': 'NKY',
        'PT1 Index': 'TSX',
        'QZ1 Index': 'SG',
        'SM1 Index': 'SMI',
        'ST1 Index': 'MIB',
        'XP1 Index': 'AS51',
        'Z 1 Index': 'FTSE'}
    BLOOM_EQUITY_COLUMNS = {'ES1 r:03_0_n index': 'SPX',
                            'QC1 r:03_0_n index': 'OMX',
                            'cf1 r:03_0_n index': 'CAC',
                            'eo1 r:03_0_n index': 'AEX',
                            'gx1 r:03_0_n index': 'DAX',
                            'hi1 r:03_0_n index': 'HSI',
                            'ib1 r:03_0_n index': 'IBEX',
                            'ni1 r:03_0_n index': 'NKY',
                            'pt1 r:03_0_n index': 'TSX',
                            'qz1 r:03_0_n index': 'SG',
                            'sm1 r:03_0_n index': 'SMI',
                            'st1 r:03_0_n index': 'MIB',
                            'xp1 r:03_0_n index': 'AS51',
                            'z 1 r:03_0_n index': 'FTSE'}

    BLOOM_EQUITY_WEST_GROUP = ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']
    BLOOM_EQUITY_EAST_GROUP = ['NKY', 'AS51', 'HSI', 'SG']

    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_index_and_return(self, from_db=True, save_file=True):
        self.logger.info("[STEP 1] LOAD DATA")

        if from_db:
            self.logger.info("[STEP 1 - 1] CONNECT TO BLOOM DATABASE")
            self._connect_database()

            self.logger.info("[STEP 1 - 3 ] CHANGE BLOOM DATA INTO STANDARD FORMAT")
            equity_columns = list(EquityStrategy.BLOOM_EQUITY_COLUMNS.keys())
            equity_columns = [column.upper() for column in equity_columns]
            bloom_query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(equity_columns))
            bloom = self.engine.execute(bloom_query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom = pd.DataFrame(rows, columns=columns)
            df_bloom = df_bloom[df_bloom['field'] == 'PX_LAST']

            df_bloom_pivot = df_bloom.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index = self.rename_columns(df_bloom_pivot, EquityStrategy.BLOOM_EQUITY_COLUMNS)
            self.ret = self.raw_index.pct_change(periods=1).iloc[1:]

            self.logger.info("[STEP 1 - 3] CHANGE BLOOM DATA INTO STANDARD FORMAT")
            equity_columns = list(EquityStrategy.BLOOM_EQUITY_COLUMNS_RECENT.keys())
            equity_columns = [column.upper() for column in equity_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(equity_columns))
            bloom = self.engine.execute(query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom_recent = pd.DataFrame(rows, columns=columns)

            df_bloom_pivot = df_bloom_recent.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index_recent = self.rename_columns(df_bloom_pivot, EquityStrategy.BLOOM_EQUITY_COLUMNS_RECENT)
            self.ret_recent = self.raw_index_recent.pct_change(periods=1).iloc[1:]

            self.logger.info("[STEP 1 - 3] CHANGE BLOOM DATA INTO STANDARD FORMAT")
            equity_columns = list(EquityStrategy.BLOOM_EQUITY_COLUMNS_NEXT.keys())
            equity_columns = [column.upper() for column in equity_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(equity_columns))
            bloom = self.engine.execute(query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom_next = pd.DataFrame(rows, columns=columns)

            df_bloom_pivot = df_bloom_next.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index_next = self.rename_columns(df_bloom_pivot, EquityStrategy.BLOOM_EQUITY_COLUMNS_NEXT)
            self.ret_next = self.raw_index_next.pct_change(periods=1).iloc[1:]

            for i in range(1, self.raw_index.shape[0] - 1):
                for j, column in enumerate(self.raw_index.columns):
                    if self.raw_index.index[i] <= pd.to_datetime('2007-12-31'):
                        continue
                    tday_front_diff = abs(
                        self.raw_index_recent.loc[self.raw_index.index[i], column] - self.raw_index.loc[self.raw_index.index[i], column])
                    tday_next_diff = abs(
                        self.raw_index_next.loc[self.raw_index.index[i], column] - self.raw_index.loc[self.raw_index.index[i], column])
                    yday_front_diff = abs(
                        self.raw_index_recent.loc[self.raw_index.index[i-1], column] - self.raw_index.loc[self.raw_index.index[i-1], column])
                    yday_next_diff = abs(
                        self.raw_index_next.loc[self.raw_index.index[i-1], column] - self.raw_index.loc[self.raw_index.index[i-1], column])

                    if (tday_next_diff < tday_front_diff) & (yday_next_diff >= yday_front_diff):
                        self.ret.loc[self.raw_index.index[i], column] = self.ret_next.loc[self.raw_index.index[i], column]
                    else:
                        self.ret.loc[self.raw_index.index[i], column] = self.ret.loc[self.raw_index.index[i], column]

            past_query = "SELECT * FROM GRP_PAST WHERE ORIGIN in ('totindex', 'priceindex')"
            past = self.engine.execute(past_query)
            rows = [row for row in past]
            columns = past.keys()
            df_past = pd.DataFrame(rows, columns=columns)
            df_past_totindex = df_past[df_past['origin'] == 'totindex']
            df_past_price_index = df_past[df_past['origin'] == 'priceindex']

            df_past_totindex = df_past_totindex.pivot(index='tdate', columns='ticker', values='value')
            df_past_totindex.index = pd.to_datetime(df_past_totindex.index)
            df_past_totindex = df_past_totindex[self.raw_index.columns]
            df_past_totindex = df_past_totindex.astype(np.float32)
            df_past_totindex.drop_duplicates(inplace=True)
            df_past_totindex_return = df_past_totindex.pct_change(periods=1)

            df_past_price_index = df_past_price_index.pivot(index='tdate', columns='ticker', values='value')
            df_past_price_index.index = pd.to_datetime(df_past_price_index.index)
            df_past_price_index = df_past_price_index[self.raw_index.columns]
            df_past_price_index = df_past_price_index.astype(np.float32)
            df_past_price_index.drop_duplicates(inplace=True)
            df_past_price_index_return = df_past_price_index.pct_change(periods=1)
            df_past_totindex_return[df_past_totindex_return.isna()] = df_past_price_index_return[df_past_totindex_return.isna()]
            self.raw_index = pd.concat([df_past_totindex.loc[:'2007-12-31'], self.raw_index.loc['2008-01-01':]], axis=0)
            self.ret = pd.concat([df_past_totindex_return.loc[:'2007-12-31'], self.ret.loc['2008-01-01':]], axis=0)
            self.ret.fillna(0, inplace=True)
            self.index = (1 + self.ret).cumprod()

            if save_file:
                self.logger.info("[STEP 1 - 4] SAVE RETURN DATA INTO CSV FORMAT")
                self.raw_index.to_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA.csv"))
                self.raw_index_recent.to_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA_RECENT.csv"))
                self.ret.to_csv(os.path.join(database_path, self.asset_type + ".csv"))
            else:
                self.logger.info("[STEP 1 - 1] LOAD DATA FROM FILE")
                self.raw_index = pd.read_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA.csv"),
                                             header=0,
                                             index_col=0,
                                             parse_dates=True)
                self.raw_index_recent = pd.read_csv(
                    os.path.join(database_path, self.asset_type + "_RAW_DATA_RECENT.csv"),
                    header=0,
                    index_col=0,
                    parse_dates=True)
                self.ret = pd.read_csv(os.path.join(database_path, self.asset_type + ".csv"),
                                       header=0,
                                       index_col=0,
                                       parse_dates=True)
                self.index = (1. + self.ret).cumprod()
            self.raw_index.columns.name = 'ticker'
            self.index.columns.name = 'ticker'
            self.ret.columns.name = 'ticker'
            self.raw_index.index.name = 'tdate'
            self.index.index.name = 'tdate'
            self.ret.index.name = 'tdate'


class EmergingStrategy(Strategy):
    BLOOM_EMERGING_COLUMNS_NEXT = {
        'AI2 Index': 'SA',
        'IDO2 Index': 'ID',
        'IH2 Index': 'IN',
        'IK2 Index': 'MY',
        'IS2 Index': 'MX',
        'KM2 Index': 'KR',
        'XU2 Index': 'CN',
        'EWZ US Equity': 'BR',
        'RSX US Equity': 'RU',
        'TW2 Index': 'TW'}
    BLOOM_EMERGING_COLUMNS_RECENT = {
        'AI1 Index': 'SA',
        'IDO1 Index': 'ID',
        'IH1 Index': 'IN',
        'IK1 Index': 'MY',
        'IS1 Index': 'MX',
        'KM1 Index': 'KR',
        'XU1 Index': 'CN',
        'EWZ US Equity': 'BR',
        'RSX US Equity': 'RU',
        'TW1 Index': 'TW'}
    BLOOM_EMERGING_COLUMNS = {'AI1 r:03_0_n index': 'SA',
                              'IDO1 r:03_0_n index': 'ID',
                              'IH1 r:03_0_n index': 'IN',
                              'IK1 r:03_0_n index': 'MY',
                              'IS1 r:03_0_n index': 'MX',
                              'KM1 r:03_0_n index': 'KR',
                              'XU1 r:03_0_n index': 'CN',
                              'ewz us equity': 'BR',
                              'rsx us equity': 'RU',
                              'tw1 r:03_0_n index': 'TW'
                              }

    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_index_and_return(self, from_db=True, save_file=True):
        self.logger.info("[STEP 1] LOAD DATA")

        if from_db:
            self.logger.info("[STEP 1 - 1] CONNECT TO BLOOM DATABASE")
            self._connect_database()

            self.logger.info("[STEP 1 - 3] CHANGE BLOOM AND PAST DATA INTO STANDARD FORMAT")
            emerging_columns = list(EmergingStrategy.BLOOM_EMERGING_COLUMNS.keys())
            emerging_columns = [column.upper() for column in emerging_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(emerging_columns))
            bloom = self.engine.execute(query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom = pd.DataFrame(rows, columns=columns)
            df_bloom = df_bloom[df_bloom['field'] == 'PX_LAST']

            df_bloom_pivot = df_bloom.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index = self.rename_columns(df_bloom_pivot, EmergingStrategy.BLOOM_EMERGING_COLUMNS)
            self.raw_index = self.raw_index.resample("B").last()
            self.ret = self.raw_index.pct_change(periods=1).iloc[1:]

            self.logger.info("[STEP 1 - 3] CHANGE BLOOM DATA INTO STANDARD FORMAT")
            emerging_columns = list(EmergingStrategy.BLOOM_EMERGING_COLUMNS_RECENT.keys())
            emerging_columns = [column.upper() for column in emerging_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(emerging_columns))
            bloom = self.engine.execute(query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom_recent = pd.DataFrame(rows, columns=columns)

            df_bloom_pivot = df_bloom_recent.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index_recent = self.rename_columns(df_bloom_pivot, EmergingStrategy.BLOOM_EMERGING_COLUMNS_RECENT)
            self.raw_index_recent = self.raw_index_recent.resample("B").last()
            self.ret_recent = self.raw_index_recent.pct_change(periods=1).iloc[1:]

            self.logger.info("[STEP 1 - 3] CHANGE BLOOM DATA INTO STANDARD FORMAT")
            emerging_columns = list(EmergingStrategy.BLOOM_EMERGING_COLUMNS_NEXT.keys())
            emerging_columns = [column.upper() for column in emerging_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(emerging_columns))
            bloom = self.engine.execute(query)
            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom_next = pd.DataFrame(rows, columns=columns)

            df_bloom_pivot = df_bloom_next.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            self.raw_index_next = self.rename_columns(df_bloom_pivot, EmergingStrategy.BLOOM_EMERGING_COLUMNS_NEXT)
            self.raw_index_next = self.raw_index_next.resample("B").last()
            self.ret_next = self.raw_index_next.pct_change(periods=1).iloc[1:]

            for i in range(1, self.raw_index.shape[0] - 1):
                if self.raw_index.index[i] <= pd.to_datetime('2012-12-31'):
                    continue
                for j, column in enumerate(self.raw_index.columns):
                    tday_front_diff = abs(
                        self.raw_index_recent.loc[self.raw_index.index[i], column] - self.raw_index.loc[self.raw_index.index[i], column])
                    tday_next_diff = abs(
                        self.raw_index_next.loc[self.raw_index.index[i], column] - self.raw_index.loc[self.raw_index.index[i], column])
                    yday_front_diff = abs(
                        self.raw_index_recent.loc[self.raw_index.index[i-1], column] - self.raw_index.loc[self.raw_index.index[i-1], column])
                    yday_next_diff = abs(
                        self.raw_index_next.loc[self.raw_index.index[i-1], column] - self.raw_index.loc[self.raw_index.index[i-1], column])

                    if (tday_next_diff < tday_front_diff) & (yday_next_diff >= yday_front_diff):
                        self.ret.loc[self.raw_index.index[i], column] = self.ret_next.loc[self.raw_index.index[i], column]
                    else:
                        self.ret.loc[self.raw_index.index[i], column] = self.ret.loc[self.raw_index.index[i], column]

            past_query = "SELECT * FROM GRP_PAST WHERE ORIGIN in ('totindex-em', 'priceindex-em')"
            past = self.engine.execute(past_query)
            rows = [row for row in past]
            columns = past.keys()
            df_past = pd.DataFrame(rows, columns=columns)
            df_past_totindex = df_past[df_past['origin'] == 'totindex-em']
            df_past_price_index = df_past[df_past['origin'] == 'priceindex-em']

            df_past_totindex = df_past_totindex.pivot(index='tdate', columns='ticker', values='value')
            df_past_totindex.index = pd.to_datetime(df_past_totindex.index)
            df_past_totindex = df_past_totindex[self.raw_index.columns]
            df_past_totindex = df_past_totindex.astype(np.float32)
            df_past_totindex_return = df_past_totindex.pct_change(periods=1)

            df_past_price_index = df_past_price_index.pivot(index='tdate', columns='ticker', values='value')
            df_past_price_index.index = pd.to_datetime(df_past_price_index.index)
            df_past_price_index = df_past_price_index[self.raw_index.columns]
            df_past_price_index = df_past_price_index.astype(np.float32)
            df_past_price_index_return = df_past_price_index.pct_change(periods=1)
            df_past_price_index_return.loc[df_past_totindex_return.index] = df_past_totindex_return

            self.logger.info("[STEP 1 - 4] MERGE BLOOM AND PAST DATA")
            self.raw_index = pd.concat([df_past_totindex.loc[:'2012-12-31'], self.raw_index.loc['2013-01-01':]], axis=0)
            self.ret = pd.concat([df_past_price_index_return.loc[:'2012-12-31'], self.ret.loc['2013-01-01':]], axis=0).iloc[1:]
            # self.raw_index = self.raw_index.reindex(pd.date_range(start=self.raw_index.index[0], end=self.raw_index.index[-1], freq='B'))
            # self.ret = self.ret.reindex(pd.date_range(start=self.ret.index[0], end=self.ret.index[-1], freq='B'))

            self.ret.fillna(0, inplace=True)
            self.index = (1. + self.ret).cumprod()
            self.index.index.name = 'tdate'
            self.ret.index.name = 'tdate'

            if save_file:
                self.logger.info("[STEP 1 - 5] SAVE RETURN DATA INTO CSV FORMAT")
                self.raw_index.to_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA.csv"))
                self.ret.to_csv(os.path.join(database_path, self.asset_type + ".csv"))

        else:
            self.logger.info("[STEP 1 - 1] LOAD DATA FROM FILE")
            self.raw_index = pd.read_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA.csv"), header=0,
                                         index_col=0,
                                         parse_dates=True)
            self.ret = pd.read_csv(os.path.join(database_path, self.asset_type + ".csv"), header=0,
                                         index_col=0,
                                         parse_dates=True)
            self.index = (1. + self.ret).cumprod()

        self.raw_index.columns.name = 'ticker'
        self.index.columns.name = 'ticker'
        self.ret.columns.name = 'ticker'
        self.raw_index.index.name = 'tdate'
        self.index.index.name = 'tdate'
        self.ret.index.name = 'tdate'


class IRStrategy(Strategy):
    BLOOM_IR_COLUMNS = {'cn1 n:04_0_r comdty': 'CANA',
                        'g 1 n:04_0_r comdty': 'GILT',
                        'jb1 n:04_0_r comdty': 'JGB',
                        'rx1 n:04_0_r comdty': 'BUND',
                        'ty1 n:04_0_r comdty': 'BOND'
                       }

    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_index_and_return(self, from_db=True, save_file=True):
        self.logger.info("[STEP 1] LOAD DATA")

        if from_db:
            self.logger.info("[STEP 1 - 1] CONNECT TO BLOOM DATABASE")
            self._connect_database()

            self.logger.info("[STEP 1 - 2] GET DATA FROM BLOOM DATABASE")
            ir_columns = list(IRStrategy.BLOOM_IR_COLUMNS.keys())
            ir_columns = [column.upper() for column in ir_columns]
            query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(ir_columns))
            bloom = self.engine.execute(query)

            rows = [row for row in bloom]
            columns = bloom.keys()
            df_bloom = pd.DataFrame(rows, columns=columns)
            df_bloom = df_bloom[df_bloom['field'] == 'PX_LAST']

            self.logger.info("[STEP 1 - 3] CHANGE BLOOM DATA INTO STANDARD FORMAT")
            df_bloom_pivot = df_bloom.pivot(index='tdate', columns='ticker', values='value')
            df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
            df_bloom_pivot = self.rename_columns(df_bloom_pivot, IRStrategy.BLOOM_IR_COLUMNS)
            self.raw_index = df_bloom_pivot
            self.ret = df_bloom_pivot.pct_change(periods=1)
            self.ret.fillna(0, inplace=True)
            self.index = (1. + self.ret).cumprod()
            self.index = self.index / self.index.iloc[0]

            if save_file:
                self.logger.info("[STEP 1 - 5] SAVE RETURN DATA INTO CSV FORMAT")
                self.ret.to_csv(os.path.join(database_path, self.asset_type + ".csv"))
                self.raw_index.to_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA.csv"))
        else:
            self.logger.info("[STEP 1 - 1] LOAD DATA FROM FILE")
            self.raw_index = pd.read_csv(os.path.join(database_path, self.asset_type + "_RAW_DATA.csv"), header=0,
                                         index_col=0,
                                         parse_dates=True)
            self.ret = pd.read_csv(os.path.join(database_path, self.asset_type + ".csv"), header=0,
                                         index_col=0,
                                         parse_dates=True)
            self.index = (1. + self.ret).cumprod()
        self.raw_index.columns.name = 'ticker'
        self.index.columns.name = 'ticker'
        self.ret.columns.name = 'ticker'
        self.raw_index.index.name = 'tdate'
        self.index.index.name = 'tdate'
        self.ret.index.name = 'tdate'


if __name__ == "__main__":
    strategy = EquityStrategy(asset_type="EQUITY", strategy_name='EPP')
    strategy.load_index_and_return(from_db=True, save_file=False)

