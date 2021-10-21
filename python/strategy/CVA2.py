import os
import sys

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from strategy import CommodityStrategy
from tester import Tester

base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
bloom_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


class CVA2(CommodityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.commopos = None

    def load_strategy_data(self, table='bloom', origin='commo pos'):
        self._connect_database()
        source = table.upper()
        metadata = sqlalchemy.MetaData(bind=self.engine)
        table = sqlalchemy.Table('GRP_{}'.format(source), metadata, autoload=True)
        query = "select * from info_bloom"
        info = self.engine.execute(query)
        rows = [row for row in info]
        columns = info.keys()
        info = pd.DataFrame(rows, columns=columns)
        data_info = info[info.origin == origin].set_index('ticker')
        query = sqlalchemy.select('*').where(table.c.ticker.in_(data_info.index.str.upper()))
        db_data = pd.read_sql(query, self.engine)
        db_data = db_data.pivot_table('value', 'tdate', 'ticker')
        grp = pd.unique(data_info.grp_ticker)

        commo = []
        for g in grp:
            tickers_ = data_info[data_info.grp_ticker == g].index

            for ticker in tickers_:
                if ticker.split(' ')[0][-2:].upper() == 'CN':
                    ticker1 = ticker
                if ticker.split(' ')[0][-2:].upper() == 'CS':
                    ticker2 = ticker

            k = db_data[ticker1.upper()] / db_data[ticker2.upper()]
            commo.append(k)

        commo = pd.concat(commo, 1)
        commo.columns = grp
        commo.index = pd.to_datetime(commo.index)
        commo.index.name = 'tdate'
        self.commopos = commo.shift(1)

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=12):
        """

        :param cs_num: Percent of positions for Cross Sectional Signal
        :param no_pos: no position zone in the middle of two extreme
        :param lookback_period: lookback period for calculating stdev
        :return:
        """
        self.logger.info('[STEP 3] CACULATE SIGNAL')

        for statrun in range(2):
            if statrun == 0:
                cret = self.ret[CommodityStrategy.BLOOM_COMMODITY_WEATHER_GROUP]
            if statrun == 1:
                cret = self.ret[CommodityStrategy.BLOOM_COMMODITY_NOTWEATHER_GROUP]

            compos1 = self.commopos[cret.columns]
            compos1 = compos1.iloc[
                compos1.reset_index().groupby(compos1.index.to_period('M'))[compos1.index.name].idxmax()]
            compos1 = compos1["1998":]
            Zscore = (compos1.iloc[minobs1-1:] - compos1.rolling(minobs1).mean()) / (compos1.rolling(minobs1).std())
            Zscore = Zscore.iloc[minobs1-1:]
            RV = -Zscore
            # 수정 : 2019-12-31 고정욱
            RVrank = RV.iloc[minobs1 - 1:] * 0

            for i in range(len(RV) - minobs1+1):
                data = RV.iloc[:(minobs1 + i)].rank(pct=True)
                RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]
#             pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
#             RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
#             RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 5. Long Short
            truecount = (RVrank.notnull().sum(axis=1) * CS).apply(round)
            tiebreaker = RVrank.rolling(5).mean() * 0.0000001
            tiebreaker.iloc[:4] = 0
            tied_RVrank = RVrank + tiebreaker

            # 1. Cross sectional
            CSRV = tied_RVrank.rank(axis=1, method='first')  # Short
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRV = CSRVpos

            # 2. Time Series
            TSRV = RVrank.fillna(0) * 0
            TSRV[RVrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV[RVrank < (1 - nopos) / 2] = -1  # Short

            if statrun == 0:
                TSRVrun1 = TSRV
                CSRVrun1 = CSRV
            else:
                TSRVrun2 = TSRV
                CSRVrun2 = CSRV

        TSRV = pd.concat([TSRVrun1, TSRVrun2], axis=1)/2
        CSRV = pd.concat([CSRVrun1, CSRVrun2], axis=1)/2

        TSRV = TSRV[self.ret.columns]
        CSRV = CSRV[self.ret.columns]

        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')

        # Align dates with each other
        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]


if __name__ == "__main__":
    strategy = CVA2(strategy_name="CVA2", asset_type="COMMODITY")
    strategy.load_index_and_return(from_db=False, save_file=False)
    strategy.set_rebalance_period(freq='month')
    strategy.load_strategy_data(table='bloom', origin='commo pos')
    strategy.calculate_signal(CS=0.35, nopos=0.4, minobs1=12)
    strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.15)
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='2001-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()