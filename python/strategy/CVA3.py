import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from strategy import CommodityStrategy
from tester import Tester

base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
bloom_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


class CVA3(CommodityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.fx = None
        self.fut1price =None

    def load_strategy_data1(self, table='bloom', origin='fx'):
        self.fx = self._load_strategy_data(table=table, origin=origin)

    def load_strategy_data2(self, table='past', origin='fut1price-com'):
        if self.engine is None:
            self._connect_database()

        commdity_columns = list(CommodityStrategy.BLOOM_COMMODITY_COLUMNS.keys())
        commdity_columns = [column.upper() for column in commdity_columns]
        query = "SELECT * FROM GRP_BLOOM WHERE TICKER IN {} AND FIELD = 'PX_LAST'".format(tuple(commdity_columns))
        bloom = self.engine.execute(query)
        rows = [row for row in bloom]
        columns = bloom.keys()
        df_bloom = pd.DataFrame(rows, columns=columns)

        df_bloom_pivot = df_bloom.pivot(index='tdate', columns='ticker', values='value')
        df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]
        self.fut1price = self.rename_columns(df_bloom_pivot, CommodityStrategy.BLOOM_COMMODITY_COLUMNS)

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs=60, minobs1=12, SMA=1, LMA=12, Lwindow=54):
        """

        :param cs_num: Percent of positions for Cross Sectional Signal
        :param no_pos: no position zone in the middle of two extreme
        :param lookback_period: lookback period for calculating stdev
        :return:
        """
        self.logger.info('[STEP 3] CACULATE SIGNAL')

        for i in range(2):
            if i == 0:
                cret = self.ret[CommodityStrategy.BLOOM_COMMODITY_WEATHER_GROUP]
            if i == 1:
                cret = self.ret[CommodityStrategy.BLOOM_COMMODITY_NOTWEATHER_GROUP]

            index = self.index[cret.columns]
            fut1price = self.fut1price
            fx = self.fx.iloc[self.fx.reset_index().groupby(self.fx.index.to_period('M'))[self.fx.index.name].idxmax()]

            monthly_index = [index for index in fut1price.index if index in fx.index]
            fut1price = fut1price.loc[monthly_index, index.columns]

            shortma = fut1price.rolling(SMA).mean()
            shortma = shortma.iloc[SMA - 1:]

            longma = fut1price.rolling(LMA).mean()
            longma = longma.iloc[LMA - 1:]

            longma1 = longma.iloc[:longma.shape[0] - Lwindow + 1, :]
            longma1.index = longma.index[longma.shape[0] - longma1.shape[0]:]
            shortma = shortma.loc[longma1.index, :]

            # 2. Longma - Shortma 비교
            # ratio of long ma price to short ma price
            Rvalue = (longma1 / shortma) - 1

            # substract average of all other comdty
            RV = Rvalue.sub(Rvalue.mean(axis=1), axis=0)

            # RV rank
            RVrank = RV.iloc[minobs1 - 1:, ] * 0
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.rolling(minobs, min_periods=1).apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 3. Cross Sectional
            RV1 = RV.iloc[minobs1 - 1:, ]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider

            CSRV = RV1.rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRV = CSRVpos.fillna(0)

            # 4. Time Series
            TSRV = RVrank * 0
            TSRV[RVrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV[RVrank < (1 - nopos) / 2] = -1  # Short

            if i == 0:
                TSRVrun1 = TSRV
                CSRVrun1 = CSRV
            else:
                TSRVrun2 = TSRV
                CSRVrun2 = CSRV

        TSRV = pd.concat([TSRVrun1, TSRVrun2], axis=1)
        CSRV = pd.concat([CSRVrun1, CSRVrun2], axis=1)

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
    strategy = CVA3(strategy_name="CVA3", asset_type="COMMODITY")
    strategy.load_index_and_return(from_db=True, save_file=True)
    strategy.set_rebalance_period(freq='month')  # rebalance_day: monday = 0, sunday = 6
    strategy.load_strategy_data1(table='bloom', origin='fx')
    strategy.load_strategy_data2(table='past', origin='fut1price-com')
    strategy.calculate_signal(CS=0.35, nopos=0.4, minobs=60, minobs1=12, SMA=1, LMA=12, Lwindow=54)
    strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.15)
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='2001-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()