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


class CVO(CommodityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    @staticmethod
    def skewness(x):
        return sum((x - np.mean(x)) ** 3 / np.var(x) ** (3 / 2)) / len(x)

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs=52):
        """

        :param CS: percentage of position for cross sectional signal
        :param minobs:
        :param longterm_length: long term price momentum period
        :param shortterm_length: short term price momentum period
        :return:
        """
        self.logger.info('[STEP 3] CACULATE SIGNAL')

        for i in range(2):
            if i == 0:
                self.logger.info('[STEP 2 - 1] CACULATE SIGNAL FOR WEATHER GROUP')
                cret = self.ret[CommodityStrategy.BLOOM_COMMODITY_WEATHER_GROUP]
            if i == 1:
                self.logger.info('[STEP 2 - 2] CACULATE SIGNAL FOR NOT WEATHER GROUP')
                cret = self.ret[CommodityStrategy.BLOOM_COMMODITY_NOTWEATHER_GROUP]

            cindex = self.index[cret.columns]
            cindex = cindex[cindex.index.to_series().dt.dayofweek == self.rebalance_weekday]

            # Skewness 기준으로 정렬
            ret = pd.DataFrame(cindex.iloc[1:].to_numpy() / cindex.iloc[:-1].to_numpy(), columns=cindex.columns)
            ret -= 1
            ret.index = cindex.index[1:]
            STDEV = ret.rolling(window=minobs).apply(lambda x: CVO.skewness(x), raw=True).dropna()
            STDEV.index = ret.index[minobs - 1:]
            RV = -1 * STDEV  # multiply -1 for inverse rank

            # Rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs - 1:]

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
    strategy = CVO(strategy_name="CVO", asset_type="COMMODITY")
    strategy.load_index_and_return(from_db=False, save_file=True)

    strategy.set_rebalance_period(freq='week', rebalance_weekday=3)  # rebalance_day: monday = 0, sunday = 6
    strategy.calculate_signal(CS=0.35, nopos=0.4, minobs=52)
    strategy.set_portfolio_parameter(cs_strategy_type='vol')
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1991-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()