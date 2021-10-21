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


class CPM(CommodityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def calculate_signal(self, CS=0.35, minobs=52, longlen=52, shortlen=35):
        """

        :param CS: percentage of position for cross sectional signal
        :param minobs:
        :param longlen: long term price momentum period
        :param shortlen: short term price momentum period
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

            # 1. Long term Signal
            # 1-1. Magnitude
            index_52_week_after = cindex.iloc[longlen:]
            index_52_week_before = cindex.iloc[:index_52_week_after.shape[0]]
            Mag = pd.DataFrame(index_52_week_after.to_numpy() / index_52_week_before.to_numpy(), columns=cindex.columns)
            Mag -= 1
            Mag.index = index_52_week_after.index

            # 1-2. reliability
            ret = pd.DataFrame(cindex.iloc[1:].to_numpy() / cindex.iloc[:-1].to_numpy(), columns=cindex.columns)
            ret -= 1
            ret.index = cindex.index[1:]
            stdev = ret.rolling(window=longlen, min_periods=1).std().fillna(0).iloc[longlen - 1:] * np.sqrt(longlen)
            stdev.index = cindex.iloc[longlen:].index

            # 1-3-1. Cross Sectional 1
            RV = Mag.copy()
            RV1 = RV.iloc[minobs - 1:, ]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider

            CSRV = RV1.rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRelpos = CSRVpos

            # 1-3-2. Time Series
            TSRel = RV.iloc[minobs - 1:]
            TS1 = TSRel * 0
            TS1[TSRel > 0.] = 1.
            TS1[TSRel < 0.] = -1.

            # 1-3-3. Cross Sectional 2
            up = ret.applymap(lambda x: 1 if x >= 0 else 0)
            Conroll = up.rolling(longlen, min_periods=1).sum().iloc[minobs - 1:, ] / (longlen)
            RV = Conroll.iloc[:Conroll.shape[0], :]
            RV1 = RV.iloc[minobs - 1:, ]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider

            CSRV = RV1.rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSConpos = CSRVpos

            # 1-3-4. Time Series 2
            TSRel = RV.iloc[minobs - 1:, :]
            TS2 = TSRel * 0
            TS2[TSRel > 0.5] = 1
            TS2[TSRel < 0.5] = -1

            # 1-4. Final position
            TSRVL = TS1 * 1 / 3 + TS2 * 1 / 3  # 52 week return * 1/3 + 52 up weeks ratio * 1/3
            CSRVL = CSRelpos * 1 / 3 + CSConpos * 1 / 3

            # 2. Short term Signal
            # 2-1. Magnitude
            Mag = cindex.iloc[longlen:, :]
            obs = Mag.shape[0]
            Mag = pd.DataFrame(
                cindex.iloc[shortlen:shortlen + obs, :].to_numpy() / \
                cindex.iloc[:obs].to_numpy(),
                columns=cindex.columns)
            Mag -= 1
            Mag.index = cindex.iloc[longlen:, :].index

            # 2-2. Reliability
            ret = pd.DataFrame(cindex.iloc[1:].to_numpy() / cindex.iloc[:-1].to_numpy(), columns=cindex.columns)
            ret -= 1
            ret.index = cindex.index[1:]
            STDEV = ret.rolling(window=longlen, min_periods=1).std().fillna(0).iloc[longlen - 1:] * np.sqrt(52)
            STDEV.index = cindex.iloc[longlen:].index

            # 2-3-1. Cross Sectional 1
            RV = Mag
            RV1 = RV.iloc[minobs - 1:, ]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider

            CSRV = RV1.rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRelpos = CSRVpos

            # 2-3-2. Time Series 1
            TSRel = RV.iloc[minobs - 1:]
            TS1 = TSRel * 0
            TS1[TSRel > 0.] = 1.
            TS1[TSRel < 0.] = -1.

            # # Cross Sectional 2
            # up = ret.applymap(lambda x: 1 if x >= 0 else 0)
            # Conroll = up.rolling(shortlen, min_periods=1).sum().iloc[minobs - 1:, ] / \
            #           (shortlen)
            # RV = Conroll.iloc[:Conroll.shape[0] - shortlen, :]
            # RV1 = RV.iloc[minobs - 1:, ]
            # truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
            #
            # CSRV = RV1.rank(axis=1, method='first')
            # CSRV1 = (RV1 * -1).rank(axis=1, method='first')
            #
            # CSRVpos = CSRV * 0
            # CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            # CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            # CSConpos = CSRVpos
            #
            # # TS2
            # TSRel = RV.iloc[minobs - 1:, :]
            # TS2 = TSRel * 0
            # TS2[TSRel > 0.5] = 1
            # TS2[TSRel < 0.5] = -1

            # final position
            TSRVSh = TS1 * 1 / 3  # + TS2*WGT[2]
            CSRVSh = CSRelpos * 1 / 3  # + CSConpos*WGT2[2]

            # 5. Long Momentum + Short Momentum Combination
            TSRV = TSRVSh.loc[TSRVL.index, :] * 1 + TSRVL * 1
            CSRV = CSRVSh.loc[CSRVL.index, :] * 1 + CSRVL * 1

            if i == 0:
                TSRVrun1 = TSRV
                CSRVrun1 = CSRV
            if i == 1:
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
    strategy = CPM(strategy_name="CPM", asset_type="COMMODITY")
    strategy.load_index_and_return(from_db=True, save_file=True)
    strategy.set_rebalance_period(freq='week', rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
    strategy.calculate_signal(CS=0.35, minobs=52, longlen=52, shortlen=17)
    strategy.set_portfolio_parameter(cs_strategy_type='vol')
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1991-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()