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


class CVA(CommodityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def calculate_signal(self, CS=0.35, nopos=0.6, lookback_period=63, minobs1=52):
        """

        :param cs_num: Percent of positions for Cross Sectional Signal
        :param nopos: no position zone in the middle of two extreme
        :param lookback_period: lookback period for calculating stdev
        :return:
        """
        self.logger.info('[STEP 3] CACULATE SIGNAL')

        # Notes. 대표님의 요구로 날씨 민감 그룹과 날씨 민감하지 않은 그룹을 나눠서 실제 했었음 > 나누지 않는 것이 더 나았음.
        for i in range(2):
            if i == 0:
                self.logger.info('[STEP 2 - 1] CACULATE SIGNAL FOR WEATHER GROUP')
                cret = self.ret[CommodityStrategy.BLOOM_COMMODITY_WEATHER_GROUP]
            if i == 1:
                self.logger.info('[STEP 2 - 2] CACULATE SIGNAL FOR NOT WEATHER GROUP')
                cret = self.ret[CommodityStrategy.BLOOM_COMMODITY_NOTWEATHER_GROUP]

            # Making Signal
            Retpos = cret[cret >= 0]
            Retpos.iloc[0] = 0
            Retneg = cret[cret <= 0]
            Retneg.iloc[0] = 0

            STDpos = Retpos.rolling(window=lookback_period, min_periods=1).std().fillna(0).iloc[lookback_period - 1:]
            STDneg = Retneg.rolling(window=lookback_period, min_periods=1).std().fillna(0).iloc[lookback_period - 1:]

            RV = STDneg - STDpos
            RV = RV[RV.index.weekday == self.rebalance_weekday]  # to weekly

            # 수정
            RVrank = RV.iloc[minobs1 - 1:]
            RVrank = RVrank * 0
            for j in range(RV.shape[0] - minobs1 + 1):
                RVrank.iloc[j, :] = RV.iloc[:minobs1 + j].rank().iloc[minobs1 + j - 1] / (
                            (minobs1 + j) - RV.iloc[0:minobs1 + j].isna().sum())

            # 수정 전
            # pctrank = lambda x: pd.Series(x).rank(pct=True, method='first').iloc[-1]
            # RVrank = RV.expanding().apply(pctrank)  # it takes some time

            TSRV = RVrank.fillna(0) * 0
            TSRV[RVrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV[RVrank < (1 - nopos) / 2] = -1  # Short

            # Cross Sectional Signal
            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            CSRV = (RV1).rank(axis=1, method='first')  # Short
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRV = CSRVpos

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
    strategy = CVA(strategy_name="CVA", asset_type="COMMODITY")
    strategy.load_index_and_return(from_db=False, save_file=True)
    strategy.set_rebalance_period(freq='week', rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
    strategy.calculate_signal(CS=0.35, nopos=0.6, lookback_period=63, minobs1=52)
    strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.15)
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='2001-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()