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


class CCA3(CommodityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_strategy_data(self, table='bloom', origin1='carry-com', origin2='carry-com2'):
        self.carry1 = self._load_strategy_data(table=table, origin=origin1)
        self.carry1 = self.carry1.resample('M').last()
        self.carry2 = self._load_strategy_data(table=table, origin=origin2)
        self.carry2 = self.carry2.resample('M').last()

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs=60):
        """

        :param cs_num: Percent of positions for Cross Sectional Signal
        :param no_pos: no position zone in the middle of two extreme
        :param lookback_period: lookback period for calculating stdev
        :return:
        """
        self.logger.info('[STEP 3] CACULATE SIGNAL')
        w_group = ["C", "S", "SB", "SM", "W", "KC", "CT"]  # weather sensitive comdty group
        n_w_group = [x for x in self.ret.columns if x not in w_group]
        c_groups = [w_group, n_w_group]
        div_group_flag = True  # divide groups for cross sectional strategy

        RV1 = self.carry1 + self.carry1.shift(1)
        RV2 = self.carry2 + self.carry2.shift(1)

        pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
        RVrank1 = RV1.rolling(minobs).apply(pctrank)  # it takes some time

        # Time Series Signal
        TSRV1 = RVrank1 * 0.
        TSRV1[RVrank1 > (nopos + (1. - nopos) / 2.)] = 1.
        TSRV1[RVrank1 < ((1. - nopos) / 2.)] = -1.

        TSRV2 = RV2 * 0.
        TSRV2[RV2 > 0.] = 1.
        TSRV2[RV2 < 0.] = -1.

        TSRV = TSRV1 * 0.5 + TSRV2

        # Cross SEctional Signal
        if div_group_flag:
            CSRV = pd.DataFrame()
            for c_group in c_groups:
                truecount_lower = (RV1[c_group].notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
                truecount_upper = RV1[c_group].notnull().sum(axis=1) - truecount_lower

                CSRV1 = RV1[c_group].rank(axis=1)
                CSRV1[CSRV1.apply(lambda x: x <= truecount_lower, axis=0)] = -1.
                CSRV1[CSRV1.apply(lambda x: x > truecount_upper, axis=0)] = 1.

                CSRV1 = CSRV1.applymap(lambda x: x if x == 1. or x == -1. else 0.)

                truecount_lower = (RV2[c_group].notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
                truecount_upper = RV2[c_group].notnull().sum(axis=1) - truecount_lower

                CSRV2 = RV2[c_group].rank(axis=1)
                CSRV2[CSRV2.apply(lambda x: x <= truecount_lower, axis=0)] = -1.
                CSRV2[CSRV2.apply(lambda x: x > truecount_upper, axis=0)] = 1.

                CSRV2 = CSRV2.applymap(lambda x: x if x == 1. or x == -1. else 0.)

                CSRV = pd.concat([CSRV, (CSRV1 * 0.5 + CSRV2)], axis=1)

        else:
            truecount_lower = (RV1.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
            truecount_upper = RV1.notnull().sum(axis=1) - truecount_lower

            CSRV1 = RV1.rank(axis=1)
            CSRV1[CSRV1.apply(lambda x: x <= truecount_lower, axis=0)] = -1.
            CSRV1[CSRV1.apply(lambda x: x > truecount_upper, axis=0)] = 1.

            CSRV1 = CSRV1.applymap(lambda x: x if x == 1. or x == -1. else 0.)

            truecount_lower = (RV2.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
            truecount_upper = RV2.notnull().sum(axis=1) - truecount_lower

            CSRV2 = RV2.rank(axis=1)
            CSRV2[CSRV2.apply(lambda x: x <= truecount_lower, axis=0)] = -1.
            CSRV2[CSRV2.apply(lambda x: x > truecount_upper, axis=0)] = 1.

            CSRV2 = CSRV2.applymap(lambda x: x if x == 1. or x == -1. else 0.)
            CSRV = CSRV1 * 0.5 + CSRV2

        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')

        # Align dates with each other
        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]


if __name__ == "__main__":
    strategy = CCA3(strategy_name="CCA3", asset_type="COMMODITY")
    strategy.load_index_and_return(from_db=True, save_file=True)
    strategy.load_strategy_data(origin1='carry-com', origin2='carry-com2')
    strategy.set_rebalance_period(freq='month', rebalance_weekday=1)
    strategy.calculate_signal(CS=0.35, nopos=0.4, minobs=60)
    strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.15)
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='2001-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()