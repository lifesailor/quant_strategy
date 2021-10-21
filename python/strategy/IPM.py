import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from strategy import IRStrategy
from tester import Tester


base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
bloom_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


class IPM(IRStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def calculate_signal(self, CS=0.5, nopos=0.4, minobs=260, minobs1=52, longlen=52, shortlen=4, use_JGB=False):
        if not use_JGB:
            selected_columns = ['BOND', 'CANA',  'BUND', 'GILT']
            self.index = self.index[selected_columns]
            self.ret = self.ret[selected_columns]
        else:
            pass

        # Param Settings
        index = self.index

        # index to weekly
        index = index[index.index.weekday == self.rebalance_weekday] 
        
        # Making Signal
        # Momentum
        # Time Series
        Mag = index.pct_change(longlen - shortlen).shift(shortlen).iloc[minobs1:]
        TS_l = Mag * 0.
        TS_l[Mag > 0.] = 1.
        TS_l[Mag < 0.] = -1.
        
        # Cross Sectional
        # 수정 후
        RV = Mag
        RV1 = RV.iloc[minobs1 - 1:]
        truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
        CSRV = RV1.rank(axis=1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0
        CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
        CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
        CSRV_l = CSRVpos

        # 수정 전
        # truecount_lower = (Mag.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
        # truecount_upper = Mag.notnull().sum(axis=1) - truecount_lower
        #
        # CSRV = Mag.rank(axis=1)
        # CSRV[CSRV.apply(lambda x: x <= truecount_lower, axis=0)] = -1.
        # CSRV[CSRV.apply(lambda x: x > truecount_upper, axis=0)] = 1.
        # CSRV_l = CSRV.applymap(lambda x: x if x == 1. or x == -1. else 0.)
        
        # Short-term momentum
        # Time Series
        Mag = index.pct_change(shortlen).iloc[minobs1:]
        TS_s = Mag * 0.
        TS_s[Mag > 0.] = 1.
        TS_s[Mag < 0.] = -1.
        
        # Cross Sectional
        # 수정 후
        RV = Mag
        RV1 = RV.iloc[minobs1 - 1:]
        truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
        CSRV = RV1.rank(axis=1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0
        CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
        CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
        CSRV_s = CSRVpos

        # 수정 전
        # truecount_lower = (Mag.notnull().sum(axis=1) * CS).apply(round) # number of asset to consider
        # truecount_upper = Mag.notnull().sum(axis=1) - truecount_lower
        #
        # CSRV = Mag.rank(axis=1)
        # CSRV[CSRV.apply(lambda x: x <= truecount_lower, axis=0)] = -1.
        # CSRV[CSRV.apply(lambda x: x > truecount_upper, axis=0)] = 1.
        # CSRV_s = CSRV.applymap(lambda x: x if x == 1. or x == -1. else 0.)

        TSRV = TS_s * 0.5 + TS_l * 1.
        CSRV = CSRV_s * 0.5 + CSRV_l * 1.

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
    strategy = IPM(strategy_name="IPM", asset_type="IR")
    strategy.load_index_and_return(from_db=False, save_file=False)
    strategy.set_rebalance_period(freq='week', rebalance_weekday=1)
    strategy.calculate_signal(CS=0.5, nopos=0.4, minobs=260, minobs1=52, longlen=52, shortlen=4, use_JGB=False)
    strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.04)
    strategy.make_portfolio()

    tester = Tester(strategy)

    tester.set_period(start='1995-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()