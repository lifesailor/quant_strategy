import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from strategy import IRStrategy, EquityStrategy
from tester import Tester

base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
database_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


class ICA2(IRStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.twoYR = None
        self.tenYR = None

    def load_strategy_data(self, table='bloom', origin1='bonds2yr', origin2="bonds10yr"):
        self.twoYR = self._load_strategy_data(table=table, origin=origin1)
        self.tenYR = self._load_strategy_data(table=table, origin=origin2)

    def calculate_signal(self, CS=0.5, minobs=12, use_JGB=False):

        # calculate carry
        if not use_JGB:
            selected_columns = list(self.ret.columns)
            selected_columns.remove('JGB')

            self.index = self.index[selected_columns]
            self.ret = self.ret[selected_columns]
            twoYR = self.twoYR[selected_columns]
            tenYR = self.tenYR[selected_columns]
        else:
            twoYR = self.twoYR
            tenYR = self.tenYR

        carry = tenYR - twoYR

        # calculate 252 day daily volatility
        carry_std = carry.rolling(window=252).std()

        # adjust to volatility
        RV = carry / carry_std
        RV = RV.loc[RV.groupby(RV.index.to_period('M')).apply(lambda x: x.index.max())].iloc[minobs:]

        # Time Series Signal
        pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
        TSRV = RV.expanding(minobs).apply(pctrank)  # it takes some time

        # Cross Sectional Signal
        carry_monthly = carry.loc[carry.groupby(carry.index.to_period('M')).apply(lambda x: x.index.max())].iloc[minobs:]

        # strategy1. 3 month carry change
        carry_3m_chg = carry_monthly - carry_monthly.shift(3)
        RV = carry_3m_chg

        # Cross Sectional Signal
        truecount = (RV.notnull().sum(axis=1) * CS).apply(round)

        # 1. Cross sectional
        CSRV = (RV).rank(axis=1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV.fillna(0) * 0
        CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
        CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
        CSRV_1 = CSRVpos

        # strategy2. 6 month carry change
        carry_6m_chg = carry_monthly - carry_monthly.shift(6)
        RV = carry_6m_chg

        CSRV = (RV).rank(axis=1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV.fillna(0) * 0
        CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
        CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
        CSRV_2 = CSRVpos
        CSRV = CSRV_1 + CSRV_2

        # Align dates with Return DataFrame
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
    strategy = ICA2(strategy_name="ICA2", asset_type="IR")
    strategy.load_index_and_return(from_db=True, save_file=True)
    strategy.load_strategy_data(table='bloom', origin1='bonds2yr', origin2='bonds10yr')
    strategy.set_rebalance_period(freq='month')
    strategy.calculate_signal(CS=0.5, minobs=12, use_JGB=False)
    strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.04)
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1995-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()