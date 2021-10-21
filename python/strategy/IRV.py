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
bloom_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


class IRV(IRStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.err = None
        self.twoYR = None
        self.tenYR = None
        self.ry = None
        self.ry2 = None

    def load_strategy_data1(self, table='ds', origin='ERR'):
        self.err = self._load_strategy_data(table=table, origin=origin)

    def load_strategy_data2(self, table='bloom', origin1="bonds10yr", origin2='realyield', origin3='realyield2'):
        self.tenYR = self._load_strategy_data(table=table, origin=origin1)
        self.ry = self._load_strategy_data(table=table, origin=origin2)
        self.ry2 = self._load_strategy_data(table=table, origin=origin3)
        self.ry2 = self.ry2.resample("BM").last().shift(2)

    def calculate_signal(self, CS=0.5, nopos=0.4, minobs=12, use_JGB=False):
        if not use_JGB:
            selected_columns = ['BOND', 'CANA',  'BUND', 'GILT']
            dm_idx_tickers = [u'SPX', u'TSX', u'DAX', u'FTSE']
            self.index = self.index[selected_columns]
            self.ret = self.ret[selected_columns]
        else:
            dm_idx_tickers = [u'SPX', u'TSX', u'DAX', u'FTSE', u'NKY']

        RET = self.ret
        err = self.err
        tenYR = self.tenYR
        ry = self.ry
        ry2 = self.ry2
        err = err[dm_idx_tickers]

        # Time Series
        RV = -1 * err + err.shift(3)
        pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
        RVrank = RV.expanding(min_periods=minobs).apply(pctrank)  # it takes some time
        RVrank.columns = RET.columns

        TSRV = RVrank * 0.
        TSRV[RVrank > (nopos + (1. - nopos) / 2.)] = 1.
        TSRV[RVrank < ((1. - nopos) / 2.)] = -1.
        TSRV.dropna(how='all', inplace=True)

        # Cross Sectional
        RV = tenYR.loc[ry2.index] - ry2
        RVrank = RV.expanding(min_periods=minobs).apply(pctrank)  # it takes some time

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
        CSRV_1 = CSRVpos
        CSRV_1.fillna(0, inplace=True)

        # Strategy 2. yr 2month change
        RV2 = (ry - ry.shift(2)).iloc[minobs:]
        truecount = (RV2.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
        CSRV = RV2.rank(axis=1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0
        CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
        CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
        CSRV_2 = CSRVpos
        CSRV = CSRV_1 + CSRV_2 * 0.5

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
    strategy = IRV(strategy_name="IRV", asset_type="IR")
    strategy.load_index_and_return(from_db=True, save_file=False)
    strategy.load_strategy_data1(table='ds', origin='ERR')
    strategy.load_strategy_data2(table='bloom', origin1='bonds10yr', origin2='realyield', origin3='realyield2')

    strategy.set_rebalance_period(freq='month')
    strategy.calculate_signal(CS=0.5, nopos=0.4, minobs=12, use_JGB=False)
    strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.04)
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1995-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()