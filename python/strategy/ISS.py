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


class ISS(IRStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def calculate_signal(self, CS=0.5, day1=24, short=0.2, use_JGB=False):
        if not use_JGB:
            selected_columns = list(self.ret.columns)
            selected_columns.remove('JGB')
            self.index = self.index[selected_columns]
            self.ret = self.ret[selected_columns]
        else:
            pass

        # ISS - TS: Daily, CS: monthly
        RET = self.ret

        # Time Series Signal
        TSRV = RET * 0. - 1 * short
        TSRV.loc[(np.roll(TSRV.index.day >= day1, -1)) & (TSRV.index.month == np.roll(TSRV.index.month, -1))] = 1

        # Cross Sectional Signal
        RET_ym = RET.copy()
        RET_ym['yearmonth'] = RET_ym.index.year * 100 + RET_ym.index.month
        RET_ym['month'] = RET_ym.index.month

        from_ym = 199202
        start_ym = 199702
        ym_list = [x for x in RET_ym['yearmonth'].unique() if x >= start_ym]
        bible = {}
        for ym in ym_list:
            month_start_date = RET_ym.loc[(RET_ym.yearmonth == ym)].index[0]
            bible[month_start_date] = \
                RET_ym.loc[(RET_ym.yearmonth < ym) & (RET_ym.yearmonth >= from_ym) & (
                            RET_ym.month == month_start_date.month), RET.columns].mean() / \
                RET_ym.loc[(RET_ym.yearmonth < ym) & (RET_ym.yearmonth >= from_ym) & (
                            RET_ym.month == month_start_date.month), RET.columns].std()
        RV = pd.DataFrame(bible).T

        truecount_lower = (RV.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
        truecount_upper = RV.notnull().sum(axis=1) - truecount_lower

        truecount = (RV.notnull().sum(axis=1) * CS).apply(round)  # number of asset to consider
        CSRV = RV.rank(axis=1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0
        CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
        CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
        CSRV = CSRVpos
        CSRV = CSRV.shift(-1) # to make same signal with R. Not sure this is right for this logic

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
    base_path = os.path.abspath('..')
    data_path = os.path.join(base_path, 'data')
    bond = pd.read_csv(os.path.join(data_path, 'bonds.csv'), header=0, index_col=0, parse_dates=True)
    BRet = bond.pct_change(1)
    Bindex = (1. + BRet).cumprod().iloc[1:]
    BRet = BRet.iloc[1:]

    iss = ISS(strategy_name="ISS", asset_type="IR")
    iss.index = Bindex.copy()
    iss.ret = BRet.copy()

    # iss.load_index_and_return(from_db=False, save_file=False)
    iss.set_rebalance_period(ts_freq='day', cs_freq='month')
    iss.calculate_signal(cs=0.5, day1=24, short=0.2, use_JGB=False)

    iss.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.04)
    iss.make_portfolio()

    # strategy = ISS(strategy_name="ISS", asset_type="IR")
    # strategy.load_index_and_return(from_db=False, save_file=True)
    # strategy.set_rebalance_period(freq='month')
    # strategy.calculate_signal(cs=0.5, day1=24, short=0.2, use_JGB=False)
    # strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.04)
    # strategy.make_portfolio()
    #
    # tester = Tester(strategy)
    # tester.set_period(start='1997-01-01', end='2018-05-09')
    # tester.run()
    # tester.plot_result()