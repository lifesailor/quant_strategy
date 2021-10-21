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


class IEQ(IRStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.Eindex = None

    def calculate_signal(self, CS=0.5, nopos=0.4, use_JGB=False):
        if not use_JGB:
            selected_columns = ['BOND', 'CANA',  'BUND', 'GILT']
            dm_idx_tickers = [u'SPX', u'TSX', u'DAX', u'FTSE']

            self.index = self.index[selected_columns]
            self.ret = self.ret[selected_columns]
        else:
            dm_idx_tickers = [u'SPX', u'TSX', u'DAX', u'FTSE', u'NKY']

        RET = self.ret

        # Making Signal
        Eqindex = self.Eindex.loc[self.Eindex.groupby(self.Eindex.index.to_period('M')).apply(
            lambda x: x.index.max()), dm_idx_tickers]  # to monthly, and selected ticker

        RV1 = ((Eqindex.pct_change(1) + 1) * -1).loc[self.index.index[0]:]
        RV3 = ((Eqindex.pct_change(3) + 1) * -1).loc[self.index.index[0]:]
        RV6 = ((Eqindex.pct_change(6) + 1) * -1).loc[self.index.index[0]:]
        RV = (RV1 + RV3 + RV6).iloc[12-1:]

        pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
        RVrank = RV.expanding().apply(pctrank)  # it takes some time
        RVrank.columns = RET.columns

        # Time Series
        TSRV = RVrank * 0.
        TSRV[RVrank > (nopos + (1. - nopos) / 2.)] = 1.
        TSRV[RVrank < ((1. - nopos) / 2.)] = -1.
        TSRV.dropna(how='all', inplace=True)

        # Cross Sectional
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
        CSRV.fillna(0, inplace=True)

        # Align dates with Return DataFrame
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

    eindex_path = os.path.join(data_path, 'totindex.csv')
    eindex1_path = os.path.join(data_path, 'priceindex.csv')
    efuture_path = os.path.join(data_path, 'FutGenratio1.csv')

    total_ret_idx = pd.read_csv(eindex_path, header=0, index_col=0, parse_dates=True)
    total_ret = total_ret_idx.pct_change(1)
    prc_idx = pd.read_csv(eindex1_path, header=0, index_col=0, parse_dates=True)
    prc_ret = prc_idx.pct_change(1)
    fut_idx = pd.read_csv(efuture_path, header=0, index_col=0, parse_dates=True)
    fut_ret = fut_idx.pct_change(1)

    # 만약에 eindex에 빈 정보가 있으면 eindex1으로 대체
    no_data_dates = total_ret[(total_ret.isnull().sum(axis=1) > 0).values].index
    total_ret.loc[no_data_dates] = prc_ret.loc[no_data_dates]

    # 2007년까지는 eindex 2008년부터는 future 사용
    ERet = pd.concat([total_ret.loc[:'2007-12-31'], fut_ret.loc['2008-01-01':]], axis=0)
    ERet.fillna(0, inplace=True)
    Eindex = (1. + ERet).cumprod()

    ERet.index.name = 'tdate'
    Eindex.index.name = 'tdate'

    ERet.columns.name = 'ticker'
    Eindex.columns.name = 'ticker'

    # equity_strategy = EquityStrategy(strategy_name='helper', asset_type='EQUITY')
    # equity_strategy.load_index_and_return(from_db=False, save_file=False)
    ieq = IEQ(strategy_name="IEQ", asset_type="IR")
    ieq.index = Bindex.copy()
    ieq.ret = BRet.copy()

    # ieq.load_index_and_return(from_db=True, save_file=False)
    ieq.Eindex = Eindex.copy()

    ieq.set_rebalance_period(ts_freq='month', cs_freq='month')
    ieq.calculate_signal(CS=0.5, nopos=0.4, use_JGB=False)


    # equity_strategy = EquityStrategy(strategy_name='helper', asset_type='EQUITY')
    # equity_strategy.load_index_and_return(from_db=True, save_file=False)
    #
    # strategy = IEQ(strategy_name="IEQ", asset_type="IR")
    # strategy.load_index_and_return(from_db=True, save_file=False)
    # strategy.Eindex = equity_strategy.index
    # strategy.set_rebalance_period(freq='month')
    # strategy.calculate_signal(CS=0.5, nopos=0.4, use_JGB=False)
    # strategy.set_portfolio_parameter(cs_strategy_type='vol', min_vol=0.04)
    # strategy.make_portfolio()
    #
    # tester = Tester(strategy)
    # tester.set_period(start='1995-01-01', end='2018-05-09')
    # tester.run()
    # tester.plot_result()