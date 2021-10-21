import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from strategy import EmergingStrategy
from tester import Tester


base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
bloom_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


class EMPE(EmergingStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_strategy_data1(self, table='datastream', origin1='DPS-em', origin2="DPS1-em"):
        self.eps = self._load_strategy_data(table=table, origin=origin1)
        self.eps1 = self._load_strategy_data(table=table, origin=origin2)

    def load_strategy_data2(self, table='bloom', origin='10Yield-em'):
        self.yield10 = self._load_strategy_data(table=table, origin=origin)

    def load_raw_index(self):
        self.raw_index = self._load_strategy_data(table='datastream', origin='priceindex-em')

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=12, minobs=60, IDN='out'):
        group_1 = ['CN', 'KR', 'TW', 'IN']
        group_2 = ['MY', 'ID', 'BR', 'MX', 'RU', 'SA']

        if IDN == 'out':
            group_2.remove('ID')

        eps = self.eps
        eps1 = self.eps1
        price_index = self.raw_index.iloc[self.raw_index.reset_index().groupby(self.raw_index.index.to_period('M'))[
            self.raw_index.index.name].idxmax()]

        start_date = max(eps.index[0], price_index.index[0])
        end_date = min(eps.index[-1], price_index.index[-1])

        eps = eps[start_date:end_date]
        eps1 = eps1[start_date:end_date]
        price_index = price_index[start_date:end_date]

        Rvalue = eps / price_index.reindex(eps.index)
        Rvalue2 = eps / eps1

        CSRV1, TSRV1 = self.empe_csrv_tsrv(Rvalue, Rvalue2, group_1, CS)
        CSRV2, TSRV2 = self.empe_csrv_tsrv(Rvalue, Rvalue2, group_2, CS)

        TSRV = pd.concat([TSRV1, TSRV2], 1).reindex(columns=self.ret.columns).fillna(0)
        CSRV = pd.concat([CSRV1, CSRV2], 1).reindex(columns=self.ret.columns).fillna(0)

        TSRV = TSRV[self.ret.columns]
        CSRV = CSRV[self.ret.columns]

        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')

        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]

    def empe_csrv_tsrv(self, Rvalue, Rvalue2, use_cols, CS=0.35, minobs1=12, nopos=0.4):
        Rvalue = Rvalue.copy()[use_cols]
        Rvalue2 = Rvalue2.copy()[use_cols]
        RV = Rvalue

        RVrank = RV.iloc[minobs1 - 1:] * 0

        for i in range(len(RV) - minobs1 + 1):
            data = RV.iloc[:(minobs1 + i)].rank(pct=True)
            RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

        truecount = np.round((RVrank.shape[-1] - RVrank.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RVrank.columns)).reshape(RVrank.shape)

        tiebreaker = RVrank.rolling(5).mean() * 0.0000001  # tie breaker what is this??
        tiebreaker.iloc[:4] = 0

        tied_RVrank = RVrank + tiebreaker

        CSRV = tied_RVrank.rank(1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        #     CSRV1 = tied_RVrank.rank(1, method = 'first', ascending = False)

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        CSRV = CSRVpos.fillna(0)

        TSRV = RVrank * 0

        TSRV[RVrank > (nopos + (1 - nopos) / 2)] = 1
        TSRV[RVrank < ((1 - nopos) / 2)] = -1
        TSRV.fillna(0, inplace=True)

        CSRV_price = CSRV.copy()
        TSRV_price = TSRV.copy()

        RV = Rvalue2

        RVrank = RV.iloc[minobs1 - 1:] * 0

        for i in range(len(RV) - minobs1 + 1):
            data = RV.iloc[:(minobs1 + i)].rank(pct=True)
            RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

        truecount = np.round((RVrank.shape[-1] - RVrank.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RVrank.columns)).reshape(RVrank.shape)

        tiebreaker = RVrank.rolling(5).mean() * 0.0000001  # tie breaker what is this??
        tiebreaker.iloc[:4] = 0
        tied_RVrank = RVrank + tiebreaker

        CSRV = tied_RVrank.rank(1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T
        CSRVpos = CSRV * 0
        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1
        CSRV = CSRVpos.fillna(0)

        TSRV = RVrank * 0
        TSRV[RVrank > (nopos + (1 - nopos) / 2)] = 1
        TSRV[RVrank < ((1 - nopos) / 2)] = -1
        TSRV.fillna(0, inplace=True)

        CSRV_ep = CSRV
        TSRV_ep = TSRV

        CSRV = CSRV_price * 0.8 + CSRV_ep
        TSRV = TSRV_ep
        return CSRV, TSRV


if __name__ == "__main__":
    strategy = EMPE(strategy_name="EMPE", asset_type="EMERGING")
    strategy.load_index_and_return(from_db=False, save_file=True)
    strategy.load_strategy_data1(table='DS', origin1='EPS-em', origin2='EPS1-em')
    strategy.load_strategy_data2(table='bloom', origin='10Yield-em')

    strategy.set_rebalance_period(freq='month')
    strategy.calculate_signal(CS=0.35, nopos=0.4, minobs1=12, minobs=60, IDN='out')
    strategy.set_portfolio_parameter(cs_strategy_type='notional', min_vol=0.15, factorsd=12, assetsd=12, statsd=12)
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1991-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()