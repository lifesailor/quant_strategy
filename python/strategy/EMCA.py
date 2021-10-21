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


class EMCA(EmergingStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_strategy_data(self, table='bloom', origin='carry-em'):
        self.carry_em = self._load_strategy_data(table=table, origin=origin)

    def calculate_signal(self, CS=0.5, minobs1=12, nopos=0.4):
        carry = self.carry_em
        use_col = carry.columns.drop(['MY', 'RU', 'BR', 'ID'])
        self.carry = carry

        TSRV, CSRV = self.emca_tsrv_csrv(carry, use_col, CS, nopos, minobs1)
        TSRV = TSRV.reindex(columns=self.ret.columns).fillna(0)
        CSRV = CSRV.reindex(columns=self.ret.columns).fillna(0)

        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')

        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]

    def emca_tsrv_csrv(self, carry, use_cols, CS=0.35,  nopos=0.4, minobs1=12):
        carry = carry.copy()[use_cols]
        # RV rank
        RV = carry.iloc[minobs1 - 1:]
        RVrank = RV.iloc[minobs1 - 1:] * 0

        for i in range(len(RV) - minobs1 + 1):
            data = RV.iloc[:(minobs1 + i)].rank(pct=True)
            RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

        # CSRV
        RV1 = RV.iloc[minobs1 - 1:]
        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        CSRV = RV1.rank(1, method='first')
        CSRV1 = RV1.rank(1, method='first', ascending=False)
        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1
        CSRV = CSRVpos
        CSRV.fillna(0, inplace=True)

        # TSRV
        TSRV = RVrank * 0
        TSRV[RVrank > (nopos + (1 - nopos) / 2)] = 1
        TSRV[RVrank < ((1 - nopos) / 2)] = -1
        return TSRV, CSRV


if __name__ == "__main__":
    strategy = EMCA(strategy_name="EMCA", asset_type="EMERGING")
    strategy.load_index_and_return(from_db=False, save_file=True)
    strategy.load_strategy_data(table='bloom', origin='carry-em')
    strategy.set_rebalance_period(freq='month')
    strategy.calculate_signal(CS=0.5, minobs1=12, nopos=0.4)
    strategy.set_portfolio_parameter(cs_strategy_type='notional', min_vol=0.15)
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1991-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()