import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from strategy import EquityStrategy
from tester import Tester


base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
bloom_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


class ECA(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.carry_dm = None

    def load_strategy_data(self, table='bloom', origin='carry_dm'):
        self.carry_dm = self._load_strategy_data(table=table, origin=origin)
        self.carry_dm['DAX'] = np.nan

    def calculate_signal(self,  CS=0.35, nopos=0.4, minobs1=12):
        self.index.index.name = 'tdate'

        for type in range(2):
            if type == 0:
                RET = self.ret.loc[:, ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']]
            else:
                RET = self.ret.loc[:, ['NKY', 'AS51', 'HSI', "SG"]]

            carry = self.carry_dm
            carry = carry[RET.columns]

            RV = carry.iloc[minobs1 - 1:]
            RVrank = RV.iloc[minobs1 - 1:] * 0
            for i in range(len(RV) - minobs1 + 1):
                data = RV.iloc[:(minobs1 + i)].rank(pct=True)
                RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

            RV1 = RV.iloc[minobs1 - 1:]
            truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
            truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

            CSRV = RV1.rank(1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV * 0
            CSRVpos[CSRV <= truecount] = -1
            CSRVpos[CSRV1 <= truecount] = 1
            CSRV = CSRVpos
            CSRV.fillna(0, inplace=True)

            TSRV = RVrank * 0
            TSRV[RVrank > (nopos + (1 - nopos) / 2)] = 1
            TSRV[RVrank < ((1 - nopos) / 2)] = -1

            if type == 0:
                TSRVrun1 = TSRV
                CSRVrun1 = CSRV
                RV_1 = RV.copy()
            else:
                TSRVrun2 = TSRV
                CSRVrun2 = CSRV
                RV_2 = RV.copy()

        TSRV = pd.concat([TSRVrun1, TSRVrun2], axis=1)
        CSRV = pd.concat([CSRVrun1, CSRVrun2], axis=1)

        TSRV = TSRV[self.ret.columns]
        CSRV = CSRV[self.ret.columns]

        self.RV = pd.concat([RV_1, RV_2], axis=1)
        self.RV = self.RV[self.ret.columns]

        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')

        # Align dates with each other
        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]


if __name__ == "__main__":
    strategy = ECA(strategy_name="ECA", asset_type="EQUITY")
    strategy.load_index_and_return(from_db=False, save_file=True)
    strategy.load_strategy_data(table='bloom', origin='carry-dm')
    strategy.set_rebalance_period(freq='month')
    strategy.calculate_signal(minobs1=12, nopos=0.4, CS=0.35)
    strategy.set_portfolio_parameter(cs_strategy_type='notional')
    strategy.make_portfolio()
    tester = Tester(strategy)
    tester.set_period(start='2006-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()