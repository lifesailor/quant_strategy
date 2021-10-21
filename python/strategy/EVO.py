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


class EVO(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.growthvalue = None

    def load_strategy_data(self, table='bloom', origin='ivol'):
        self.ivol = self._load_strategy_data(table=table, origin=origin)

    def calculate_signal(self, minobs1=12, minobs=60, nopos=0.4, CS=0.35):
        for statrun in range(2):
            if statrun == 0:
                RET = self.ret.loc[:, ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']]
            else:
                RET = self.ret.loc[:, ['NKY', 'AS51', 'HSI', "SG"]]

            index = self.index.loc[:, RET.columns]

            # 1. Load Data
            ivol = self.ivol.copy()
            ivol = ivol[[column for column in ivol.columns if column in index.columns]]
            ivol = ivol['2002':]
            ivol.index = pd.to_datetime(ivol.index)

            # 3. Signal
            ivol1 = (ivol - ivol.shift(1)).iloc[1:]
            RV = (ivol1.T - ivol1.mean(axis=1).T).T

            # 4. Rank : 고정욱 수정 -2019-12-31 위 코드는 특정구간에서 nan이 됨
            RVrank = RV.iloc[minobs1 - 1:] * 0

            for i in range(minobs - minobs1):
                data = RV.iloc[:(minobs1 + i)].rank(pct=True)
                RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

            for i in range(minobs - minobs1, len(RV) - minobs1 + 1):
                data = RV.iloc[i - (minobs - minobs1): (minobs1 + i)].rank(pct=True)
                RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

            # 5. trade
            truecount = (RVrank.notnull().sum(axis=1) * CS).apply(round)
            tiebreaker = RVrank.rolling(5).mean() * 0.0000001
            tiebreaker.iloc[:4] = 0
            tied_RVrank = RVrank + tiebreaker

            # 1. Cross sectional
            CSRV = tied_RVrank.rank(axis=1, method='first')  # Short
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRV = CSRVpos
            CSRV.fillna(0, inplace=True)

            # 2. Time Series
            TSRV = RVrank.fillna(0) * 0
            TSRV[RVrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV[RVrank < (1 - nopos) / 2] = -1  # Short

            if statrun == 0:
                TSRVrun1 = TSRV
                CSRVrun1 = CSRV
                RV_1 = RV.copy()
            else:
                TSRVrun2 = TSRV
                CSRVrun2 = CSRV
                RV_2 = RV.copy()

        TSRV = pd.concat([TSRVrun1, TSRVrun2], axis=1)
        CSRV = pd.concat([CSRVrun1, CSRVrun2], axis=1)

        TSRV['SG'] = 0
        TSRV['IBEX'] = 0
        CSRV['SG'] = 0
        CSRV['IBEX'] = 0

        TSRV = TSRV[self.ret.columns]
        CSRV = CSRV[self.ret.columns]

        self.RV = pd.concat([RV_1, RV_2], axis=1)
        self.RV['SG'] = 0
        self.RV['IBEX'] = 0
        self.RV = self.RV[self.ret.columns]

        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')

        # Align dates with each other
        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]


if __name__ == "__main__":
    strategy = EVO(strategy_name="EVO", asset_type="EQUITY")
    strategy.load_index_and_return(from_db=False, save_file=False)
    strategy.load_strategy_data(table='bloom', origin='ivol')
    strategy.set_rebalance_period(freq='month')
    strategy.calculate_signal(minobs1=12, nopos=0.4, cs=0.35)
    strategy.set_portfolio_parameter(cs_strategy_type="notional")
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='2003-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()