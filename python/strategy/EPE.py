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


class EPE(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.yield10 = None
        self.eps = None
        self.eps1 = None
        self.index1 = None

    def load_strategy_data(self, table='datastream', origin1='EPS', origin2="EPS1"):
        self.eps = self._load_strategy_data(table=table, origin=origin1)
        self.eps1 = self._load_strategy_data(table=table, origin=origin2)

    def load_raw_index(self):
        self.raw_index = self._load_strategy_data(table='datastream', origin='priceindex')

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=12):
        for i in range(2):
            if i == 0:
                RET = self.ret.loc[:, ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']]
            else:
                RET = self.ret.loc[:, ['NKY', 'AS51', 'HSI', "SG"]]

            index = self.index.loc[:, RET.columns]

            # 1. Load Data
            eps = self.eps
            eps1 = self.eps1
            index1 = self.raw_index
            index1 = index1.loc[index1.index.isin(eps.index)]

            eps = eps[index.columns]
            eps1 = eps1[index.columns]
            index1 = index1[index.columns]

            start_date = max(eps.index[0], index1.index[0])
            end_date = min(eps.index[-1], index1.index[-1])

            eps = eps[start_date:end_date]
            eps1 = eps1[start_date:end_date]
            index1 = index1[start_date:end_date]

            # 2. Parameter Setting
            EY = eps
            EYD = eps1

            # Signal1 = EPS / index
            Rvalue = pd.DataFrame(EY.to_numpy() / index1.to_numpy(), columns=index1.columns, index=index1.index)
            RV = Rvalue

            # 4. Rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 5. Long Short
            truecount = (RVrank.notnull().sum(axis=1) * CS).apply(round)
            tiebreaker = RVrank.rolling(5).mean() * 0.0000001
            tiebreaker.iloc[:4] = 0
            tied_RVrank = RVrank + tiebreaker

            # 1. Cross sectional
            CSRV = tied_RVrank.rank(axis=1, method='first')
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

            CSRV6 = CSRV
            TSRV6 = TSRV

            # Signal 2 - EPS / EPS1
            Rvalue = EY / EYD
            RV = Rvalue

            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 5. Long Short
            truecount = (RVrank.notnull().sum(axis=1) * CS).apply(round)
            tiebreaker = RVrank.rolling(5).mean().fillna(0) * 0.0000001

            # 1. Cross sectional
            CSRV = (RVrank + tiebreaker).rank(axis=1, method='first')  # Short
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

            CSRV = CSRV6 + CSRV
            TSRV = TSRV

            if i == 0:
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
    epe = EPE(strategy_name="EPE", asset_type="EQUITY")
    epe.load_index_and_return(from_db=True, save_file=True)
    epe.load_strategy_data(table='DS', origin1='EPS', origin2='EPS1')

    epe.set_rebalance_period(ts_freq='month', cs_freq='month')
    epe.calculate_signal(minobs1=12, nopos=0.4, CS=0.35)
    epe.set_portfolio_parameter(cs_strategy_type='notional')
    epe.make_portfolio()

    tester = Tester(epe)
    tester.set_period(start='1995-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()