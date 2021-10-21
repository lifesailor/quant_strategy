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


class EQL(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.eps = None

    def load_strategy_data(self, table='factset', origin1='ROA', origin2="ICR"):
        self.roa = self._load_strategy_data(table=table, origin=origin1)
        self.icr = self._load_strategy_data(table=table, origin=origin2)

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=60, lag=1):
        """

        :param cs_num: percentage of position for cross sectional signal
        :param min_obs:
        :param longlen: long term price momentum period
        :param shortlen: short term price momentum period
        :return:
        """
        self.logger.info('[STEP 3] CACULATE SIGNAL')
        for i in range(2):
            if i == 0:
                RET = self.ret.loc[:, ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']]
            else:
                RET = self.ret.loc[:, ['NKY', 'AS51', 'HSI', "SG"]]

            index = self.index.loc[:, RET.columns]

            # 1. Load Data
            roa = self.roa.copy()
            roa = roa[index.columns]

            icr = self.icr.copy()
            icr = icr[index.columns]

            # 3. RV
            RV1 = roa
            RV = (RV1 - RV1.shift(lag)).iloc[lag:]

            # 4. rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 5. trade
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
            CSRVone = CSRV

            # 2. Time Series
            TSRV1 = RVrank * 0
            TSRV1[RVrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV1[RVrank < (1 - nopos) / 2] = -1  # Short

            # 2. Signal2
            RV1 = icr
            RV = (RV1 - RV1.shift(lag)).iloc[lag:]

            # 1. rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 2. trade
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
            CSRVtwo = CSRV

            # 2. Time Series
            TSRV2 = RVrank.fillna(0) * 0
            TSRV2[RVrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV2[RVrank < (1 - nopos) / 2] = -1  # Short

            TSRV = (2 * TSRV1 + TSRV2) / 3
            CSRV = (CSRVone + 2 * CSRVtwo) / 3

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
    eql = EQL(strategy_name="EQL", asset_type="EQUITY")
    eql.load_index_and_return(from_db=False, save_file=False)
    eql.load_strategy_data(table='FS', origin1='ROA', origin2='ICR')
    eql.set_rebalance_period(ts_freq='month', cs_freq='month')  # rebalance_day: monday = 0, sunday = 6
    eql.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, lag=1)
    eql.set_portfolio_parameter(cs_strategy_type="notional")
    eql.make_portfolio()

    tester = Tester(eql)
    tester.set_period(start='2006-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()