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


class EEM(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.err = None

    def load_strategy_data(self, table='datastream', origin='ERR'):
        self.err = self._load_strategy_data(table=table, origin=origin)

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=12, longlen=12, shortlen=6, lag=0):
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

            # 1. Load Data
            err = self.err.copy()

            Mag = err - err.rolling(window=longlen + 1).mean()
            Mag = Mag.iloc[longlen:]
            Mag = Mag[RET.columns]

            STDEV = Mag.rolling(window=shortlen).mean()
            STDEV = STDEV.iloc[shortlen - 1:]

            RV = STDEV
            RV1 = RV.iloc[:len(RV) - lag]
            RV1.index = RV.index[lag:]
            RV = RV1

            # 4. Rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 3-2. Long Short
            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            CSRV = RV1.rank(1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRV = CSRVpos

            # 2. Time Series
            TSRV = RVrank.fillna(0) * 0
            TSRV[RVrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV[RVrank < (1 - nopos) / 2] = -1  # Short

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
    strategy = EEM(strategy_name="EEM", asset_type="EQUITY")
    strategy.load_index_and_return(from_db=True, save_file=True)  # from_db=False
    strategy.load_strategy_data(table='datastream', origin='ERR')
    strategy.set_rebalance_period(ts_freq='month', cs_freq='month')
    strategy.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, longlen=12, shortlen=6, lag=0)
    strategy.set_portfolio_parameter(cs_strategy_type='notional')
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1995-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()