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


class ELQ(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.m2gdp = None

    def load_strategy_data(self, table='CEIC', origin='m2gdp'):
        self.m2gdp = self._load_strategy_data(table=table, origin=origin)

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=12, longlen=6, shortlen=0):
        for i in range(2):
            if i == 0:
                RET = self.ret.loc[:, ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']]
            else:
                RET = self.ret.loc[:, ['NKY', 'AS51', 'HSI', "SG"]]

            # 1. Load Data
            index = self.index[RET.columns]

            m2gdp = self.m2gdp
            m2gdp = m2gdp[index.columns]

            # 3. Signal 1. m2gdb d+1year / d+0year
            Mag = m2gdp.iloc[longlen:]
            Mag = m2gdp.iloc[longlen - shortlen:].to_numpy() / m2gdp[:-longlen].to_numpy() - 1
            Mag = pd.DataFrame(Mag, columns=m2gdp.columns)
            Mag.index = m2gdp.index[longlen:]

            # 4. Rank
            RV = Mag
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            CSRV = (RV1).rank(axis=1, method='first')  # Short
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1

            # Final CS signal
            Magrank = RVrank
            CSMagpos = CSRVpos

            # 3. Signal 1. (m2gdb d+1year / d+0year) / 1년치 std
            ret = m2gdp.iloc[1:].to_numpy() / m2gdp.iloc[:-1].to_numpy() - 1
            ret = pd.DataFrame(ret, columns=m2gdp.columns)
            ret.index = m2gdp.index[1:]

            STDEV = ret.rolling(longlen).std() * np.sqrt(12)
            STDEV1 = STDEV.iloc[longlen - 1:]
            RV = (Mag) / STDEV1
            RV1 = RV.iloc[minobs1 - 1:]

            # 4. Rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 5. Long Short
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            # 1. Cross sectional
            CSRV = (RV1).rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1

            # Final CS signal
            Relrank = RVrank
            CSRelpos = CSRVpos

            # only use CSRelpos since WGT2[1] is 0
            CSRV = CSRelpos * 1 + CSMagpos * 0

            # 2. Time Series
            TSRV1 = Magrank.fillna(0) * 0
            TSRV1[Magrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV1[Magrank < (1 - nopos) / 2] = -1  # Short

            TSRV2 = Relrank.fillna(0) * 0
            TSRV2[Relrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV2[Relrank < (1 - nopos) / 2] = -1  # Short
            TSRV = TSRV1 * 0 + TSRV2 * 1

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
    elq = ELQ(strategy_name="ELQ", asset_type="EQUITY")
    elq.load_index_and_return(from_db=True, save_file=True)
    elq.load_strategy_data(table='CEIC', origin='m2gdp')

    elq.set_rebalance_period(ts_freq='month', cs_freq='month')
    elq.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, longlen=6, shortlen=0)
    elq.set_portfolio_parameter(cs_strategy_type='notional')
    elq.make_portfolio()


    tester = Tester(elq)
    tester.set_period(start='2000-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()