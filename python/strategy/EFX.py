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


class EFX(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.fx = None

    def load_strategy_data(self, table='bloom', origin='fx'):
        self.fx = self._load_strategy_data(table=table, origin=origin)
        self.fx = self.fx.iloc[self.fx.reset_index().groupby(self.fx.index.to_period('M'))[self.fx.index.name].idxmax()]
        self.fx.index = pd.to_datetime(self.fx.index, format='%Y%m%d')

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=12, longlen=12, shortlen=0, SDEV=12):
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

            index = self.index[RET.columns]
            fx = self.fx
            fx = fx[index.columns]

            # 3. Signal 1. Reverse of FX Strength
            Mag = fx.iloc[longlen - shortlen:].to_numpy() / fx[:-(longlen - shortlen)].to_numpy() - 1
            Mag = pd.DataFrame(Mag, columns=fx.columns)
            Mag.index = fx.index[longlen:]
            RV = -1 * Mag

            # 3-1. Rank
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

            # Final CS signal
            Magrank = RVrank
            CSMagpos = CSRVpos

            # Signal2. Reverse of FX Strength / 12 month STDEV
            ret = fx.iloc[1:].to_numpy() / fx[:-1].to_numpy() - 1
            ret = pd.DataFrame(ret, columns=fx.columns)
            ret.index = fx.index[1:]

            STDEV = ret.rolling(SDEV).std() * np.sqrt(12)
            STDEV1 = STDEV.iloc[longlen - 1:]
            RV = (-1 * Mag) / STDEV1
            RV1 = RV.iloc[minobs1 - 1:]

            # 4. Rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 5. Long Short
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            # 1. Cross sectional
            CSRV = RV1.rank(1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1

            # Final CS signal
            Relrank = RVrank
            CSRelpos = CSRVpos
            CSRV = CSRelpos * 1 / 2 + CSMagpos * 1 / 2

            # 2. Time Series
            TSRV1 = Magrank.fillna(0) * 0
            TSRV1[Magrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV1[Magrank < (1 - nopos) / 2] = -1  # Short

            TSRV2 = Relrank.fillna(0) * 0
            TSRV2[Relrank > nopos + (1 - nopos) / 2] = 1  # Long
            TSRV2[Relrank < (1 - nopos) / 2] = -1  # Short
            TSRV = 1 / 2 * TSRV1 + 1 / 2 * TSRV2

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
    strategy = EFX(strategy_name="EFX", asset_type="EQUITY")
    strategy.load_index_and_return(from_db=True, save_file=True)
    strategy.load_strategy_data(table='bloom', origin='fx')
    strategy.set_rebalance_period(freq='month')
    strategy.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, longlen=12, shortlen=0, SDEV=12)
    strategy.set_portfolio_parameter(cs_strategy_type='notional')
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1995-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()