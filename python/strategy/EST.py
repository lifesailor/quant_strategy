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


class EST(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.growthvalue = None

    def load_strategy_data(self, table='datastream', origin='EPS'):
        self.growthvalue = self._load_strategy_data(table=table, origin=origin)

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=12, per=3):
        for i in range(2):
            if i == 0:
                RET = self.ret.loc[:, ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']]
            else:
                RET = self.ret.loc[:, ['NKY', 'AS51', 'HSI', "SG"]]

            self.logger.info('[STEP 3] CACULATE SIGNAL')
            GV = self.growthvalue
            sent = pd.DataFrame(GV.iloc[per:].to_numpy() / GV.iloc[:-per].to_numpy(),
                                columns=GV.columns,
                                index=GV.iloc[per:].index)

            b_columns = []
            v_columns = []

            for column in sent.columns:
                if column[-1].upper() == "V":
                    v_columns.append(column)
                else:
                    b_columns.append(column)

            sent = sent - 1
            sent1 = pd.DataFrame(
                sent.loc[:, b_columns].to_numpy() - sent.loc[:, v_columns].to_numpy(),
                columns=b_columns,
                index=sent.index)
            sent1 = sent1[RET.columns]
            RV = sent1.sub(sent1.mean(axis=1), axis=0)

            # 4. Rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 5. Long Short
            RV1 = RV.iloc[minobs1 - 1:, ]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)
            tiebreaker = RVrank.rolling(5).mean() * 0.0000001  # tie breaker what is this??
            tiebreaker.iloc[:4] = 0
            tied_RVrank = RV1 + tiebreaker

            # 1. Cross sectional
            CSRV = tied_RVrank.rank(axis=1, method='first')  # Short
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1

            # Final CS signal
            CSRV = CSRVpos
            CSRV.fillna(0, inplace=True)

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
    est = EST(strategy_name="EST", asset_type="EQUITY")
    est.load_index_and_return(from_db=False, save_file=False)
    est.load_strategy_data(table='FS', origin='growthvalue')
    est.set_rebalance_period(ts_freq='month', cs_freq='month')  # rebalance_day: monday = 0, sunday = 6
    est.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, per=3)
    est.set_portfolio_parameter(cs_strategy_type="notional")
    est.make_portfolio()

    tester = Tester(est)
    tester.set_period(start='1995-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()