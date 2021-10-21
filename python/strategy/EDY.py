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


class EDY(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.yield10 = None
        self.dps = None
        self.dps1 = None
        self.index1 = None

    def load_strategy_data1(self, table='datastream', origin1='DPS', origin2="DPS1"):
        self.dps = self._load_strategy_data(table=table, origin=origin1)
        self.dps1 = self._load_strategy_data(table=table, origin=origin2)

    def load_strategy_data2(self, table='bloom', origin='10Yield'):
        self.yield10 = self._load_strategy_data(table=table, origin=origin)
        self.yield10 = self.yield10.resample('M').last()

    def load_raw_index(self):
        equity_columns = list(EquityStrategy.BLOOM_EQUITY_COLUMNS.values())
        equity_columns = [column.upper() for column in equity_columns]
        bloom_query = "SELECT * FROM GRP_RAW_INDEX WHERE TICKER IN {}".format(tuple(equity_columns))
        bloom = self.engine.execute(bloom_query)
        rows = [row for row in bloom]
        columns = bloom.keys()
        df_bloom = pd.DataFrame(rows, columns=columns)
        df_bloom_pivot = df_bloom.pivot(index='tdate', columns='ticker', values='value')
        df_bloom_pivot.index = [''.join(str(index).split('-')) for index in df_bloom_pivot.index]

    def calculate_signal(self, CS=0.35, nopos=0.4, minobs1=12):
        self.index.index.name = 'time'

        for i in range(2):
            if i == 0:
                RET = self.ret.loc[:, ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']]
            else:
                RET = self.ret.loc[:, ['NKY', 'AS51', 'HSI', "SG"]]

            # 1. Load Data
            DPS = self.dps
            DPS1 = self.dps1
            yield10 = self.yield10
            yield10 = yield10.loc[yield10.index.isin(DPS.index)]
            index1 = self.raw_index
            index1 = index1.loc[index1.index.isin(DPS.index)]

            start_date = max(DPS.index[0], index1.index[0], yield10.index[0])
            end_date = min(DPS.index[-1], index1.index[-1], yield10.index[-1])

            index1 = index1.loc[start_date:end_date]
            yield10 = yield10.loc[start_date:end_date]
            DPS = DPS.loc[start_date:end_date]
            DPS1 = DPS1.loc[start_date:end_date]

            index1 = index1[RET.columns]
            yield10 = yield10[RET.columns]
            DPS = DPS[RET.columns]
            DPS1 = DPS1[RET.columns]

            # 3. Calculate Dividend Yield
            no_data_dates = DPS[(DPS.isnull().sum(axis=1) > 0).values].index
            DPS.loc[no_data_dates] = DPS1.loc[no_data_dates]

            DYP = index1.loc[DPS.index, :]
            DY = DPS / DYP
            Rvalue = DY - yield10 / 100
            RV = Rvalue

            # 4. Rank
            pctrank = lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            RVrank = RV.expanding().apply(pctrank, raw=True)  # it takes some time
            RVrank = RVrank.iloc[minobs1 - 1:, ]

            # 5. Long Short
            truecount = (RVrank.notnull().sum(axis=1) * CS).apply(round)
            tiebreaker = RVrank.rolling(5).mean().fillna(0) * 0.0000001

            # 1. Cross sectional
            CSRV = (RVrank + tiebreaker).rank(axis=1, method='first')  # Short
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1

            # Final CS signal
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

        self.TSRV.to_csv('EDY_tsrv' + str(i)+'.csv', ',')
        self.CSRV.to_csv('EDY_csrv' + str(i)+'.csv', ',')        
        
        # Align dates with each other
        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]


if __name__ == "__main__":
    strategy = EDY(strategy_name="EDY", asset_type="EQUITY")
    strategy.load_index_and_return(from_db=True, save_file=True)
    strategy.load_strategy_data1(table='DS', origin1='DPS', origin2='DPS1')
    strategy.load_strategy_data2(table='bloom', origin='10Yield')
    strategy.set_rebalance_period(ts_freq='month', cs_freq='month')
    strategy.calculate_signal(minobs1=12, nopos=0.4, CS=0.35)
    strategy.set_portfolio_parameter(cs_strategy_type='notional')
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='2006-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()