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


class EPM(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.eps = None

    def load_strategy_data(self, table='datastream', origin='EPS'):
        self.eps = self._load_strategy_data(table=table, origin=origin)

    def calculate_signal(self, CS=0.35, minobs1=52, longlen=52, longlen2=13, shortlen=2):
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
                RET = self.ret[EquityStrategy.BLOOM_EQUITY_WEST_GROUP]
                index = self.index[EquityStrategy.BLOOM_EQUITY_WEST_GROUP]
            if i == 1:
                RET = self.ret[EquityStrategy.BLOOM_EQUITY_EAST_GROUP]
                index = self.index[EquityStrategy.BLOOM_EQUITY_EAST_GROUP]

            index = index[index.index.weekday == 1]

            self.logger.info('[STEP 3 - 1 - 1] SIGNAL 1. LONG TERM MOMENTUM')
            Mag = index.pct_change(longlen - shortlen).shift(shortlen).iloc[minobs1:]
            RV = Mag
            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            # 1. Cross sectional
            CSRV = (RV1).rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRelpos = CSRVpos

            TSRel = RV.iloc[minobs1 - 1:]
            TS1 = TSRel.copy() * 0.
            TS1[TSRel > 0.] = 1.
            TS1[TSRel < 0.] = -1.

            self.logger.info('[STEP 3 - 1 - 2] SIGNAL 2. LONG TERM MOMENTUM - PERCENTAGE OF UP DAYS')
            ret = index.pct_change(1).iloc[1:]
            up = ret.applymap(lambda x: 1 if x > 0 else 0)
            conroll = up.rolling(longlen - shortlen).sum() / (longlen - shortlen)
            conroll = conroll.iloc[longlen - shortlen - 1:]
            RV = conroll.iloc[:-1 * shortlen]
            RV.index = Mag.index
            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            # 1. Cross sectional
            CSRV = (RV1).rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSConpos = CSRVpos
            TSRel = RV.iloc[minobs1 - 1:]
            TS2 = TSRel.copy() * 0.
            TS2[TSRel > 0.5] = 1.
            TS2[TSRel < 0.5] = -1.

            TSRVL = TS1 * 1 / 3 + TS2 * 1 / 3
            CSRVL = CSRelpos * 1 / 3 + CSConpos * 1 / 3

            self.logger.info('[STEP 3 - 2] SIGNAL 3. SHORT TERM  MOMENTUM')
            Ret = RET
            Mag = index.iloc[longlen2:]
            obs = Mag.shape[0]
            Mag = pd.DataFrame(
                (index.iloc[(longlen2 - shortlen):(longlen2 - shortlen + obs)].values / index.iloc[:obs].values),
                columns=index.columns,
                index=index.iloc[:obs].index) - 1
            Mag.index = index.index[longlen2:]
            RV = Mag
            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            # 1. Cross sectional
            CSRV = (RV1).rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSRelpos = CSRVpos
            TSRel = RV.iloc[minobs1 - 1:]
            TS1 = TSRel.copy() * 0.
            TS1[TSRel > 0.] = 1.
            TS1[TSRel < 0.] = -1.

            self.logger.info('[STEP 3 - 2 - 2] SIGNAL 4. SHORT TERM MOMENTUM PERCENTAGE OF UP DAY')
            up = ret.applymap(lambda x: 1 if x > 0 else 0)
            conroll = up.rolling(longlen2 - shortlen).sum() / (longlen2 - shortlen)
            conroll = conroll.iloc[longlen2 - shortlen - 1:]
            RV = conroll.iloc[:-1 * shortlen]
            RV.index = Mag.index
            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            # 1. Cross sectional
            CSRV = (RV1).rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSConpos = CSRVpos
            TSRel = RV.iloc[minobs1 - 1:]
            TS2 = TSRel.copy() * 0.
            TS2[TSRel > 0.5] = 1.
            TS2[TSRel < 0.5] = -1.

            self.logger.info('[STEP 3 - 3 - 1] SIGNAL 5. SHORT TERM  REVERSAL(ONLY CS)')
            STRV = (-1) * index.pct_change(4).iloc[4:]
            RV = STRV.iloc[52 - 1:]
            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            # 1. Cross sectional
            CSRV = (RV1).rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSREVpos = CSRVpos

            STRV = (-1) * index.pct_change(1).iloc[1:]
            RV = STRV.iloc[52 - 1:]
            RV1 = RV.iloc[minobs1 - 1:]
            truecount = (RV1.notnull().sum(axis=1) * CS).apply(round)

            # 1. Cross sectional
            CSRV = (RV1).rank(axis=1, method='first')
            CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

            CSRVpos = CSRV.fillna(0) * 0
            CSRVpos[CSRV.apply(lambda x: x <= truecount, axis=0)] = -1
            CSRVpos[CSRV1.apply(lambda x: x <= truecount, axis=0)] = 1
            CSREV2pos = CSRVpos
            TSRVSh = TS1 * 1 / 3 + TS2 * 1 / 3
            CSRVSh = CSREVpos * 1 / 2 + CSREV2pos.iloc[3:]
            TSRV = TSRVSh.loc[TSRVL.index] * 1 / 2 + TSRVL * 1
            CSRV = CSRVSh.copy()
            TSRV = TSRV.loc[CSRV.index]

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
    epm = EPM(strategy_name="EPM", asset_type="EQUITY")
    epm.load_index_and_return(from_db=False, save_file=False)
    epm.load_strategy_data(table='datastream', origin='EPS')

    epm.set_rebalance_period(ts_freq='week', cs_freq='month',
                             rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
    epm.calculate_signal(minobs1=52, longlen=52, longlen2=13, shortlen=2, cs=0.35)
    epm.set_portfolio_parameter(cs_strategy_type='notional')
    epm.make_portfolio()

    tester = Tester(epm)
    tester.set_period(start='1995-01-01', end='2018-06-30')
    tester.run()
    tester.plot_result()