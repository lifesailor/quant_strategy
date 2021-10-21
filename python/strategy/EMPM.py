import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from strategy import EmergingStrategy
from tester import Tester


base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
bloom_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


class EMPM(EmergingStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def calculate_signal(self, CS=0.35, minobs1=52, minobs=260, longlen1=52, longlen2=13, shortlen=2, IDN = 'out'):
        # weekday R에서 2를 더해야 같은 값
        group_1 = ['CN', 'KR', 'TW', 'IN']
        group_2 = ['MY', 'ID', 'BR', 'MX', 'RU', 'SA']

        if IDN == 'out':
            group_2.remove('ID')

        t = list(map(lambda x: True if x.weekday() == self.rebalance_weekday else False, self.index.index))
        index = self.index[t]
        ret = index.pct_change().dropna()

        up = ret * 0
        up[ret >= 0] = 1

        obs = len(index) - longlen1 - 1
        Mag = index.pct_change(longlen1 - shortlen).shift(shortlen).dropna().iloc[:obs + 1]
        Conroll = (up.rolling(longlen1 - shortlen).sum() / (longlen1 - shortlen)).shift(shortlen).dropna()

        obs = len(index) - longlen2 - 1
        Mag_short = index.pct_change(longlen2 - shortlen).shift(shortlen).dropna().iloc[:obs + 1]
        Conroll_short = (up.rolling(longlen2 - shortlen).sum() / (longlen2 - shortlen)).shift(shortlen).dropna()

        strv = -index.pct_change(4).dropna()
        strv_short = -ret.copy()

        CSRV1, TSRV1 = self.empm_csrv_tsrv(Mag,
                                      Conroll,
                                      Mag_short,
                                      Conroll_short,
                                      strv,
                                      strv_short, group_1, CS, minobs=minobs, minobs1=minobs1)
        CSRV2, TSRV2 = self.empm_csrv_tsrv(Mag,
                                      Conroll,
                                      Mag_short,
                                      Conroll_short,
                                      strv,
                                      strv_short, group_2, CS, minobs=minobs, minobs1=minobs1)

        TSRV = pd.concat([TSRV1, TSRV2], 1).reindex(columns=self.ret.columns).fillna(0)
        CSRV = pd.concat([CSRV1, CSRV2], 1).reindex(columns=self.ret.columns).fillna(0)

        TSRV = TSRV[self.ret.columns]
        CSRV = CSRV[self.ret.columns]

        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')

        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]

    def empm_csrv_tsrv(self, Mag, Conroll, Mag_short, Conroll_short, strv, strv_short, use_cols,
                             CS=0.35, minobs=260, minobs1=52):
        WGT = [1/3, 1/3, 1/3]
        WGT2 = [1/3, 1/3, 0]
        Mag = Mag.copy()[use_cols]
        Conroll = Conroll.copy()[use_cols]
        Mag_short = Mag_short.copy()[use_cols]
        Conroll_short = Conroll_short.copy()[use_cols]
        strv = strv.copy()[use_cols]
        strv_short = strv_short.copy()[use_cols]

        # long_momentum
        RV = Mag
        RVrank = RV.iloc[minobs - 1:] * 0

        RV1 = RV.iloc[minobs1 - 1:]

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        CSRV = RV1.rank(1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        Relrank = RVrank
        CSRelpos = CSRVpos
        TSRel = RV.iloc[minobs1 - 1:]

        TS1 = TSRel * 0

        TS1[TSRel > 0] = 1
        TS1[TSRel < 0] = -1
        # long
        RV = Conroll

        RVrank = RV.iloc[minobs - 1:] * 0

        RV1 = RV.iloc[minobs1 - 1:]

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        CSRV = RV1.rank(1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        Relrank = RVrank
        CSConpos = CSRVpos
        TSRel = RV.iloc[minobs1 - 1:]

        TS2 = TSRel * 0

        TS2[TSRel > 0.5] = 1
        TS2[TSRel < 0.5] = -1

        TSRVL = TS1 * WGT[0] + TS2 * WGT[2]

        CSRVL = CSRelpos * WGT[0] + CSConpos * WGT2[1]

        ## Short momentum
        RV = Mag_short

        RVrank = RV.iloc[minobs - 1:] * 0

        RV1 = RV.iloc[minobs1 - 1:]

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        CSRV = RV1.rank(1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        Relrank = RVrank
        CSRelpos = CSRVpos
        TSRel = RV.iloc[minobs1 - 1:]

        TS1 = TSRel * 0

        TS1[TSRel > 0] = 1
        TS1[TSRel < 0] = -1

        # short
        RV = Conroll_short

        RVrank = RV.iloc[minobs - 1:] * 0

        RV1 = RV.iloc[minobs1 - 1:]

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        CSRV = RV1.rank(1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        Relrank = RVrank
        CSConpos = CSRVpos
        TSRel = RV.iloc[minobs1 - 1:]

        TS2 = TSRel * 0

        TS2[TSRel > 0.5] = 1
        TS2[TSRel < 0.5] = -1
        # long momentum
        RV = strv.iloc[52 - 1:]

        RVrank = RV.iloc[minobs - 1:] * 0

        RV1 = RV.iloc[minobs1 - 1:]

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        CSRV = RV1.rank(1, method='first')
        #     CSRV1 = RV1.rank(1, method = 'first', ascending = False)
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T
        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        Relrank = RVrank
        CSREVpos = CSRVpos
        # short momentum

        RV = strv_short.iloc[52 - 1:]

        RVrank = RV.iloc[minobs - 1:] * 0

        RV1 = RV.iloc[minobs1 - 1:]

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        CSRV = RV1.rank(1, method='first')
        #     CSRV1 = RV1.rank(1, method = 'first', ascending = False)
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        Relrank = RVrank
        CSREV2pos = CSRVpos

        TSRVSh = TS1 * WGT[0] + TS2 * WGT[1]

        CSRVSh = CSREVpos * 0.5 + CSREV2pos.reindex(CSREVpos.index)

        TSRV = TSRVSh.reindex(TSRVL.index) * 0.5 + TSRVL

        CSRV = CSRVSh

        TSRV = TSRV.reindex(CSRVSh.index)
        return CSRV, TSRV


if __name__ == "__main__":
    strategy = EMPM(strategy_name="EMPM", asset_type="EMERGING")
    strategy.load_index_and_return(from_db=False, save_file=False)
    strategy.set_rebalance_period(freq='month', rebalance_weekday=3)  # rebalance_day: monday = 0, sunday = 6
    strategy.calculate_signal(CS=0.35, minobs=260, minobs1=52, longlen1=52, longlen2=13, shortlen=2)
    strategy.set_portfolio_parameter(cs_strategy_type="notional")
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1991-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()