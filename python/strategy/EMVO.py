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


class EMVO(EmergingStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_strategy_data2(self, table='bloom', origin='fx-em'):
        self.yield10 = self._load_strategy_data(table=table, origin=origin)

    def calculate_signal(self, CS=0.35, minobs1=12, minobs=60, longlen1=12, longlen2=3, shortlen=0, IDN = 'out'):
        # weekday R에서 2를 더해야 같은 값
        group_1 = ['CN', 'KR', 'TW', 'IN']
        group_2 = ['MY', 'ID', 'BR', 'MX', 'RU', 'SA']
        fx = self.fx
        if IDN == 'out':
            group_2.remove('ID')

        t = list(map(lambda x: True if x.weekday() == self.rebalance_weekday else False, self.index.index))
        index = self.index[t]
        ret = index.pct_change().dropna()

        up = ret * 0
        up[ret >= 0] = 1

        WGT = WGT2 = np.array([0.5, 0.5])

        # long term momentum
        obs = len(fx) - longlen1 - 1
        Mag = fx.pct_change(longlen1 - shortlen).shift(shortlen).dropna().iloc[:obs + 1]

        # shor term momentum
        obs = len(fx) - longlen2 - 1
        # longlen- shortlen의 차이만큼 모멘텀,
        Mag_short = fx.pct_change(longlen2 - shortlen).shift(shortlen).dropna().iloc[:obs + 1]

        TSRV1, CSRV1 = self.emvo_csrv_tsrv(Mag, Mag_short, use_cols=group_1, CS=CS, WGT=WGT, WGT2=WGT)
        TSRV2, CSRV2 = self.emvo_csrv_tsrv(Mag, Mag_short, use_cols=group_2, CS=CS, WGT=WGT, WGT2=WGT)

        TSRV = pd.concat([TSRV1, TSRV2], 1).reindex(columns=self.ret.columns).fillna(0)
        CSRV = pd.concat([CSRV1, CSRV2], 1).reindex(columns=self.ret.columns).fillna(0)

        self.TSRV = TSRV.reindex(self.ret.index).fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.reindex(self.ret.index).fillna(method='ffill').dropna(how='all')

        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]

    def emvo_csrv_tsrv(self, Mag, Mag_short, use_cols,
                             CS=0.35, minobs = 60, minobs1=12, nopos = 0.4):
        WGT = WGT2 = np.array([0.5, 0.5])

        Mag = Mag.copy()[use_cols]
        Mag_short = Mag_short.copy()[use_cols]
        RET = self.ret.copy()[use_cols]

        Ret = RET.copy()

        RV = Mag
        RVrank = RV.iloc[minobs1 - 1:] * 0

        for i in range(minobs - minobs1):
            data = RV.iloc[:(minobs1 + i)].rank(pct=True)
            RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

        for i in range(minobs - minobs1, len(RV) - minobs1 + 1):
            data = RV.iloc[i - (minobs - minobs1): (minobs1 + i)].rank(pct=True)
            RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

        truecount = np.round((RVrank.shape[-1] - RVrank.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RVrank.columns)).reshape(RVrank.shape)

        tiebreaker = RVrank.rolling(5).mean() * 0.0000001
        tiebreaker.iloc[:4] = 0

        tied_RVrank = RVrank + tiebreaker

        CSRV = tied_RVrank.rank(1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T
        #     CSRV1 = tied_RVrank.rank(1, method = 'first', ascending = False)

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        Magrank = RVrank
        CSMagpos = CSRVpos

        RV = Mag_short

        RVrank = RV.iloc[minobs1 - 1:] * 0

        for i in range(minobs - minobs1):
            data = RV.iloc[:(minobs1 + i)].rank(pct=True)
            RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

        for i in range(minobs - minobs1, len(RV) - minobs1 + 1):
            data = RV.iloc[i - (minobs - minobs1): (minobs1 + i)].rank(pct=True)
            RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]
        RV1 = RV.iloc[minobs1 - 1:]

        truecount = np.round((RVrank.shape[-1] - RVrank.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RVrank.columns)).reshape(RVrank.shape)

        tiebreaker = RVrank.rolling(5).mean() * 0.0000001
        tiebreaker.iloc[:4] = 0

        tied_RVrank = RVrank + tiebreaker

        CSRV = tied_RVrank.rank(1, method='first')
        #     CSRV1 = tied_RVrank.rank(1, method = 'first', ascending = False)
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        Relrank = RVrank
        CSRelpos = CSRVpos

        CS_index = sorted(list(set(CSRelpos.index) & set(CSMagpos.index)))

        CSMagpos = CSMagpos.reindex(CS_index)
        CSRelpos = CSRelpos.reindex(CS_index)

        CSRV = CSRelpos * WGT2[0] + CSMagpos * WGT2[1]

        # translate to positions
        TS1 = Magrank * 0

        TS1[Magrank > (nopos + (1 - nopos) / 2)] = 1
        TS1[Magrank < ((1 - nopos) / 2)] = -1

        TS2 = Relrank * 0

        TS2[Relrank > (nopos + (1 - nopos) / 2)] = 1
        TS2[Relrank < ((1 - nopos) / 2)] = -1

        TS_index = sorted(list(set(TS1.index) & set(TS2.index)))

        TS1 = TS1.reindex(TS_index)
        TS2 = TS2.reindex(TS_index)

        TSRV = TS1 * WGT[0] + TS2 * WGT[1]

        # TSRVstat1, CSRVstat1 = set_dates(TSRV, CSRV, RET)
        TSRVstat1 = TSRV.reindex(RET.index).fillna(method='ffill').dropna(how='all')
        CSRVstat1 = CSRV.reindex(RET.index).fillna(method='ffill').dropna(how='all')
        Ret = RET.copy()

        Retpos = Ret.copy()
        Retneg = Ret.copy()

        Retpos = Retpos[Retpos >= 0]
        Retneg = Retneg[Retneg <= 0]

        per = 63

        STDpos = Retpos.rolling(per, min_periods=1).std().iloc[per - 1:]

        STDneg = Retneg.rolling(per, min_periods=1).std().iloc[per - 1:]

        RV = - STDpos + STDneg

        t = list(map(lambda x: True if x.weekday() == self.rebalance_weekday else False, RV.index))  # weekday R에서 2를 더해야 같은 값

        RV = RV[t]

        RVrank = RV.iloc[minobs1 - 1:] * 0

        for i in range(len(RV) - minobs1 + 1):
            data = RV.iloc[:(minobs1 + i)].rank(pct=True)
            RVrank.iloc[i] = data.iloc[-1][(len(data) - data.isna().sum()) >= minobs1]

        truecount = np.round((RVrank.shape[-1] - RVrank.isna().sum(1)) * CS)

        truecount = np.repeat(truecount.values, len(RVrank.columns)).reshape(RVrank.shape)

        tiebreaker = RVrank.rolling(5).mean() * 0.0000001
        tiebreaker.iloc[:4] = 0
        tied_RVrank = RVrank + tiebreaker

        CSRV = tied_RVrank.rank(1, method='first')
        CSRV1 = (CSRV.count(1).T + 1 - CSRV.T).T
        #     CSRV1 = tied_RVrank.rank(1, method = 'first', ascending = False)

        CSRVpos = CSRV * 0

        CSRVpos[CSRV <= truecount] = -1
        CSRVpos[CSRV1 <= truecount] = 1

        CSRV = CSRVpos
        CSRV.fillna(0, inplace=True)
        # TSRV
        TSRV = RVrank * 0

        TSRV[RVrank > (nopos + (1 - nopos) / 2)] = 1
        TSRV[RVrank < ((1 - nopos) / 2)] = -1

        # TSRVstat2, CSRVstat2 = set_dates(TSRV, CSRV, RET)
        TSRVstat2 = TSRV.reindex(RET.index).fillna(method='ffill').dropna(how='all')
        CSRVstat2 = CSRV.reindex(RET.index).fillna(method='ffill').dropna(how='all')
        new_index = sorted(list(set(TSRVstat1.index) & set(TSRVstat2.index)))

        TSRVstat1 = TSRVstat1.reindex(new_index)
        TSRVstat2 = TSRVstat2.reindex(new_index)
        CSRVstat1 = CSRVstat1.reindex(new_index)
        CSRVstat2 = CSRVstat2.reindex(new_index)

        TSRV = TSRVstat1 * 1 + TSRVstat2
        CSRV = CSRVstat1 * 1 + CSRVstat2

        return CSRV, TSRV


if __name__ == "__main__":
    strategy = EMVO(strategy_name="EMVO", asset_type="EMERGING")
    strategy.load_index_and_return(from_db=False, save_file=False)
    strategy.set_rebalance_period(freq='month', rebalance_weekday=3)  # rebalance_day: monday = 0, sunday = 6
    strategy.calculate_signal(cs=0.35, minobs=260, minobs1=52, longlen1=52, longlen2=13, shortlen=2)
    strategy.set_portfolio_parameter(cs_strategy_type="notional")
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1991-01-01', end='2018-05-09')
    tester.run()
    tester.plot_result()