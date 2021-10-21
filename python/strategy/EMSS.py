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


class EMSS(EmergingStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def calculate_signal(self, CS=0.35, short=0.2, day1=24, fundwgt=1, statwgt=1, IDN='out'):
        group_1 = ['CN', 'KR', 'TW', 'IN']
        group_2 = ['MY', 'ID', 'BR', 'MX', 'RU', 'SA']

        if IDN == 'out':
            group_2.remove('ID')

        for type in range(2):
            if type == 0:
                RET = self.ret.loc[:, group_1]
                index = self.index.loc[:, RET.columns]
            else:
                RET = self.ret.loc[:, group_2]
                index = self.index.loc[:, RET.columns]

            Ret = RET
            TOM = (RET.index + pd.tseries.offsets.BDay(1)).day
            TOM2 = pd.DataFrame([-short] * len(TOM), index=Ret.index)
            TOM2[TOM >= day1] = 1
            SIG = Ret * 0 + 1
            SIG = pd.DataFrame(SIG.values * TOM2.values, index=TOM2.index, columns=Ret.columns)
            TSRV1 = SIG * fundwgt

            short = 0
            CSRV = index * 0

            statday = (Ret.index + pd.tseries.offsets.DateOffset(months=1)).strftime('%Y-%m').unique()
            bible_idx = statday[36 - short:len(statday) - short]
            bible_ls = []

            for i in range(len(bible_idx)):
                ave = np.mean(Ret[(Ret.index < bible_idx[i]) & (Ret.index >= statday[i]) & (
                            Ret.index.month != pd.to_datetime(bible_idx[i]).month)], axis=0)
                ave = ave.iloc[:len(index.columns)]
                bible_temp = np.mean(Ret[(Ret.index < bible_idx[i]) & (Ret.index >= statday[i]) & (
                            Ret.index.month == pd.to_datetime(bible_idx[i]).month)], axis=0) / \
                             np.std(Ret[(Ret.index < bible_idx[i]) & (Ret.index >= statday[i]) & (
                                         Ret.index.month == pd.to_datetime(bible_idx[i]).month)], axis=0)
                bible_ls.append(bible_temp.iloc[:len(index.columns)])

            bible = pd.DataFrame(bible_ls, index=bible_idx)
            RV1 = bible.iloc[:, :len(index.columns)]

            truecount = np.round(RV1.notnull().sum(axis=1)*CS)
            truecount = np.transpose(np.tile(truecount, (len(RV1.columns), 1)))

            bibleRV = RV1.rank(axis=1, method='first')
            bibleRV1 = -1 * bibleRV.sub(RV1.count(axis=1), axis=0) + 1
            bibleRVpos = bibleRV * 0
            bibleRVpos[bibleRV <= truecount] = -1
            bibleRVpos[bibleRV1 <= truecount] = 1

            for i in range(len(bible) - 1):
                CSRV_ym = (pd.to_datetime(bible_idx[i]) - pd.tseries.offsets.DateOffset(months=1)).strftime('%Y-%m')
                CSRV_range = CSRV[CSRV.index.strftime('%Y-%m') == CSRV_ym]
                CSRV[CSRV.index.strftime('%Y-%m') == CSRV_ym] = np.tile(bibleRVpos.iloc[i].values * statwgt, (CSRV_range.shape[0], 1))

            bibleTS1 = bible
            bibleTS = bibleTS1 * 0
            bibleTS[bibleTS1 < (-0.5)] = -1
            bibleTS[bibleTS1 > (0.5)] = 1
            TSRV = CSRV * 0

            for i in range(len(bible)):
                TSRV_ym = (pd.to_datetime(bible_idx[i]) - pd.tseries.offsets.DateOffset(months=1)).strftime('%Y-%m')
                # TSRV_ym = (pd.to_datetime(bible_idx[i])).strftime('%Y-%m')
                TSRV_range = TSRV[TSRV.index.strftime('%Y-%m') == TSRV_ym]
                TSRV[TSRV.index.strftime('%Y-%m') == TSRV_ym] = [bibleTS.iloc[i, :]] * TSRV_range.shape[0]

            AAA = [x for x in TSRV1 if x in TSRV]
            TSRV1 = TSRV1.loc[:, AAA]
            TSRV = TSRV.loc[:, AAA]

            TSRV = TSRV1 * 1 + TSRV * 0

            if type == 0:
                TSRVrun1 = TSRV
                CSRVrun1 = CSRV
            else:
                TSRVrun2 = TSRV
                CSRVrun2 = CSRV

        TSRV = pd.concat([TSRVrun1, TSRVrun2], 1).reindex(columns=self.ret.columns).fillna(0)
        CSRV = pd.concat([CSRVrun1, CSRVrun2], 1).reindex(columns=self.ret.columns).fillna(0)

        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all').fillna(0) # add fillna(0) for 'ID'
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all').fillna(0) # add fillna(0) for 'ID'

        # Align dates with each other
        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]


if __name__ == "__main__":
    # EM equity data
    emindex = pd.read_csv(os.path.join(data_path, 'totindex-em.csv'), header=0, index_col=0, parse_dates=True)
    emindex1 = pd.read_csv(os.path.join(data_path, 'priceindex-em.csv'), header=0, index_col=0, parse_dates=True)
    emfut = pd.read_csv(os.path.join(data_path, 'fut1return-em.CSV'), header=0, index_col=0, parse_dates=True)

    total_em_ret = emindex.pct_change()
    prc_em_ret = emindex1.pct_change()
    fut_em_ret = emfut.pct_change()
    fut_em_ret = fut_em_ret.reindex(pd.date_range(fut_em_ret.index[0], fut_em_ret.index[-1], freq='B'))  # bizday로 변환

    prc_em_ret.loc[total_em_ret.index] = total_em_ret
    raw_index = pd.concat([emindex1.loc[:'2012-12-31'], emfut.loc['2013-01-01':]]).iloc[1:]
    EMRet = pd.concat([prc_em_ret.loc[:'2012-12-31'], fut_em_ret.loc['2013-01-01':]]).iloc[1:]

    EMRet.fillna(0, inplace=True)
    EMindex = EMRet
    EMindex.iloc[0] = 0
    EMindex = (1 + EMindex).cumprod()

    raw_index.index.name = 'tdate'
    raw_index.columns.name = 'ticker'

    EMindex.index.name = 'tdate'
    EMRet.index.name = 'tdate'

    EMindex.columns.name = 'ticker'
    EMRet.columns.name = 'ticker'

    raw_index = pd.read_csv(os.path.join(data_path, 'priceindex-mon-em.csv'), header=0, index_col=0, parse_dates=True)
    raw_index.index.name = 'tdate'
    raw_index.columns.name = 'ticker'

    emss = EMSS(strategy_name="EMSS", asset_type="EMERGING")
    emss.index = EMindex.copy()
    emss.ret = EMRet.copy()

    # emss.load_index_and_return(from_db=False, save_file=False)
    emss.set_rebalance_period(ts_freq='month', cs_freq='month')  # rebalance_day: monday = 0, sunday = 6
    emss.calculate_signal(CS=0.35, short=0.2, day1=24, fundwgt=1, statwgt=1)
    emss.set_portfolio_parameter(cs_strategy_type="notional")
    emss.make_portfolio()

    tester = Tester(emss)
    tester.set_period(start=start_date, end=end_date)
    tester.run(save_file=False)

    # strategy = EMSS(strategy_name="EMSS", asset_type="EMERGING")
    # strategy.load_index_and_return(from_db=False, save_file=False)
    # strategy.set_rebalance_period(freq='month')  # rebalance_day: monday = 0, sunday = 6
    # strategy.calculate_signal(CS=0.35, short=0.2, day1=24, fundwgt=1, statwgt=1)
    # strategy.set_portfolio_parameter(cs_strategy_type="notional")
    # strategy.make_portfolio()
    #
    # tester = Tester(strategy)
    # tester.set_period(start='1991-01-01', end='2018-05-09')
    # tester.run()
    # tester.plot_result()