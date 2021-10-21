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


class ESS(EquityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)
        self.eps = None

    def load_strategy_data(self, table='datastream', origin='EPS'):
        self.eps = self._load_strategy_data(table=table, origin=origin)

    def calculate_signal(self, short=0.2, day1=24, CS=0.35, statwgt=1, fundwgt=1):
        for type in range(2):
            if type == 0:
                RET = self.ret.loc[:, ['SPX', 'TSX', 'FTSE', 'DAX', 'CAC', 'SMI', 'MIB', 'IBEX', 'OMX', 'AEX']]
            else:
                RET = self.ret.loc[:, ['NKY', 'AS51', 'HSI', "SG"]]
                short = 0.2
                
            Ret = RET.copy()
            index = self.index.loc[:, Ret.columns]

            TOM = (RET.index + pd.tseries.offsets.BDay(1)).day
            # print('t1: ', len(TOM))
            # initially short instead of 0.2
            TOM2 = pd.DataFrame([-short] * len(TOM), index=RET.index)
            # print('t2:', TOM2)
            TOM2[TOM >= day1] = 1

            SIG = RET * 0 + 1
            SIG = pd.DataFrame(SIG.values * TOM2.values, index=TOM2.index, columns=RET.columns)
            
            # print('sig: ', SIG)

            TSRV1 = SIG * fundwgt  # +SIG2

            short = 0
            CSRV = index * 0
            statday = (RET.index + pd.tseries.offsets.DateOffset(months=1)).strftime('%Y-%m').unique()

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
            # print('bible: ', bible)
            
            RV1 = bible.iloc[:, :len(index.columns)]
#             truecount = np.round(1 - np.sum(np.isnan(RV1), axis=1) * CS)
            truecount = np.round(RV1.notnull().sum(axis=1)*CS)
            truecount = np.transpose(np.tile(truecount, (len(RV1.columns), 1)))

            # print('truecount: ' ,truecount)
            # lower value lower rank
            bibleRV = RV1.rank(axis=1, method='first')
            # print('bibleRV: ', bibleRV)
            bibleRV1 = -1 * bibleRV.sub(RV1.count(axis=1), axis=0) + 1
            #             bibleRV1 = (-RV1).rank(axis=1, method='first').T
            #             bibleRV1 = (RV1.count(1).T + 1 - RV1.T).T
            #             bibleRV1 = bibleRV1.T
            # print('RV1: ', bibleRV1)

            bibleRVpos = bibleRV * 0
            bibleRVpos[bibleRV <= truecount] = -1
            bibleRVpos[bibleRV1 <= truecount] = 1

            # print('shapes:', CSRV.shape, bibleRVpos.shape)
            # print(bibleRVpos)
            for i in range(len(bible)):
                CSRV_ym = (pd.to_datetime(bible_idx[i]) - pd.tseries.offsets.DateOffset(months=1)).strftime('%Y-%m')
                #                 print('ym: ', CSRV_ym)
                CSRV_range = CSRV[CSRV.index.strftime('%Y-%m') == CSRV_ym]

#                 CSRV[CSRV.index.strftime('%Y-%m') == CSRV_ym] = 
#                 [bibleRVpos.iloc[i, :] * statwgt] * CSRV_range.shape[0]
                CSRV[CSRV.index.strftime('%Y-%m') == CSRV_ym] = np.tile(bibleRVpos.iloc[i].values * statwgt, (CSRV_range.shape[0], 1))

            # print(CSRV)

            bibleTS1 = bible
            bibleTS = bibleTS1 * 0
            bibleTS[bibleTS1 < (-0.5)] = -1
            bibleTS[bibleTS1 > (0.5)] = 1

            TSRV = CSRV * 0

            for i in range(len(bible)):
                TSRV_ym = (pd.to_datetime(bible_idx[i])).strftime('%Y-%m')
                TSRV_range = TSRV[TSRV.index.strftime('%Y-%m') == TSRV_ym]
                #                 print(TSRV.index)
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
                # print('tsrvrun2: ', TSRV1)
                CSRVrun2 = CSRV
            
        TSRV = pd.concat([TSRVrun1, TSRVrun2], axis=1)
        CSRV = pd.concat([CSRVrun1, CSRVrun2], axis=1)

        TSRV = TSRV[self.ret.columns]
        CSRV = CSRV[self.ret.columns]
#         print('track csrv: ', CSRV.tail())
        
        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all').fillna(0)
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all').fillna(0)
#         print('track csrv2: ', self.CSRV.tail())
        # Align dates with each other
        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]
#         print('track csrv3: ', self.CSRV.tail())

        
if __name__ == "__main__":
    strategy = ESS(strategy_name="ESS", asset_type="EQUITY")
    strategy.load_index_and_return(from_db=False, save_file=False)
    strategy.set_rebalance_period(ts_freq='day', cs_freq = 'month')
    strategy.calculate_signal(short=0.2, day1=24, CS=0.35, statwgt=1, fundwgt=1)
    strategy.set_portfolio_parameter(cs_strategy_type='notional')
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1995-01-01', end='2018-06-30')
    tester.run()
    tester.plot_result()