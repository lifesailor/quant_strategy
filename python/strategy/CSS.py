import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine


sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from strategy import CommodityStrategy
from tester import Tester


base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
bloom_path = os.path.join(data_path, 'bloom')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


# CS=0.35, obs=52, longlen=52, shortlen=35

class CSS(CommodityStrategy):
    def __init__(self, strategy_name, asset_type):
        super().__init__(strategy_name=strategy_name, asset_type=asset_type)

    def load_strategy_data1(self, table='bloom', origin1='fx', origin2='carry-com', origin3='carry-com2'):
        self.fx = self._load_strategy_data(table=table, origin=origin1)
        self.fx = self.fx.iloc[self.fx.reset_index().groupby(self.fx.index.to_period('M'))[self.fx.index.name].idxmax()]
        self.fx.index = pd.to_datetime(self.fx.index, format='%Y%m%d')
        self.carry_com = self._load_strategy_data(table=table, origin=origin2)
        self.carry_com = self.carry_com.resample("M").last()
        self.carry_com2 = self._load_strategy_data(table=table, origin=origin3)
        self.carry_com2 = self.carry_com2.resample("M").last()

    def load_strategy_data2(self, table='past', origin='seasonal'):
        query = "select * from grp_past where origin = :origin"
        param = {'origin': origin}
        fs = self.engine.execute(query, param)
        rows = [row for row in fs]
        columns = fs.keys()
        df = pd.DataFrame(rows, columns=columns)
        df = df[['tdate', 'ticker', 'value']]
        df.drop_duplicates(inplace=True)
        df_pivot = df.pivot(index='tdate', columns='ticker', values='value')
        df_pivot.index = pd.to_datetime(df_pivot.index)
        df_pivot = df_pivot.astype(np.float32)
        df_pivot.fillna(0, inplace=True)
        self.seasonal = df_pivot

    def calculate_signal(self, CS=0.35, obs=52, longlen=52, shortlen=35):
        """

        :param cs_num: percentage of position for cross sectional signal
        :param min_obs:
        :param longlen: long term price momentum period
        :param shortlen: short term price momentum period
        :return:
        """
        self.logger.info('[STEP 3] CACULATE SIGNAL')
        group_1 = CommodityStrategy.BLOOM_COMMODITY_WEATHER_GROUP
        group_2 = CommodityStrategy.BLOOM_COMMODITY_NOTWEATHER_GROUP

        CSRV1, TSRV1 = self.css_csrv_tsrv(use_col=group_1, CS=CS)
        CSRV2, TSRV2 = self.css_csrv_tsrv(use_col=group_2, CS=CS)

        TSRV = pd.concat([TSRV1, TSRV2], 1).reindex(columns=self.ret.columns).fillna(0)
        CSRV = pd.concat([CSRV1, CSRV2], 1).reindex(columns=self.ret.columns).fillna(0)
        self.TSRV = TSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')
        self.CSRV = CSRV.loc[self.ret.index].fillna(method='ffill').dropna(how='all')

        # Align dates with each other
        if self.TSRV.index[0] > self.CSRV.index[0]:
            self.CSRV = self.CSRV.loc[self.TSRV.index[0]:]
        else:
            self.TSRV = self.TSRV.loc[self.CSRV.index[0]:]

    def css_csrv_tsrv(self, use_col, CS=0.35):

        cret = self.ret.copy()[use_col]
        Ret = cret.copy()

        fx = self.fx
        seasonal = self.seasonal[cret.columns]
        carry = self.carry_com[cret.columns]

        TSpos = pd.DataFrame(index=fx.index, columns=Ret.columns).fillna(0)

        index = self.index.loc[:, Ret.columns]

        seasonal.fillna(0, inplace=True)
        seasonal['mon'] = pd.to_datetime(seasonal.index).month

        TSpos['mon'] = (pd.to_datetime(TSpos.index) + pd.tseries.offsets.BMonthBegin(1)).month

        TSpos = TSpos.apply(lambda x: seasonal.set_index('mon').loc[x['mon']], 1)

        TSRV1 = TSpos

        carry['YM'] = pd.to_datetime(carry.index).strftime('%Y-%m')
        carry['mon'] = pd.to_datetime(carry.index).month

        fx = fx.loc[carry.index].dropna(how='all')

        bible = pd.DataFrame(index=fx.iloc[60:].index, columns=cret.columns).fillna(0)

        statday1 = (bible.index + pd.tseries.offsets.DateOffset(months=3)).strftime('%Y-%m').unique()
        statday = (fx.index + pd.tseries.offsets.DateOffset(months=3)).strftime('%Y-%m').unique()

        bible['YM'] = statday1
        bible['mon'] = pd.to_datetime(statday1).month

        bible_idx = statday[60:len(statday)]
        bible_ls = []

        bible_ls = []

        # why compute ave??
        for i in range(len(bible)):
            #     ave = np.mean(carry[(carry['YM'] < bible['YM'][i]) & (carry['YM'] >= statday[i]) & (carry['mon'] != bible['mon'][i])])[cret.columns]
            b = np.mean(carry[(carry['YM'] < bible['YM'][i]) & (carry['YM'] >= statday[i]) & (
                        carry['mon'] == bible['mon'][i])])[cret.columns]
            bible_ls.append(b)

        bible_ls = pd.concat(bible_ls, 1).T

        bible_ls.index = bible.index

        RV1 = bible_ls

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        bibleRV = RV1.rank(1, method='first')
        bibleRV1 = (bibleRV.count(1).T + 1 - bibleRV.T).T

        CSRVpos = bibleRV * 0

        CSRVpos[bibleRV <= truecount] = -1
        CSRVpos[bibleRV1 <= truecount] = 1
        CSRVpos.index = fx.index[-len(CSRVpos):]

        CSRVori = CSRV = CSRVpos.copy()

        bibleTS1 = RV1
        bibleTS = bibleTS1 * 0
        bibleTS[bibleTS1 < 0] = -1
        bibleTS[bibleTS1 > 0] = 1
        bibleTS.index = fx.index[-len(CSRVpos):]
        TSRV2 = bibleTS.copy()

        bible = pd.DataFrame(index=fx.iloc[60:].index, columns=cret.columns).fillna(0)

        statday1 = (bible.index + pd.tseries.offsets.DateOffset(months=2)).strftime('%Y-%m').unique()
        statday = (fx.index + pd.tseries.offsets.DateOffset(months=2)).strftime('%Y-%m').unique()

        bible['YM'] = statday1
        bible['mon'] = pd.to_datetime(statday1).month

        bible_idx = statday[60:len(statday)]
        bible_ls = []

        # why compute ave??
        for i in range(len(bible)):
            #     ave = np.mean(carry[(carry['YM'] < bible['YM'][i]) & (carry['YM'] >= statday[i]) & (carry['mon'] != bible['mon'][i])])[cret.columns]
            b = np.mean(carry[(carry['YM'] < bible['YM'][i]) & (carry['YM'] >= statday[i]) & (
                        carry['mon'] == bible['mon'][i])])[cret.columns]
            bible_ls.append(b)

        bible_ls = pd.concat(bible_ls, 1).T

        bible_ls.index = bible.index

        fx = self.fx.copy()

        RV1 = bible_ls

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        bibleRV = RV1.rank(1, method='first')
        bibleRV1 = (bibleRV.count(1).T + 1 - bibleRV.T).T

        CSRVpos = bibleRV * 0

        CSRVpos[bibleRV <= truecount] = -1
        CSRVpos[bibleRV1 <= truecount] = 1
        CSRVpos.index = fx.index[-len(CSRVpos):]

        CSRV2 = CSRVpos.copy()

        bibleTS1 = RV1
        bibleTS = bibleTS1 * 0
        bibleTS[bibleTS1 < 0] = -1
        bibleTS[bibleTS1 > 0] = 1
        bibleTS.index = fx.index[-len(CSRVpos):]
        TSRV1 = bibleTS.copy()

        Ret['YM'] = pd.to_datetime(Ret.index).strftime('%Y-%m')
        Ret['mon'] = pd.to_datetime(Ret.index).month

        fx = self.fx
        fx = fx.reindex(Ret.index).dropna(how='all')

        bible = pd.DataFrame(index=fx.iloc[60:].index, columns=cret.columns).fillna(0)

        statday1 = (bible.index + pd.tseries.offsets.DateOffset(months=1)).strftime('%Y-%m').unique()
        statday = (fx.index + pd.tseries.offsets.DateOffset(months=1)).strftime('%Y-%m').unique()

        bible['YM'] = statday1
        bible['mon'] = pd.to_datetime(statday1).month

        bible_ls = []
        # why compute ave??
        for i in range(len(bible)):
            #     ave = np.mean(Ret[(Ret['YM'] < bible['YM'][i]) & (Ret['YM'] >= statday[i]) & (Ret['mon'] != bible['mon'][i])])[cret.columns]
            b = Ret[(Ret['YM'] < bible['YM'][i]) & (Ret['YM'] >= statday[0]) & (Ret['mon'] == bible['mon'][i])][
                cret.columns]
            bible_ls.append(np.mean(b) / np.std(b, ddof=1) * np.sqrt(len(b)))

        bible_ls = pd.concat(bible_ls, 1).T

        bible_ls.index = bible.index

        fx = self.fx

        RV1 = bible_ls

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        bibleRV = RV1.rank(1, method='first')
        bibleRV1 = (bibleRV.count(1).T + 1 - bibleRV.T).T

        CSRVpos = bibleRV * 0

        CSRVpos[bibleRV <= truecount] = -1
        CSRVpos[bibleRV1 <= truecount] = 1
        CSRVpos.index = fx.index[-len(CSRVpos):]

        CSRVcv = CSRVpos.copy()

        bibleTS1 = RV1
        bibleTS = bibleTS1 * 0
        bibleTS[bibleTS1 < 0] = -1
        bibleTS[bibleTS1 > 0] = 1
        bibleTS.index = fx.index[-len(CSRVpos):]
        TSRV4 = bibleTS.copy()

        fx = self.fx
        fx = fx.reindex(Ret.index).dropna(how='all')

        bible = pd.DataFrame(index=fx.iloc[60:].index, columns=cret.columns).fillna(0)

        statday1 = (bible.index + pd.tseries.offsets.DateOffset(months=2)).strftime('%Y-%m').unique()
        statday = (fx.index + pd.tseries.offsets.DateOffset(months=2)).strftime('%Y-%m').unique()

        bible['YM'] = statday1
        bible['mon'] = pd.to_datetime(statday1).month

        bible_ls = []
        # why compute ave??
        for i in range(len(bible)):
            #     ave = np.mean(Ret[(Ret['YM'] < bible['YM'][i]) & (Ret['YM'] >= statday[i]) & (Ret['mon'] != bible['mon'][i])])[cret.columns]
            b = Ret[(Ret['YM'] < bible['YM'][i]) & (Ret['YM'] >= statday[0]) & (Ret['mon'] == bible['mon'][i])][
                cret.columns]
            bible_ls.append(np.mean(b) / np.std(b, ddof=1) * np.sqrt(len(b)))

        bible_ls = pd.concat(bible_ls, 1).T

        bible_ls.index = bible.index

        fx = self.fx

        RV1 = bible_ls

        truecount = np.round((RV1.shape[-1] - RV1.isna().sum(1)) * CS)
        truecount = np.repeat(truecount.values, len(RV1.columns)).reshape(RV1.shape)

        bibleRV = RV1.rank(1, method='first')
        bibleRV1 = (bibleRV.count(1).T + 1 - bibleRV.T).T

        CSRVpos = bibleRV * 0

        CSRVpos[bibleRV <= truecount] = -1
        CSRVpos[bibleRV1 <= truecount] = 1
        CSRVpos.index = fx.index[-len(CSRVpos):]

        CSRVcv2 = CSRVpos.copy()

        bibleTS1 = RV1
        bibleTS = bibleTS1 * 0
        bibleTS[bibleTS1 < 0] = -1
        bibleTS[bibleTS1 > 0] = 1
        bibleTS.index = fx.index[-len(CSRVpos):]
        TSRV3 = bibleTS.copy()

        TSRV = TSRV1 + TSRV2 + TSRV3 + TSRV4
        TSRV.dropna(how='all', inplace=True)

        CSRV = CSRV + CSRV2 + CSRVcv + CSRVcv2
        CSRV.dropna(how='all', inplace=True)
        return CSRV, TSRV


if __name__ == "__main__":
    strategy = CSS(strategy_name="CSS", asset_type="COMMODITY")
    strategy.load_index_and_return(from_db=False, save_file=True)
    strategy.load_strategy_data1(table='bloom', origin1='fx', origin2='carry-com', origin3='carry-com2')
    strategy.load_strategy_data2(table='past', origin='seasonal')
    strategy.set_rebalance_period(freq='week', rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
    strategy.calculate_signal(CS=0.35)
    strategy.set_portfolio_parameter(cs_strategy_type='vol')
    strategy.make_portfolio()

    tester = Tester(strategy)
    tester.set_period(start='1991-01-01', end='2019-07-31')
    tester.run()
    tester.plot_result()