import os
import sys
base_path = os.path.abspath('../..')
data_path = os.path.join(base_path, 'data')
train_data_path = os.path.join(os.path.abspath('.'), 'data')
database_path = os.path.join(data_path, 'database')
strategy_path = os.path.join(base_path, 'strategy')
sys.path.append(base_path)


import pandas as pd
import numpy as np

from strategy.strategy import CommodityStrategy, EquityStrategy, IRStrategy, EmergingStrategy
from tester import Tester
from feature_data_gen import GrpEquityFeature
class Data_gen:
    def __init__(self, asset = 'E', freq='W-TUE', save_file = True, load_file = True):
        self.freq = freq
        #local file 이 True면 특정 경로에 필요한 pickle 파일 만듬, 있으면 불러냄
        self.save_file = save_file
        self.load_file = load_file
        self.asset = asset
        # 기본 학습데이터가 들어갈 경로, 없으면 만듬
        if save_file:
            if not os.path.exists(train_data_path):
                os.mkdir(train_data_path)
        if self.load_file:
            if not os.path.exists(os.path.join(train_data_path, 'ret_data_{}.pkl'.format(self.asset))):
                print('file do not exist: {}'.format(train_data_path))
                self.load_file = False
        if not self.load_file:
            if asset == 'E':
                self.ret_data_loader = EquityStrategy(strategy_name = 'EPM', asset_type = 'EQUITY')
                self.feature_loader = GrpEquityFeature
            elif asset == 'EM':
                self.ret_data_loader = EmergingStrategy(strategy_name = "EMPM", asset_type = 'EMERGING')
            elif asset == 'C':
                self.ret_data_loader = CommodityStrategy(strategy_name = 'CPM', asset_type = 'COMMODITY')
            elif asset == 'I':
                self.ret_data_loader = IRStrategy(strategy_name = 'IPM', asset_type = 'IR')


        self.load_ret_data()

        self.ref_dates = [5, 10, 21, 42, 63, 126, 252]
    def concat_old_data(self, old_data, new_data):
        return old_data.loc[:new_data.index[0]].iloc[:-1].append(new_data)

    def load_feature_data(self):
        if self.load_file:
            feature_path = os.path.join(train_data_path, 'feature_data_{}.pkl'.format(self.asset))
            rv_path = os.path.join(train_data_path, 'rv_data_{}.pkl'.format(self.asset))
            feature_data = pd.read_pickle(feature_path)
            rv_data = pd.read_pickle(rv_path)

        else:
            feature = self.feature_loader()
            feature_data, rv_data = feature.load_data_async()

            if self.save_file:
                feature_data.to_pickle(os.path.join(train_data_path, 'feature_data_{}.pkl'.format(self.asset)))
                rv_data.to_pickle(os.path.join(train_data_path, 'rv_data_{}.pkl'.format(self.asset)))
        return feature_data, feature_data.columns.levels[0], rv_data, rv_data.columns.levels[0]

    def load_ret_data(self):
        if self.load_file:
            self.ret_data = pd.read_pickle(os.path.join(train_data_path, 'ret_data_{}.pkl'.format(self.asset)))
            self.index = (self.ret_data + 1).cumprod()
        else:
            self.ret_data_loader.load_index_and_return(from_db=True, save_file=False)

            self.ret_data_loader.index.columns.name = 'ticker'
            self.ret_data_loader.ret.columns.name = 'ticker'

            self.ret_data_loader.index.index.name = 'tdate'
            self.ret_data_loader.ret.index.name = 'tdate'

            self.ret_data_loader.index.drop_duplicates(inplace=True)
            self.ret_data_loader.ret.drop_duplicates(inplace=True)

            df_index = self.ret_data_loader.index

            self.ret_data_ = df_index.reindex(pd.date_range(df_index.index[0], df_index.index[-1])).ffill().pct_change()
            # cap filtering
            self.ret_data_.index = pd.to_datetime(self.ret_data_.index.astype(str))

            self.index = (self.ret_data_.fillna(0) + 1).cumprod()  # 수익률로 부터 주가지수 생성
            self.index.index = pd.to_datetime(self.index.index)
            self.ret_data = self.index.pct_change()
            if self.save_file:
                self.ret_data.to_pickle(os.path.join(train_data_path, 'ret_data_{}.pkl'.format(self.asset)))
                
    def load_ret_1w(self):
        week_index = self.index.resample(self.freq).last()
        # 익월 수익률
        ret_1w = week_index.pct_change(1).shift(-1)
        if self.save_file:
                self.ret_data.to_pickle(os.path.join(train_data_path, 'ret_1w_{}.pkl'.format(self.asset)))
        return ret_1w

    #         old_data = pd.read_pickle(os.path.join(data_path, 'ret_1w.pkl'))
    #         concat_data = self.concat_old_data(old_data, ret_1m)

    #         concat_data.to_pickle(os.path.join(data_path, 'ret_1w_{}.pkl'.format(self.asset)))
    def load_target_data(self):
        ret_1w = self.load_ret_1w()
        target = ret_1w.rank(1, method='first').apply(
            lambda x: pd.qcut(x, 2, labels=False) if not x.isnull().all() else x, 1)

        target2 = ret_1w.rank(1, method='first').apply(
            lambda x: pd.qcut(x, 3, labels=False) if not x.isnull().all() else x, 1).applymap(
            lambda x: 1 if x == 2 else 0 if x == 0 or x == 1 else np.nan)

        target3 = (ret_1w.T - ret_1w.T.median()).T

        targets = [target, target2, target3]

        target_data = {}
        for i in range(1, len(targets) + 1):
            target_data[f'target_{i}'] = targets[i - 1]
        target_data = pd.concat(target_data, 1)

        return ret_1w, target_data

    def load_price_data(self):
        if self.load_file:
            price_data = pd.read_pickle(os.path.join(train_data_path, 'price_data_{}.pkl'.format(self.asset)))
        else:
            index = self.index.resample('B').last()

            ret_data_ = self.index.pct_change()

            momentum_data = {}
            df = pd.DataFrame(index=index.index, columns=index.columns)
            for ref_date in self.ref_dates:
                momentum_data['ret_{}d'.format(ref_date)] = index.pct_change(ref_date, fill_method=None)
                momentum_data[u'vol_{}d'.format(ref_date)] = index.pct_change(1, fill_method=None).rolling(
                    window=ref_date).std()
                #     momentum_data['info_dis_{}d'.format(ref_date)] = rtn_sign*(-rtn_sign).rolling(ref_date).mean()/(rtn_sign.abs()).rolling(ref_date).mean()

                df.iloc[:, 0] = index.pct_change(ref_date, fill_method=None).median(1)
                momentum_data['mean_{}d'.format(ref_date)] = df.ffill(axis=1)

                df.iloc[:, 0] = index.pct_change(ref_date, fill_method=None).std(1)
                momentum_data['std_{}d'.format(ref_date)] = df.ffill(axis=1)

            momentum_data = pd.concat(momentum_data, 1)
            momentum_data.index = pd.to_datetime(momentum_data.index.astype(str))

            ma_data = {}
            for ref_date in self.ref_dates:
                ma_data['ma_{}d'.format(ref_date)] = (index / index.rolling(ref_date).mean())
            ma_data = pd.concat(ma_data, 1)
            ma_data.index = pd.to_datetime(ma_data.index.astype(str))

            ref_date = sorted([int(x.split('_')[1][:-1]) for x in ma_data.columns.levels[0]])[::2]

            diff_data = {}
            for i in range(len(ref_date)):
                for j in range(i):
                    diff_data['ma_diff_{}-{}'.format(ref_date[j], ref_date[i])] = ma_data['ma_{}d'.format(ref_date[j])] - \
                                                                                  ma_data['ma_{}d'.format(ref_date[i])]

            diff_data = pd.concat(diff_data, 1)

            #         ab_data = {}

            #         for ref_date in self.ref_dates[2:]:
            #             alpha = []
            #             beta = []
            #             dates = []
            #             for i in range(len(ret_data_) - ref_date + 1):
            #                 # 수익률 데이터
            #                 rtn_data = ret_data_.iloc[i: i + ref_date]
            #                 # 시장데이터
            #                 #     cal_alpha_beta = index_data.rank(pct = True).iloc[-1]
            #                 # alpha beta
            #                 market_rtn_data = self.kospi.loc[rtn_data.index].values

            #                 alpha_, beta_ = get_alpha_beta(market_rtn_data, rtn_data)

            #                 alpha.append(alpha_)
            #                 beta.append(beta_)

            #                 dates.append(ret_data_.index[i + ref_date - 1])
            #             alpha = pd.concat(alpha, 1).T
            #             alpha.index = dates
            #             beta = pd.concat(beta, 1).T
            #             beta.index = dates

            #             ab_data['alpha_{}d'.format(ref_date)] = alpha
            #             ab_data['beta_{}d'.format(ref_date)] = beta
            #         ab_data = pd.concat(ab_data, 1)
            #         ab_data.index = pd.to_datetime(ab_data.index.astype(str))

            price_data = pd.concat([momentum_data, ma_data, diff_data], 1).iloc[550:].resample(self.freq).last()
            if self.save_file:
                price_data.to_pickle(os.path.join(train_data_path, 'price_data_{}.pkl'.format(self.asset)))
        return price_data, price_data.columns.levels[0]

    def load_plus_data(self, model='N'):
        if self.load_file:
            plus_data = pd.read_pickle(os.path.join(train_data_path, 'plus_data_{}.pkl'.format(self.asset)))

        else:
            plus_data = {}

            quarter_data = self.ret_data.reset_index().melt('index').dropna()

            quarter_data['quarter'] = [x.quarter for x in quarter_data['index']]

            quarter_data.columns = ['td', 'code', 'value', 'quarter']

            quarter_data = quarter_data.pivot('td', 'code', 'quarter')
            plus_data['quarter'] = quarter_data
            plus_data = pd.concat(plus_data, 1).resample(self.freq).last()

            if model == 'N':
                # 더미 변수화
                plus_data = pd.get_dummies(plus_data.stack()['quarter'], 'quarter').unstack()

            if self.save_file:
                plus_data.to_pickle(os.path.join(train_data_path, 'plus_data_{}.pkl'.format(self.asset)))
        return plus_data, plus_data.columns.levels[0]


if __name__ == '__main__':
    a = Data_gen(asset = 'E', save_file = True)
    print(a.ret_data_loader)
    price_data, price_columns = a.load_price_data()
    plus_data, plus_columns = a.load_plus_data()
    ret_1w, target_data = a.load_target_data()

    print(price_data, price_columns)
    print(target_data)
