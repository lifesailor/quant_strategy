import os
import sys

base_path = os.path.abspath('../..')
current_path = os.path.abspath('.')
data_path = os.path.join(current_path, 'data')
model_path = os.path.join(current_path, 'models')
strategy_path = os.path.join(base_path, 'strategy')
util_path = os.path.join(base_path, 'utils')
sys.path.append(base_path)
sys.path.append(strategy_path)
sys.path.append(model_path)

from model_basic_parameter import NN_params_basic, NN_train_params_basic, LGBM_params_basic, LGBM_train_params_basic
import time

from utils.logger import Logger
from ml_utils import *
from simulutils import *
from data_gen import Data_gen

from sklearn.model_selection import train_test_split
from DAE import DAE
from FC_model import DNN_classifier, DNN_regressor, Residual_FC_classifier, Residual_FC_regressor
import numpy as np
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
import os

import tensorflow as tf

tf.enable_eager_execution()

from sklearn.preprocessing import MinMaxScaler
from lightgbm import LGBMRegressor, LGBMClassifier
from multiprocessing import Process



class ML_STRATEGY:

    def __init__(self, start_date=None, model_date=None, end_date=None, model='N',
                 asset='E',freq = 'W-TUE', data_load = True, data_save = True,
                 NN_params=None, NN_train_params=None, LGBM_params=None, LGBM_train_params=None):
        if NN_params is None:
            self.NN_params = NN_params_basic
        else:
            self.NN_params = NN_params
        if NN_train_params is None:
            self.NN_train_params = NN_train_params_basic
        else:
            self.NN_train_params = NN_train_params

        if LGBM_params is None:
            self.LGBM_params = LGBM_params_basic
        else:
            self.LGBM_params = LGBM_params

        if LGBM_train_params is None:
            self.LGBM_train_params = LGBM_train_params_basic
        else:
            self.LGBM_train_params = LGBM_train_params

        self.logger = Logger.set_logger('{}_{}_MODEL'.format(asset, model))
        self.data_gen = Data_gen(asset = asset, freq = freq, save_file = data_save, load_file = data_load)
        self.model = model
        self.asset = asset
        self.all_the_scores = {}

        self.data_codes = []
        self.entire_data = []
        self.features = {}

        self.ret_1w, self.target_data = self.data_gen.load_target_data()

        if end_date is None:
            self.end_date = pd.to_datetime(self.data_gen.index.index[-1]).strftime('%Y-%m-%d')
        else:
            self.end_date = end_date
        # start_date 정의 안되있으면
        if start_date is None:
            self.start_date = self.end_date
        else:
            self.start_date = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        if model_date is None:
            self.model_date = self.start_date
        else:
            self.model_date = pd.to_datetime(model_date).strftime('%Y-%m-%d')

    def set_data(self, data_code):
        if data_code == 'P':
            if not hasattr(self, 'price_data'):
                self.price_data, self.price_feature = self.data_gen.load_price_data()
                self.entire_data.append(self.price_data)
            self.data_codes.append('P')
            self.features[data_code] = self.price_feature

        if data_code == 'G':
            if not hasattr(self, 'feature_data'):
                self.feature_data, self.feature_feature, self.rv_data, self.rv_feature = self.data_gen.load_feature_data()
                self.entire_data.append(self.feature_data)
            else:
                self.entire_data.append(self.feature_data)
            self.data_codes.append('G')
            self.features[data_code] = self.feature_feature

        if data_code == 'R':
            if not hasattr(self, 'rv_data'):
                self.feature_data, self.feature_feature, self.rv_data, self.rv_feature = self.data_gen.load_feature_data()
                self.entire_data.append(self.rv_data)
            else:
                self.entire_data.append(self.rv_data)
            self.data_codes.append('R')
            self.features[data_code] = self.rv_feature

    def change_model(self):
        if self.model == 'N':
            self.model = 'L'
        else:
            self.model = 'N'
        self.logger.info("CHANGE TO MODEL : {}".format(self.model))
        self.entire_data = self.entire_data[:-1]
        if hasattr(self, 'all_data'):
            del self.all_data


    def ready_train(self):
        self.logger.info('READY FOR TRAINING.. MODEL : {}'.format(self.model))

        self.plus_data, self.plus_feature = self.data_gen.load_plus_data(self.model)
        self.entire_data.append(self.plus_data)

        self.all_data = pd.concat(self.entire_data, 1)
        self.all_data = np.clip(self.all_data, -999, 999)

        lag_target = self.target_data.shift(1)
        lag_target = lag_target.stack()
        lag_target.columns = [x + '_lag' for x in lag_target.columns]
        self.lag_target = lag_target.unstack()

        self.all_data = pd.concat([self.all_data, self.lag_target], 1)

    def run_single(self, data_code, model_date=None, start_date=None, end_date=None,
                   target_kind=1, validation=True, mode='training', save=True, load=True):

        if model_date is None:
            model_date = self.model_date

        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        test_index = self.data_gen.index.loc[start_date:]
        test_index = test_index.stack().dropna().index

        feature = list(self.features[data_code]) + list(self.plus_feature) + list(
            self.lag_target.columns.levels[0])  # [target_name+'_lag']
        if validation:
            v = 'T'
        else:
            v = 'F'

        model_number_ = '{}{}{}{}{}'.format(self.asset, target_kind, self.model, data_code, v)
        if target_kind == 3:
            how = 'reg'
        else:
            how = 'class'
        target_name = f'target_{target_kind}'
        self.logger.info('for model : {}, mode : {}'.format(model_number_, mode))
        target_data = self.target_data[target_name]
        # start_date부터는 다 가려버림
        target_data.loc[start_date:] = np.nan

        score = self.training(target_data=target_data,
                              feature=feature,
                              test_index=test_index,
                              validation=validation,
                              start_date=start_date,
                              model_date=model_date,
                              end_date=end_date,
                              model_name=model_number_,
                              how=how,
                              mode=mode,
                              save=save,
                              load=load)
        self.all_the_scores[model_number_] = score

        return score, model_number_

    def run_all(self, start_date=None, model_date=None, validations=[True, False], target_nums=[1, 2, 3],
                mode='training',
                save=True, load=True):
        if model_date is None:
            model_date = self.model_date

        if start_date is None:
            start_date = self.start_date

        for data_num in range(len(self.features)):
            data_code = self.data_codes[data_num]
            for target_kind in target_nums:
                for validation in validations:
                    self.run_single(data_code=data_code,
                                    model_date=model_date,
                                    start_date=start_date,
                                    target_kind=target_kind,
                                    validation=validation,
                                    mode=mode,
                                    save=save,
                                    load=load)
        return self.all_the_scores

    def run_single_gpu(self, gpu_num, data_code, model_date=None, start_date=None, end_date=None,
                       target_kind=1, validation=True, mode='training', save=True, load=True, simul_number=0):
        time.sleep(simul_number * 10)
        gpus = tf.config.experimental.list_physical_devices('GPU')
        tf.config.experimental.set_visible_devices(gpus[gpu_num], 'GPU')
        tf.config.experimental.set_memory_growth(gpus[gpu_num], True)
        return self.run_single(data_code, model_date, start_date, end_date,
                               target_kind, validation, mode, save, load)

    # def run_single_gpu(self, gpu_num, kwargs_list, simul_number = 0):
    #     time.sleep(simul_number * 20)
    #     gpus = tf.config.experimental.list_physical_devices('GPU')
    #     tf.debugging.set_log_device_placement(True)
    #     tf.config.experimental.set_visible_devices(gpus[gpu_num], 'GPU')
    #     tf.config.experimental.set_memory_growth(gpus[gpu_num], True)
    #     return self.run_single(data_code, model_date, start_date,
    #                            target_kind, validation, mode, save, load)

    def run_all_gpu(self, start_date=None, end_date=None, gpus=[3, 4], model_date=None, mode='training',
                    save=True, load=True):
        target_nums = [1, 2, 3]
        validations = [True, False]

        if model_date is None:
            model_date = self.model_date

        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        basic_params = {
            'model_date': model_date,
            'start_date': start_date,
            'end_date': end_date,
            'mode': mode,
            'save': save,
            'load': load,
        }

        for data_num in range(len(self.features)):
            data_code = self.data_codes[data_num]
            self.logger.info('START TRAINING FOR {}'.format(data_code))

            all_params = []
            for target_kind in target_nums:
                for validation in validations:
                    params = {'target_kind': target_kind,
                              'validation': validation,
                              'data_code': data_code}
                    all_params.append(params)

            procs = []
            gpu_num = len(gpus)
            for i, params in enumerate(all_params):
                params.update(basic_params)
                for gpu in range(gpu_num):
                    if i % gpu_num == gpu:
                        params['gpu_num'] = gpus[gpu]
                        params['simul_number'] = i
                        proc = Process(target=self.run_single_gpu,
                                       kwargs=params)
                        procs.append(proc)
                        proc.start()
            for proc in procs:
                proc.join()

    def get_target(self, target_name):
        return self.target_data[target_name].stack()

    def train_model(self, X_train, Y_train, X_val, Y_val, X_test,
                    best_path=None, AE_model=None, AE_params=None,
                    how='reg', mode='training'):
        if self.model == 'N':
            onehot = OneHotEncoder()
            if how == 'class':
                activate = Residual_FC_classifier(X_train.shape[-1], loss_function=tf.losses.softmax_cross_entropy,
                                                  AE_model=AE_model, AE_params=AE_params, **self.NN_params)
                Y_val = onehot.fit_transform(Y_val.values.reshape(-1, 1)).toarray()
                Y_train = onehot.fit_transform(Y_train.values.reshape(-1, 1)).toarray()
            else:
                activate = Residual_FC_regressor(X_train.shape[-1], n_outputs=1, loss_function='MSE',
                                                 AE_model=AE_model, AE_params=AE_params, **self.NN_params)
                Y_val = Y_val.values.reshape(-1, 1)
                Y_train = Y_train.values.reshape(-1, 1)

            if mode == 'training':
                activate.train(X_train, Y_train, eval_set=(X_val, Y_val), save_path=best_path, **self.NN_train_params)
            else:
                self.logger.info('Got Mode: Evaluation, use model from path : {}'.format(best_path))
            if how == 'reg':
                pred = activate.predict_saved(X_test, save_path=best_path)
                val_pred = activate.predict(X_val)
                train_pred = activate.predict(X_train)


            else:
                pred = activate.predict_proba_saved(X_test, save_path=best_path)[:, 1]
                val_pred = activate.predict_proba(X_val)[:, 1]
                train_pred = activate.predict_proba(X_train)[:, 1]




        elif self.model == 'L':
            if how == 'class':
                activate = LGBMClassifier(n_jobs=15, **self.LGBM_params)

            else:
                activate = LGBMRegressor(n_jobs=15, **self.LGBM_params)

            activate.fit(X_train.values, Y_train, eval_set=(X_val, Y_val), **self.LGBM_train_params)

            if how == 'reg':
                pred = activate.predict(X_test)
                val_pred = activate.predict(X_val)
                train_pred = activate.predict(X_train)

            else:
                train_pred = activate.predict_proba(X_train)[:, 1]
                val_pred = activate.predict_proba(X_val)[:, 1]
                pred = activate.predict_proba(X_test)[:, 1]

        return pred, val_pred, train_pred

    def training(self, target_data, feature, test_index, validation, start_date, model_date=None,
                 model_name='test', end_date=None, save=True, load=True, mode='training',
                 use_past=True, how='reg', scaler=MinMaxScaler, AE_model=None, AE_params=None):
        if not hasattr(self, 'all_data'):
            self.ready_train()
        #         use_data = all_data.copy()
        if model_date is None:
            model_date = start_date
        if end_date is None:
            end_date = self.end_date

        # 학습에 사용한 date를 기준으로 모델결정
        if self.model == 'N':
            base_path, model_path, best_path, model_date = set_dir(model_name, model_date, mode=mode)
        elif self.model == 'L':
            base_path, model_path, best_path, model_date = set_dir_LGBM(model_name, model_date, mode=mode)
        else:
            raise ValueError("{} is not defined, only 'N' and 'L' model is available".format(self.model))

        if load:
            if os.path.isfile(os.path.join(model_path, f'{start_date}_{end_date}_scores.pkl')):
                self.logger.info('found score file! just load it'.upper())
                return pd.read_pickle(os.path.join(model_path, f'{start_date}_{end_date}_scores.pkl'))

        # 해당 경로에 이미 파일이 있다면 안함
        X_test = self.all_data.loc[start_date: end_date]

        X_test = X_test.stack().reindex(test_index).dropna(how='all').unstack()

        X_train = self.all_data.drop(X_test.index)
        if use_past:
            X_train = X_train.loc[:model_date]
        target = target_data.stack()
        X_train = X_train.stack()

        X_test = X_test.stack()
        Y_train = target.reindex(X_train.index).dropna()
        X_train = X_train.loc[Y_train.index]

        test_index = X_test.index
        if validation:
            val_start = pd.to_datetime(model_date) - pd.tseries.offsets.MonthEnd(25)
            X_val = X_train.loc[val_start:]
            X_train.drop(X_val.index, inplace=True)
            Y_train.drop(X_val.index, inplace=True)
            Y_val = target.reindex(X_val.index).dropna()
            X_val = X_val.loc[Y_val.index]

        else:
            X_train, X_val, Y_train, Y_val = train_test_split(X_train, Y_train, shuffle=True, test_size=0.10)

        train_val_data = X_train.append(X_val)
        # imputation
        X_train = X_train.fillna(train_val_data.median())

        X_val = X_val.fillna(train_val_data.median())

        X_test = X_test.fillna(train_val_data.median())

        all_features = X_train.columns

        #     model_name = model_name + '_{}'.format(target_kind)
        self.logger.info(model_name)
        self.logger.info(feature)

        X_train_ = X_train[feature]
        X_test_ = X_test[feature]
        X_val_ = X_val[feature]
        if self.model == 'N':
            scaler = scaler()
            X_train_ = scaler.fit_transform(X_train_)
            X_test_ = scaler.transform(X_test_)
            X_val_ = scaler.transform(X_val_)
        print(X_train_.shape, Y_train.shape, X_val_.shape, )
        pred, val_pred, train_pred = self.train_model(
            X_train=X_train_,
            Y_train=Y_train,
            X_val=X_val_,
            Y_val=Y_val,
            X_test=X_test_,
            how=how,
            best_path=best_path,
            AE_model=AE_model,
            AE_params=AE_params,
            mode=mode)
        #         print(X_train_.shape, Y_train.shape, X_val_.shape, Y_val.shape, X_test_.shape)

        test_prob = pd.Series(pred.squeeze(), index=test_index).unstack()
        score = test_prob.copy()

        if save:
            score.to_pickle(os.path.join(model_path, f'{start_date}_{end_date}_scores.pkl'))
        #         sns.distplot(scores.stack().values, kde = True)
        #         plt.show()
        return score  # , train_pred

    def load_scores(self, data_code, model='N', model_date=None, start_date=None,
                    end_date=None, target_kind=1, validation=True):
        if model_date is None:
            model_date = self.model_date
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date
        if validation:
            v = 'T'
        else:
            v = 'F'

        model_name = '{}{}{}{}{}'.format(self.asset, target_kind, model, data_code, v)
        if model_date is None:
            model_date = start_date
        # 학습에 사용한 date를 기준으로 모델결정
        if model == 'N':
            base_path, model_path, best_path, model_date = set_dir(model_name, model_date, mode='evaluate')
        elif model == 'L':
            base_path, model_path, best_path, model_date = set_dir_LGBM(model_name, model_date, mode='evaluate')
        else:
            raise ValueError("{} is not defined, only 'N' and 'L' model is available".format(self.model))

        if os.path.isfile(os.path.join(model_path, f'{start_date}_{end_date}_scores.pkl')):
            self.logger.info('found score file! just load it'.upper())
            score = pd.read_pickle(os.path.join(model_path, f'{start_date}_{end_date}_scores.pkl'))
            self.all_the_scores[model_name] = score
        else:
            self.logger.info('{} IS NOT FOUND'.format(model_name))

    def load_all(self, models=['N', 'L'], data_codes=['PV', 'J', 'PVJ'],
                 start_date=None, model_date=None, end_date=None,
                 validations=[True, False], target_nums=[1, 2, 3],
                 save=True, load=True):
        if model_date is None:
            model_date = self.model_date

        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date
        for model in models:
            for data_code in data_codes:
                for target_kind in target_nums:
                    for validation in validations:
                        self.load_scores(data_code=data_code,
                                         model=model,
                                         model_date=model_date,
                                         start_date=start_date,
                                         end_date=end_date,
                                         target_kind=target_kind,
                                         validation=validation
                                         )
        return self.all_the_scores

if __name__ == '__main__':
    a = ML_STRATEGY(start_date = '2017-09-12')
    a.set_data('P')
    a.ready_train()
    a.run_all()