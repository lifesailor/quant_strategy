import os
from simulutils import *

import sys
import logging

base_path = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
log_path = os.path.join(base_path, 'log')


def mkdir_for_NN(model_name, model_date):
    dir_name = f'{model_date}'
    # 저장용 경로 생성 및 반환
    _, base_path = mkdir_for_model(model_name, save=False)
    save_path, model_path = mkdir_for_model(os.path.join(model_name, dir_name))

    model_save_path = os.path.join(save_path, model_name)
    if not os.path.isdir(model_save_path):
        os.mkdir(model_save_path)
    best_dir = f'{model_save_path}'
    best_path = f'{model_save_path}/{model_name}.h5'
    return base_path, model_path, best_dir, best_path


def set_dir(model_name, model_date, mode):
    base_path, model_path, best_dir, best_path = mkdir_for_NN(model_name, model_date)

    if mode != 'training':
        if not check_saved(best_dir):
            print(f'There is no path in {model_date}')
            if check_empty(model_path):
                delete_empty(model_path)
                model_date = fetch_recent_saved(base_path)
                base_path, model_path, best_dir, best_path = mkdir_for_NN(model_name, model_date)
            print(f'load data from {model_date}')
    return base_path, model_path, best_path, model_date


def mkdir_for_LGBM(model_name, model_date):
    dir_name = f'{model_date}'
    # 저장용 경로 생성 및 반환
    _, base_path = mkdir_for_model(model_name, save=False)
    save_path, model_path = mkdir_for_model(os.path.join(model_name, dir_name))
    best_dir = None
    best_path = None
    return base_path, model_path, best_dir, best_path


def set_dir_LGBM(model_name, model_date, mode):
    base_path, model_path, best_dir, best_path = mkdir_for_LGBM(model_name, model_date)
    if mode != 'training':
        if check_empty(model_path):
            delete_empty(model_path)
            model_date = fetch_recent_saved(base_path)
            base_path, model_path, best_dir, best_path = mkdir_for_LGBM(model_name, model_date)
        print(f'load data from {model_date}')
    return base_path, model_path, best_path, model_date


def fetch_recent_saved(base_path):
    return pd.to_datetime(os.listdir(base_path)).sort_values()[0].strftime('%Y-%m-%d')


def check_saved(best_dir):
    if os.path.isdir(best_dir) and len(os.listdir(best_dir)) != 0:
        return True


def mkdir_for_model(model_name, save=True):
    # 저장용 directory 생성

    model_path = f'./training_result/{model_name}/'

    try:
        os.mkdir(model_path)
    except FileExistsError:
        print('Caution : Model directory already exists')

    if save:
        save_path = f'./training_result/{model_name}/saved/'
        try:
            os.mkdir(save_path)
        except FileExistsError:
            print('Caution : Model directory already exists')
    else:
        save_path = None

    return save_path, model_path


def delete_empty(path):
    dir_ = []
    for root, dirs, files in os.walk(path):
        for d in dirs:
            dir_.append(os.path.join(root, d))

    for i in dir_[::-1]:
        os.rmdir(i)
    os.rmdir(path)


def check_empty(path, delete=True):
    """
    DIRECTORY 비어있는것 확인
    """
    flag = False
    for root, dirs, files in os.walk(path):
        if len(files) != 0:
            flag = True
            for f in files:
                print(os.path.join(root, f))
        for d in dirs:
            check_empty(os.path.join(root, d))
    #             print(os.path.join(root, d))
    if flag:
        return False
    else:
        return True


def set_save_path(model_name, start_date, model_num):
    '''
    model parameter를 넣으면 기간별 모델 따로 저장
    저장 경로 설정 및 생성
    params : 만들 파라미터

    '''
    # 저장 경로 설정 by model_name in parameter dict

    save_path = r'./training_result/{}/saved/{}/{}/'.format(model_name,
                                                            model_num,
                                                            start_date.strftime('%Y%m%d'))
    # 저장용 directory 생성
    try:
        os.mkdir(r'./training_result/{}/saved/{}/'.format(model_name, model_num))
    except FileExistsError:
        print('Caution : Model directory already exists')
        pass

    try:
        os.mkdir(save_path)
    except FileExistsError:
        print('Caution : Model directory already exists')
        pass

    #     try:
    #         os.mkdir(params['save_path']+'best/')
    #     except FileExistsError:
    #         print('Caution : Model directory already exists')
    #         pass
    return save_path

import pandas as pd
from scipy.special import erfinv

import matplotlib.pyplot as plt

def CAGR(res):
    # CAGR 계산
    return res.values[-1] ** (1 / (res.index[-1] - res.index[0]).days * 360) - 1


def display_IC(IC, figsize=(10, 5)):
    ic = IC.dropna()
    fig, ax = plt.subplots(figsize=figsize)
    plt.grid(b=True, which='major', color='#666666', linestyle='-')
    color = 'tab:red'
    ax.plot(ic.index, ic['per'].values, color)
    ax.set_label('L-S')
    # ic['per'].plot(ax = ax, color = color)
    # ax.set_ylim(1, 1)
    ax.set_xlabel('date', color='w')
    ax.set_ylabel('return', color='w')
    ax.tick_params(axis='y', labelcolor='w')
    plt.legend(['L-S RETURN'])
    color = 'tab:blue'
    ax2 = ax.twinx()
    ax2.bar(ic.index, ic['IC'].values, width=15, color=color)
    ax2.set_ylabel('IC', color='w')
    ax2.tick_params(axis='y', labelcolor='w')
    ax2.set_label('IC')
    # ic['IC'].plot( kind = 'bar', ax = ax2)
    # ax2.set_ylim(-1, 1)
    ax.tick_params(axis='x', labelcolor='w')

    plt.minorticks_on()
    plt.grid(b=True, which='minor', color='#999999', linestyle='-', alpha=0.2)
    plt.legend(['IC'])
    fig.tight_layout()

    plt.show()


def get_ic(ret_1m, scores):
    ret_ = ret_1m.reindex(scores.index, columns=scores.columns).loc[scores.index].values
    numbers = scores.count(1).values
    scor = scores.values

    dates = scores.index

    scor = np.expand_dims(scor, -1)
    ret_ = np.expand_dims(ret_, -1)

    cal_ic = np.concatenate([scor, ret_], axis=-1)

    cov = np.nansum(np.prod(cal_ic - np.nanmean(cal_ic, 1, keepdims=True), 2), 1)

    ic = cov / np.nanstd(cal_ic, 1).prod(-1) / numbers
    return pd.DataFrame(ic, columns=['IC'], index=dates)


def build_rank_port(scores):
    """
    결과값 scores가 들어가면 rank_port 생성
    percent_rank를 기준으로 abosolute deviation 기준으로 normalize하고
    2를 곱해 LONG/SHORT이 각각 1이 되도록 조정한 롱숏 포트폴리오 생성

    params: scores : 모델 결과값
    """
    pct_rank = scores.rank(1, pct=True).T  # , method = 'max').T

    rank_port = pct_rank - pct_rank.mean()

    rank_port = (rank_port / rank_port.abs().sum()).T * 2
    return rank_port


def data_code_to_name(data_code):
    if data_code == 'P':
        return "PRICE"

    if data_code == 'V':
        return "VOLUME"

    if data_code == 'J':
        return "FACTOR"

    if data_code == 'PV':
        return "PRICE + VOLUME"

    if data_code == 'PVJ':
        return 'PRICE + VOLUME + FACTOR'


def melt_summary(sums, data_name, target_kind, model_number_):
    all_summary = []

    for i in range(len(sums)):
        summ = sums[i][['RANK_L-S']].T

        summ['DATA'] = data_name
        summ['TARGET'] = target_kind
        summ['MODEL_NUMBER'] = model_number_
        summ['EW_L-S'] = sums[i]['EW_L-S']['RETURN']
        for j in range(5):
            summ[f'QUAN_{j}'] = sums[i][f'quan_{j}']['RETURN']

        all_summary.append(summ.set_index(['MODEL_NUMBER', 'START_DATE', 'END_DATE', 'DATA', 'TARGET']))

    return pd.concat(all_summary)

def make_report(scores, ret_data, model_name, ret_1m,
                start_date=None, end_date=None, feature_importances=None, qcut_num=5, test_cut=5, transact_fee=0.004,
                save_path=None, lagging=0, sig_data2=None):
    #     scores =scores.rolling(20).mean().resample('BM').last()
    scores.columns.name = 'code'
    scores.index.name = 'date'

    sig_data = qcut_signal(scores.loc[start_date: end_date].rank(1, method='first'), test_cut, scores.index[0])

    #     k = sig_data.set_index(['tdate', 'code']).dropna()

    #     k2 = target2.stack().reindex(k.index)

    #     accuracy = precision_score(k.dropna().values, k2.dropna().values, average = 'micro')

    sig_data = sig_data.pivot(index='tdate', columns='code', values='value')
    sig_data.index = pd.to_datetime(sig_data.index)

    ress = []
    tr_ress = []
    mdds = []
    turnovers = []
    cagrs = []
    tr_cagrs = []
    sharpes = []
    tr_sharpes = []
    ports = []
    vols = []
    rtn = ret_data.copy().loc[start_date: end_date]
    for signal in range(test_cut + 1 + 1):  # EW_L-S,  RANK_L-S

        if signal == test_cut:
            weight_sig_data = sig_data.copy().apply(sig_to_weight, axis=1, args=(test_cut - 1, 0, 1.))  # EW_L-S
            
        elif signal == test_cut + 1:
            weight_sig_data = build_rank_port(scores.loc[start_date: end_date])  # RANK_L-S
            weight_sig_data.index.name = 'tdate'
            weight_sig_data.columns.name = 'code'
        else:
            weight_sig_data = sig_data.copy().apply(long_only_sig_to_weight, axis=1, args=(signal, 1))
        # weight_sig_data = sig_dataa

        rep_ = weight_sig_data[weight_sig_data != 0]

        # rep_ = sig_data

        port_ = rep_
        port_.index += pd.tseries.offsets.BDay(lagging)
        port_.fillna(0, inplace=True)

        columns_ = rep_.columns  # 종목리스트

        ret_data_ = rtn.loc[port_.index[0]:]  # 해당 기간 맵핑
        ret_data_ = ret_data_.reindex(columns=columns_)  # 종목 일치

        port_ = port_.reindex(ret_data_.index)  # 포트폴리오 기간 맵핑
        #     port_ = port_.fillna(rep_.iloc[0])

        port_.columns = columns_  # 종목 맵핑

        port_ = port_.ffill()  # 포트폴리오를 계속 가져간다고 가정

        a = ret_data_ * port_  # 수익률에 포트폴리오 product
        a = a.sum(1)  # and sum

        turnover = rep_.diff()  # turnover 계산
        turnover.iloc[0] = port_.iloc[0]
        # 거래비용 참조
        trade_cost = turnover[turnover > 0].sum(1) * transact_fee / 2 - turnover[turnover < 0].sum(
            1) * transact_fee / 2

        # 매 포트가 변할때마다 trade_cost 발생
        a2 = a - trade_cost.reindex(a.index).fillna(0)

        a.iloc[0] = 0
        res = (a + 1).cumprod()  # 거래세 고려 x performance

        a2.iloc[0] = 0
        res_tr = (a2 + 1).cumprod()  # 거래세 고려  performance

        TO = (abs(turnover).sum(1) / 2).resample('Y').sum().mean()  # 연간 회전율 평균
        MDD = (res / res.cummax() - 1).min()  # MDD
        CAGR_ = CAGR(res)
        CAGR_tr = CAGR(res_tr)
        vol = np.std(res.pct_change().dropna())
        sharpe = np.mean(res.pct_change().dropna()) / np.std(res.pct_change().dropna()) * np.sqrt(252)
        sharpe_tr = np.mean(res_tr.pct_change().dropna()) / np.std(res.pct_change().dropna()) * np.sqrt(252)
        port = rep_[rep_ != 0].reset_index().melt('tdate').dropna()
        port.columns = ['tdate', 'code', 'value']
        port['code'] = port['code'].apply(lambda x: 'A' + x)
        #         port['SCORE'] = port.apply(lambda x: scores.loc[x.tdate, x.code[1:]], 1)
        sharpes.append(sharpe)
        tr_sharpes.append(sharpe_tr)
        mdds.append(MDD)
        turnovers.append(TO)
        tr_cagrs.append(CAGR_tr)
        cagrs.append(CAGR_)
        ports.append(port)
        tr_ress.append(res_tr)
        ress.append(res)
        vols.append(vol)

    columns = [f'quan_{d}' for d in range(test_cut)]
    columns += ['EW_L-S', 'RANK_L-S']

    ress = pd.concat(ress, 1)
    ress.columns = columns
    tr_ress = pd.concat(tr_ress, 1)
    tr_ress.columns = columns

    IC = get_ic(ret_1m, scores)
    IC['per'] = ress.iloc[:, -1].reindex(IC.index)
    ic_mean = IC.mean()['IC']
    ic_vol = IC.std()['IC']

    summary = pd.DataFrame(
        [ress.iloc[-1].values, tr_ress.iloc[-1].values, mdds, turnovers, cagrs, tr_cagrs, sharpes, tr_sharpes, vols],
        index=['RETURN', 'RETURN_TR', 'MDD', 'TURNOVER', 'CAGR', 'CAGR_TR', 'SHARPE', 'SHARPE_TR', 'VOL'],
        columns=columns)
    #     summary.loc['ACCURACY', :] = accuracy
    summary.loc['IC_MEAN', :] = ic_mean
    summary.loc['IC_VOL', :] = ic_vol
    summary.loc['START_DATE', :] = start_date
    summary.loc['END_DATE', :] = end_date

    scores.index.name = 'date'
    model_result = pd.concat([scores.reset_index().melt('date').set_index(['date', 'code']).dropna(),
                              sig_data.reset_index().melt('tdate').set_index(['tdate', 'code']).dropna()], 1).dropna()

    model_result = model_result.reset_index()

    model_result.columns = ['tdate', 'code', 'score', 'quantile']

    if save_path is not None:
        writer = pd.ExcelWriter('./backtest_report/{}_{}_{}_summary_all.xlsx'.format(model_name, start_date, end_date),
                                engine='xlsxwriter')
        summary.to_excel(writer, sheet_name='summary')

        ress.to_excel(writer, sheet_name='performance')
        tr_ress.to_excel(writer, sheet_name='performance_tr')

        np.log(ress).to_excel(writer, sheet_name='log_performance')
        np.log(tr_ress).to_excel(writer, sheet_name='log_performance_tr')
        (1 + ress.pct_change().fillna(0)).resample('Y').prod().to_excel(writer, sheet_name='Yearly')
        (1 + tr_ress.pct_change().fillna(0)).resample('Y').prod().to_excel(writer, sheet_name='Yearly_tr')

        IC.to_excel(writer, sheet_name='IC')

        model_result.to_excel(writer, sheet_name='model_result', index=False)
        ports[-4].sort_values('tdate').to_excel(writer, sheet_name='long_portfolio', index=False)
        scores.loc[start_date:end_date].to_excel(writer, sheet_name='test_prob')
        if feature_importances is not None:
            feature_importances.to_excel(writer, sheet_name='feature_importance')
        writer.close()
    return summary, ress, tr_ress, IC, ports




def make_columns(dataset, use_cols, adds=['standard', 'gr', 'rank'], include_mean_std=True):
    col1, col2 = dataset.T.reset_index().columns[:2]
    scaled_data = dataset[use_cols].T.reset_index().set_index([col1, col2]).sort_index().T.stack().T
    new_df = []
    for add in adds:
        if add == 'gr':
            eps = 0.0001
            upper = 1 - eps
            lower = -1 + eps
            data = erfinv(scaled_data.rank(method='max', pct=True) * (upper - lower) - upper)
            data = transform_dataset(data, add)

        elif add == 'rank':
            data = scaled_data.rank(pct=True)
            data = transform_dataset(data, add)

        elif add == 'standard':
            mean_data = pd.DataFrame(columns=scaled_data.columns, index=scaled_data.index)

            mean_data.iloc[0] = scaled_data.mean()

            mean_data.ffill(inplace=True)

            std_data = pd.DataFrame(columns=scaled_data.columns, index=scaled_data.index)

            std_data.iloc[0] = scaled_data.std()

            std_data.ffill(inplace=True)

            data = (scaled_data - mean_data) / std_data

            data = transform_dataset(data, add)
            if include_mean_std:
                mean_data = transform_dataset(mean_data, 'mean')
                std_data = transform_dataset(std_data, 'std')

                new_df.append(mean_data)
                new_df.append(std_data)

        new_df.append(data)
    return pd.concat(new_df, 1)


def transform_dataset(data, name):
    """
        데이터셋 만들고 원래 모양으로 되돌려주는 함수
    """
    try:
        data = data.stack().reset_index().set_index(['level_0', 'level_1']).sort_index().T.stack()
    except KeyError:
        level_0, level_1 = data.stack().reset_index().columns[:2]
        data = data.stack().reset_index().set_index([level_0, level_1]).sort_index().T.stack()

    data.columns = list(map(lambda x: x + '_{}'.format(name), data.columns))

    return data.unstack()

if __name__ == '__main__':
    logger = Logger.set_logger('test')
    logger.info('logging test')