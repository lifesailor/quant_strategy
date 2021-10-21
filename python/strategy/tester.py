import os
import sys

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

base_path = os.path.dirname(os.path.abspath(os.path.dirname(os.path.abspath(__file__))))
data_path = os.path.join(base_path, 'data')
strategy_path = os.path.join(base_path, 'strategy')
result_path = os.path.join(strategy_path, 'result')


def CAGR(res):
    return res.values[-1] ** (1 / (res.index[-1] - res.index[0]).days * 360) - 1


class Tester:
    def __init__(self, strategy):
        self.strategy = strategy

        # time
        self.start = None
        self.end = None
        self.transact_fee = 0.001

    def set_period(self, start=None, end=None):
        self.start = pd.to_datetime(start)
        self.end = pd.to_datetime(end)

    def set_parameter(self, transact_fee=0.0004):
        self.transact_fee = transact_fee

    def run(self, save_file=False, use_bt=True):
        self.strategy.logger.info("[STEP 6] START BACKTEST")

        self.strategy.logger.info("[STEP 6 - 1] BACKTEST " + self.strategy.strategy_name + " TIME SERIES")
        if use_bt:
            TS_summary, TS_result, TS_result_tr, TS_port = self.make_report_bt(lag=2, position=self.strategy.TS_position)
        else:
            TS_summary, TS_result, TS_result_tr, TS_port = self.make_report(lag=2,
                                                                               position=self.strategy.TS_position)

        self.strategy.TS_summary = TS_summary
        self.strategy.TS_result = TS_result
        self.strategy.TS_result_tr = TS_result_tr
        self.strategy.TS_port = TS_port

        self.strategy.logger.info("[STEP 6 - 2] BACKTEST " + self.strategy.strategy_name + " CROSS SECTIONAL")
        if use_bt:
            CS_summary, CS_result, CS_result_tr, CS_port = self.make_report_bt(lag=2, position=self.strategy.CS_position)
        else:
            CS_summary, CS_result, CS_result_tr, CS_port = self.make_report(lag=2,
                                                                               position=self.strategy.CS_position)
        self.strategy.CS_summary = CS_summary
        self.strategy.CS_result = CS_result
        self.strategy.CS_result_tr = CS_result_tr
        self.strategy.CS_port = CS_port

        if save_file:
            save_path = os.path.join(result_path, self.strategy.strategy_name)
            if not os.path.exists(save_path):
                os.mkdir(save_path)

            self.strategy.TS_summary.to_csv(os.path.join(save_path, 'TS_summary' + self.start + "_" + self.end + ".csv"))
            self.strategy.TS_result.to_csv(os.path.join(save_path, 'TS_result' + self.start + "_" + self.end + ".csv"))
            self.strategy.TS_result_tr.to_csv(os.path.join(save_path, 'TS_result_tr' + self.start + "_" + self.end + ".csv"))
            self.strategy.TS_port.to_csv(os.path.join(save_path, 'TS_port' + self.start + "_" + self.end + ".csv"))

            self.strategy.CS_summary.to_csv(os.path.join(save_path, 'CS_summary' + self.start + "_" + self.end + ".csv"))
            self.strategy.CS_result.to_csv(os.path.join(save_path, 'CS_result' + self.start + "_" + self.end + ".csv"))
            self.strategy.CS_result_tr.to_csv(os.path.join(save_path, 'CS_result_tr' + self.start + "_" + self.end + ".csv"))
            self.strategy.CS_port.to_csv(os.path.join(save_path, 'CS_port' + self.start + "_" + self.end + ".csv"))

    def plot_result(self):
        plt.figure(figsize=(15, 10))
        plt.plot(self.strategy.TS_result.loc[self.start:self.end], label='TS')
        plt.plot(self.strategy.CS_result.loc[self.start:self.end], label="CS")
        plt.legend()
        plt.show()

    def make_report_bt(self, lag=2, position=None):
        port_ = position.copy().loc[self.start:self.end]
        port_.fillna(0, inplace=True)
        columns = port_.columns
        ret_data_ = self.strategy.ret.loc[port_.index[0]:self.end]
        ret_data_.iloc[0] = 0
        ret_data_ = ret_data_.reindex(columns=columns)

        index = (1 + ret_data_).cumprod()

        port_.index += pd.tseries.offsets.BDay(lag)
        strats = []
        s = bt.Strategy(self.strategy.strategy_name, [bt.algos.RunOnDate(*port_.index),
                                                      bt.algos.WeighTarget(port_),
                                                      bt.algos.Rebalance()])
        strats.append(bt.Backtest(s, index, initial_capital= 1e+9))
        res = bt.run(*strats)
        return res.stats, res.prices/100, res.prices/100, port_

    def make_report(self, lag=2, position=None):
        port_ = position.copy().loc[self.start:self.end]
        port_.fillna(0, inplace=True)
        columns = port_.columns
        ret_data_ = self.strategy.ret.loc[port_.index[0]:self.end]
        ret_data_ = ret_data_.reindex(columns=columns)

        port_ = port_.reindex(ret_data_.index)
        port_ = port_.shift(lag)
        port_.columns = columns
        port_ = port_.ffill()
        a = ret_data_ * port_
        a = a.sum(axis=1)

        # TURNOVER
        turnover = position.diff()
        turnover.iloc[0] = port_.iloc[0]
        trade_cost = turnover[turnover > 0].sum(1) * self.transact_fee / 2 - turnover[turnover < 0].sum(1) * self.transact_fee / 2

        # TRADE COST
        a2 = a - trade_cost.reindex(a.index).fillna(0)
        a.iloc[0] = 0
        res = (a + 1).cumprod()
        a2.iloc[0] = 0
        res_tr = (a2 + 1).cumprod()

        TO = (abs(turnover).sum(1) / 2).resample('Y').sum().mean()  # 연간 회전율 평균
        MDD = (res / res.cummax() - 1).min()  # MDD
        CAGR_ = CAGR(res)
        CAGR_tr = CAGR(res_tr)
        vol = np.std(res.pct_change().dropna())
        sharpe = np.mean(res.pct_change().dropna()) / np.std(res.pct_change().dropna()) * np.sqrt(252)
        sharpe_tr = np.mean(res_tr.pct_change().dropna()) / np.std(res.pct_change().dropna()) * np.sqrt(252)

        port_.index.name = 'tdate'
        port_ = port_[position != 0].reset_index().melt('tdate').dropna()
        port_.columns = ['tdate', 'ticker', 'value']
        port_['ticker'] = port_['ticker'].apply(lambda x: 'A' + x)
        port_['score'] = port_.apply(lambda x: position.loc[x.tdate, x.ticker[1:]], 1)

        summary = pd.Series({'TURNOVER': TO,
                             'MDD': MDD,
                             'CAGR': CAGR_,
                             'CAGR_TR': CAGR_tr,
                             'VOL': vol,
                             "SHARPE": sharpe,
                             "SHARPE_TR": sharpe_tr})
        return summary, res, res_tr, port_

