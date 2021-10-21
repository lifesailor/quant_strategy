import os
import sys
import numpy as np
import pandas as pd
import asyncio

base_path = os.path.abspath('../..')
data_path = os.path.join(base_path, 'data\old_data')
database_path = os.path.join(data_path, 'database')
strategy_path = os.path.join(base_path, 'strategy')
check_path = os.path.join(strategy_path, 'check')
sys.path.append(strategy_path)

from strategy import CommodityStrategy, EquityStrategy, IRStrategy, EmergingStrategy

from EEM import EEM
from EDY import EDY
from EFX import EFX
from ELQ import ELQ
from EPE import EPE
from EQL import EQL
from EPM import EPM
from ESS import ESS
from EST import EST
from EVO import EVO

class GrpFeature:
    def __init__(self, asset='E', index=None, raw_index=None, ret=None):
        if index is None or raw_index is None or ret is None:
            if asset == 'E':
                self.ret_data_loader = EquityStrategy(strategy_name='EPM', asset_type='EQUITY')
            elif asset == 'EM':
                self.ret_data_loader = EmergingStrategy(strategy_name="EMPM", asset_type='EMERGING')
            elif asset == 'C':
                self.ret_data_loader = CommodityStrategy(strategy_name='CPM', asset_type='COMMODITY')
            elif asset == 'I':
                self.ret_data_loader = IRStrategy(strategy_name='IPM', asset_type='IR')
            self.ret_data_loader.load_index_and_return(from_db=True, save_file=False)
            self.index = self.ret_data_loader.index
            self.raw_index = self.ret_data_loader.raw_index
            self.ret = self.ret_data_loader.ret
        else:
            self.index = index
            self.raw_index = raw_index
            self.ret = ret

        self.rv_data = {}
        self.feature_data = {}

    def make_strategy_obj(self, strategy_name, asset_type):
        obj = eval(strategy_name)(strategy_name=strategy_name, asset_type=asset_type)
        obj.index = self.index
        obj.raw_index = self.raw_index
        obj.ret = self.ret
        return obj

    def load_data(self, strategy_list=None):
        if strategy_list is None:
            strategy_list = self.strategy_list

        for strategy_name in strategy_list:
            getattr(self, "{}_data".format(strategy_name))()

        feature_data = pd.concat(self.feature_data, 1)
        rv_data = pd.concat(self.rv_data, 1)

        return feature_data, rv_data

    async def await_wrapper(self, func):

        return await self.loop.run_in_executor(None, func)

    async def async_fetch(self, strategy_list):
        func_list = [getattr(self, "{}_data".format(strategy_name)) for strategy_name in strategy_list]
        futures = [asyncio.ensure_future(self.await_wrapper(i)) for i in func_list]
        await asyncio.gather(*futures)

    def load_data_async(self, strategy_list=None):
        if strategy_list is None:
            strategy_list = self.strategy_list
        try:
            self.loop = asyncio.new_event_loop()
            self.loop.run_until_complete(self.async_fetch(strategy_list))
            self.loop.close()
        except RuntimeError:
            # ipython에서는 이 에러뜨면서 안됨
            return self.load_data(strategy_list)

        feature_data = pd.concat(self.feature_data, 1)
        rv_data = pd.concat(self.rv_data, 1)

        return feature_data, rv_data


class GrpEquityFeature(GrpFeature):
    def __init__(self, index = None, raw_index = None, ret = None):

        super().__init__('E', index = index, raw_index = raw_index, ret = ret)
        self.strategy_list = ["EEM", "EDY", "EFX", "ELQ",
                             "EPE", "EQL", "EPM", "EST",
                             "EVO"]
        self.asset_type = "EQUITY"

    def EEM_data(self):
        eem = self.make_strategy_obj(strategy_name="EEM", asset_type=self.asset_type)

        eem.load_strategy_data(table='datastream', origin='ERR')
        eem.set_rebalance_period(ts_freq='month', cs_freq='month')
        eem.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, longlen=12, shortlen=6, lag=0)

        self.feature_data['err'] = eem.err
        self.rv_data['eem'] = eem.RV


    def EDY_data(self):
        edy = self.make_strategy_obj(strategy_name="EDY", asset_type=self.asset_type)

        edy.load_strategy_data1(table='DS', origin1='DPS', origin2='DPS1')
        edy.load_strategy_data2(table='bloom', origin='10Yield')
        edy.set_rebalance_period(ts_freq='month', cs_freq='month')
        edy.calculate_signal(minobs1=12, nopos=0.4, CS=0.35)

        self.feature_data['dps'] = edy.dps
        self.feature_data['dps1'] = edy.dps1
        self.rv_data['edy'] = edy.RV

    def EFX_data(self):
        efx = self.make_strategy_obj(strategy_name="EFX", asset_type=self.asset_type)
        efx.load_strategy_data(table='bloom', origin='fx')
        efx.set_rebalance_period(ts_freq='month', cs_freq='month')
        efx.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, longlen=12, shortlen=0, SDEV=12)

        self.feature_data['fx'] = efx.fx
        self.rv_data['efx'] = efx.RV

    def ELQ_data(self):
        elq = self.make_strategy_obj(strategy_name="ELQ", asset_type=self.asset_type)
        elq.load_strategy_data(table='CEIC', origin='m2gdp')
        elq.set_rebalance_period(ts_freq='month', cs_freq='month')
        elq.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, longlen=6, shortlen=0)

        self.feature_data['m2gdp'] = elq.m2gdp
        self.rv_data['elq'] = elq.RV

    def EPE_data(self):
        epe = self.make_strategy_obj(strategy_name="EPE", asset_type=self.asset_type)
        epe.load_strategy_data(table='DS', origin1='EPS', origin2='EPS1')
        epe.set_rebalance_period(ts_freq='month', cs_freq='month')
        epe.calculate_signal(minobs1=12, nopos=0.4, CS=0.35)

        self.feature_data['eps'] = epe.eps
        self.feature_data['eps1'] = epe.eps1
        self.rv_data['epe'] = epe.RV

    def EQL_data(self):
        eql = self.make_strategy_obj(strategy_name="EQL", asset_type=self.asset_type)
        eql.load_strategy_data(table='FS', origin1='ROA', origin2='ICR')
        eql.set_rebalance_period(ts_freq='month', cs_freq='month')  # rebalance_day: monday = 0, sunday = 6
        eql.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, lag=1)

        self.feature_data['roa'] = eql.roa
        self.feature_data['icr'] = eql.icr
        self.rv_data['eql'] = eql.RV

    def EPM_data(self):
        epm = self.make_strategy_obj(strategy_name="EPM", asset_type=self.asset_type)
        epm.load_strategy_data(table='datastream', origin='EPS')
        epm.set_rebalance_period(ts_freq='week', cs_freq='month',
                                 rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
        epm.calculate_signal(minobs1=52, longlen=52, longlen2=13, shortlen=2, CS=0.35)
        if 'eps' not in self.feature_data:
            self.feature_data['eps'] = epm.eps

        self.rv_data['epm'] = epm.RV

    def ESS_data(self):
        pass

    def EST_data(self):
        est = self.make_strategy_obj(strategy_name="EST", asset_type=self.asset_type)
        est.load_strategy_data(table='FS', origin='growthvalue')
        est.set_rebalance_period(ts_freq='month', cs_freq='month')  # rebalance_day: monday = 0, sunday = 6
        est.calculate_signal(minobs1=12, nopos=0.4, CS=0.35, per=3)
        growth = []
        values = []
        growthvalue = est.growthvalue
        for i in growthvalue.columns:
            if i[-1].upper() == 'V':
                values.append(i)
            else:
                growth.append(i)
        value_data = growthvalue[values]
        value_data.columns = [x[:-1] for x in value_data.columns]
        self.feature_data['growth'] = growthvalue[growth]
        self.feature_data['values'] =  value_data
        self.rv_data['est'] = est.RV
        
    def EVO_data(self):
        evo = self.make_strategy_obj(strategy_name="EVO", asset_type=self.asset_type)
        evo.load_strategy_data(table='bloom', origin='ivol')
        evo.set_rebalance_period(ts_freq='month', cs_freq='month')
        evo.calculate_signal(minobs1=12, nopos=0.4, CS=0.35)

        self.feature_data['ivol'] = evo.ivol
        self.rv_data['evo'] = evo.RV


class GrpCommodityFeature(GrpFeature):
    def __init__(self, index=None, raw_index=None, ret=None):
        super().__init__('E', index=index, raw_index=raw_index, ret=ret)
        self.strategy_list = ["CPM", "CVA", "CVA2", "CSS",
                              "CVO", "CCA3", "CVA3"]
        self.asset_type = "COMMODITY"

    def CPM_data(self):
        cpm = self.make_strategy_obj(strategy_name="CPM", asset_type=self.asset_type)
        cpm.set_rebalance_period(ts_freq='week', cs_freq='week',
                                 rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
        cpm.calculate_signal(CS=0.35, minobs=52, longlen=52, shortlen=17)

        self.rv_data['cpm'] = cpm.RV

    def CVA_data(self):
        cva = self.make_strategy_obj(strategy_name="CVA", asset_type=self.asset_type)
        cva.set_rebalance_period(ts_freq='week', cs_freq='week',
                                 rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
        cva.calculate_signal(CS=0.35, nopos=0.6, lookback_period=63)

        self.rv_data['cpm'] = cva.RV

    def CVO_data(self):
        cvo = self.make_strategy_obj(strategy_name="CVO", asset_type=self.asset_type)
        cvo.set_rebalance_period(ts_freq='week', cs_freq='week',
                                 rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
        cvo.calculate_signal(CS=0.35, nopos=0.4, minobs=52)

        self.rv_data['cpm'] = cvo.RV

    def CCA3_data(self):
        cca3 = self.make_strategy_obj(strategy_name="CCA3", asset_type=self.asset_type)
        cca3.load_strategy_data(origin1='carry-com', origin2='carry-com2')
        cca3.set_rebalance_period(ts_freq='month', cs_freq='month', rebalance_weekday=1)
        cca3.calculate_signal(CS=0.35, nopos=0.4, minobs=60)

        self.feature_data['carry-com'] = cca3.carry1
        self.feature_data['carry-com2'] = cca3.carry2
        self.rv_data['cca3'] = cca3.RV

    def CVA2_data(self):
        cva2 = self.make_strategy_obj(strategy_name="CVA2", asset_type=self.asset_type)
        cva2.load_strategy_data(table='bloom', origin='commo pos')
        cva2.set_rebalance_period(ts_freq='week', cs_freq='week')
        cva2.calculate_signal(CS=0.35, nopos=0.4, minobs1=12)

        self.feature_data['commo_pos'] = cva2.commopos
        self.rv_data['cva2'] = cva2.RV

    def CVA3_data(self):
        cva3 = self.make_strategy_obj(strategy_name="CVA3", asset_type=self.asset_type)
        cva3.load_strategy_data1(table='bloom', origin='fx')
        cva3.load_strategy_data2(table='past', origin='fut1price-com')
        cva3.set_rebalance_period(ts_freq='month', cs_freq='month')  # rebalance_day: monday = 0, sunday = 6
        cva3.calculate_signal(CS=0.35, nopos=0.4, minobs=60, minobs1=12, SMA=1, LMA=12, Lwindow=54)
        if 'fx' not in self.feature_data:
            self.feature_data['fx'] = cva3.fx
        self.feature_data['fut1price-com'] = cva3.fut1price
        self.rv_data['cva2'] = cva3.RV

    def CSS_data(self):
        css = self.make_strategy_obj(strategy_name="CSS", asset_type=self.asset_type)
        css.load_strategy_data1(table='bloom', origin1='fx', origin2='carry-com', origin3='carry-com2')
        css.load_strategy_data2(table='past', origin='seasonal')
        css.set_rebalance_period(ts_freq='month', cs_freq='month',
                                 rebalance_weekday=1)  # rebalance_day: monday = 0, sunday = 6
        css.calculate_signal(CS=0.35)
        if 'carry-com' not in self.feature_data:
            self.feature_data['carry-com'] = css.carry_com
        if 'carry-com2' not in self.feature_data:
            self.feature_data['carry-com2'] = css.carry_com2
        if 'fx' not in self.feature_data:
            self.feature_data['fx'] = css.fx
        self.feature_data['seasonal'] = css.seasonal
        self.rv_data['css'] = css.RV

# TODO: 이머징, IR? 만들것
class GrpEmergingFeature(GroFeature):
    pass
if __name__ == '__main__':
    feature = GrpEquityFeature()
    result= feature.load_data_async()
    print(result)