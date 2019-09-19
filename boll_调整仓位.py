from finonelib.interface.api import *
from finonelib import *
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
logger = logging.getLogger(__name__)

class Strategy(object):
    bid_qty = 10
    ask_qty = 10

    marketdata_counter = 0
    executed_bid_counter = 0
    executed_ask_counter = 0
    submited_bid_counter = 0
    submited_ask_counter = 0
    cancelled_counter = 0

    submited_in_cancel = 0
    submited_in_marketdata = 0

    maker_order_list = list()
    executed_order_list = list()
    market_vwap_bid_price_list = list()
    market_vwap_ask_price_list = list()
    
    k = 2 
    N = 20
    vwaps = list()
    boll_medium_list = list()
    boll_up_list = list()
    boll_down_list = list()
    times = list()
    best_bid_price_list = list()
    best_ask_price_list = list()
    pnls = list()
    
    def vwap2(p1,q1,p2,q2):
        return round((p1*q1 + p2*q2)/(q1+q2))

    def on_receive_marketdata(self, marketdata: ClobData):
        self.marketdata_counter += 1
        # print(f'marketdata: {self.marketdata_counter}')
        # logger.warning('on_receive_marketdata')
        pending_orders = get_my_pending_orders(marketdata.symbol)
        # print(f'pending bid: {len([order for order in pending_orders if order.side == OrderSide.BID])}')
        # print(f'pending ask: {len([order for order in pending_orders if order.side == OrderSide.ASK])}')
        if pending_orders:
            cancel_orders(pending_orders)
        else: 
            # 布林带策略
            # 获取vwap价格,这个价格处于最优ask,bid价格中间,并向量大的一侧偏移;用vwap的移动平均价格作为中枢
            vwap = vwap2(marketdata.p_ask_array[0],marketdata.q_ask_array[0],marketdata.p_bid_array[0],marketdata.q_bid_array[0])
            
            # 行情次数大于N,才开始得到布林带数据;
            if self.marketdata_counter > self.N: 
                boll_medium = np.mean(self.vwaps[-self.N:]) # 中枢
                kstd = self.k*np.array(self.vwaps[-self.N:]).std()
                boll_up = boll_medium + kstd # 布林上轨
                boll_down = boll_medium - kstd # 布林下轨
                self.boll_medium_list.append(boll_medium)
                self.boll_up_list.append(boll_up)
                self.boll_down_list.append(boll_down)
                
                if self.marketdata_counter > self.N + 1:
                    self.pnls.append(get_pnl(0))
                    try:
                        if self.pnls[-1] < self.pnls[-2] < self.pnls[-3]:
                            self.bid_qty = 5
                            self.ask_qty = 5
                            print("减小仓位")
                        if self.pnls[-1] > self.pnls[-2] > self.pnls[-3]:
                            self.bid_qty = 15
                            self.ask_qty = 15
                            print("增大仓位")
                    except: pass
                
                    best_bid = marketdata.p_bid_array[0]
                    best_ask = marketdata.p_ask_array[0]
                    try:
                        if self.boll_up_list[-1] > np.max(self.boll_up_list[-8:-2]):                
                            bid_order = create_order(marketdata.symbol, OrderType.LIMIT,OrderSide.BID, best_bid, self.bid_qty)
                            submit_orders(bid_order)
                            print("发出bid订单")
                      
                        elif self.boll_up_list[-1] < np.max(self.boll_up_list[-8:-2]):
                            ask_order = create_order(marketdata.symbol, OrderType.LIMIT, OrderSide.ASK, best_ask, self.ask_qty)
                            submit_orders(ask_order)
                            print("发出ask订单")
                        else:  #其他情况下平仓
                            inventory = get_inventory(marketdata.symbol, 0)
                            if inventory > 0:
                               order = create_order(marketdata.symbol, OrderType.MARKET, OrderSide.ASK, best_ask, inventory)
                            elif inventory < 0:
                               order = create_order(marketdata.symbol, OrderType.MARKET,OrderSide.BID,best_bid,-inventory)
                               submit_orders(order) 
                    except: pass
                           
                self.submited_in_marketdata += 1          
            self.vwaps.append(vwap)

        pass

    def on_receive_transaction(self, trade: ExecutedTrade):
        # logger.warning('on_receive_transaction')
        pass

    def on_submit_accepted(self, execution: Execution):
        # logger.warning('on_receive_transaction')
        if execution.side == OrderSide.BID:
            self.submited_bid_counter += 1
            # print(f'submited bid: {self.submited_bid_counter}')
        else:
            self.submited_ask_counter += 1
            # print(f'submited ask: {self.submited_ask_counter}')
        pass

    def on_submit_rejected(self, execution: Execution):
        # logger.warning('on_submit_rejected')
        pass

    def on_cancel_rejected(self, execution: Execution):
        # logger.warning('on_cancel_rejected')
        pass

    def on_order_partial_executed(self, execution: Execution):
        # logger.warning('on_order_partial_executed')
        pass

    def on_order_executed(self, execution: Execution):
        if execution.side == OrderSide.BID:
            self.executed_bid_counter += 1
            # print(f'executed bid: {self.executed_bid_counter}')
        else:
            self.executed_ask_counter += 1
            # print(f'executed ask: {self.executed_ask_counter}')
        # logger.warning(f'on_order_executed: {execution.order_id}')
        pass

    def on_order_cancelled(self, execution: Execution):

        pass

    def on_receive_status_update(self, repository: Repository):
        pass

    def on_receive_heartbeat(self, timestamp: int):
        pass

    def on_receive_timestamp(self, timestamp: int):
        pass

    def custom_settings(self):
        return {
            'START_TIME': 1522411140000,
            'END_TIME': 1522827000000, #endtimestamp 1522827000000,
            'HADOOP_MASTER_HOST': '',
            'HADOOP_IMPALA_HOST': '',
            'HADOOP_WORKER_HOSTS': [],
            'DEBUG': False,
            'DATASOURCE': 'local',
            'OUTPUT_REPORT': True,
            'LOCAL_DATA_PATH': {
                symbol: path
            },
            'SEND_MATCH_INFO': False,
            'PRECISION': {},
            'MARKETDATA_INTERVAL': 5000,  # in ms, 5seconds
            'ORDER_DELAY': 5,  # in ms
            'SEND_HEARTBEAT': False,
            'HEARTBEAT_INTERVAL': 1000,  # 1s
            'SPARK': False,
            'METADATA': [],
            'REPORTING_CURRENCY_MODE': 'low',  # high or low  OC先不考虑
            'REPORTING_CURRENCY': '',
            'TABLES_FOR_REPORTING': {},
            'ECON_DATA_USED': [],
            'RESULT_ID': 'top' ,
            "SYMBOL_TIME_TYPE": {'Ag(T+D)@sg': '1s'},
            "SYMBOL_MARKET_MODE": {'Ag(T+D)@sg': 'ODM'},
        }

# symbol = '180211.IB.1@CFETS'
# path = './180211.IB_ODM_10.1.csv'
symbol = 'Ag(T+D)@sg'
path = 'C:\\Users\\chenchao\\Desktop\\Trend_tracks\\Ag(T+D)_ODM_CLOB.csv'
def run():
    from finonelib.template.orderbook_pattern_template import BackTestPattern
    from finonelib.main_backtest import start_backtest
    from finonelib.state import ExecutorParams

    params = ExecutorParams(strategy_params={}, pattern_params={})
    state.initialize(Strategy,
                     BackTestPattern,
                     symbols=[symbol],
                     params=params,
                     settings={})
    start_backtest()
    from finonelib.methods import (plt_position_report,
                                  plt_total_pnl)
    for s in state.symbols:
        plt_position_report(s)
    # for symbol in state.symbols:
    #     plt_best_price(symbol)
    if len(state.symbols) > 1:
        plt_total_pnl()

if __name__ == '__main__':
    run()