from itertools import product
import pymongo
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pandas import DataFrame
from trader.constant import Status
from trader.object import TradeData, DbTickData, BacktestTickData

from abc import ABC
from collections import defaultdict
from datetime import datetime, timedelta, date
from typing import Any
import pandas as pd
from time import time
from .object import CancelRequest, OrderRequest
import json
from trader.utility import round_to_pricetick
from app.spreadTrading.stBase import (StLeg, StSpread)
from app.spreadTrading.stAlgo import SniperAlgo
from trader.constant import (Direction, Offset, Exchange, PriceType, Product)
from trader.object import ContractData
from app.backtest.base import EngineType
import gc

sns.set_style("whitegrid")


class OptimizationSetting:
    """
    Setting for runnning optimization.
    """

    def __init__(self):
        """"""
        self.params = {}
        self.target = ""

    def add_parameter(
            self, name: str, start: float, end: float = None, step: float = None
    ):
        """"""
        if not end and not step:
            self.params[name] = [start]
            return

        if start >= end:
            print("参数优化起始点必须小于终止点")
            return

        if step <= 0:
            print("参数优化步进必须大于0")
            return

        value = start
        value_list = []

        while value <= end:
            value_list.append(value)
            value += step

        self.params[name] = value_list

    def set_target(self, target: str):
        """"""
        self.target = target

    def generate_setting(self):
        """"""
        keys = self.params.keys()
        values = self.params.values()
        products = list(product(*values))

        settings = []
        for p in products:
            setting = dict(zip(keys, p))
            settings.append(setting)

        return settings


class BacktestMainEngine:
    """
    Acts as the core of VN Trader.
    """

    engine_type = EngineType.BACKTESTING
    gateway_name = "BACKTESTING"

    def __init__(self):
        """"""
        self.gateways = {}
        self.engines = {}
        self.apps = {}

        self.init_engines()

        """"""
        self.exchange = None
        self.start = None
        self.end = None
        self.rate = 0
        self.slippage = 0
        self.size = 1
        self.pricetick = 0
        self.capital = 1
        self.me = None  # 初始化主引擎

        self.strategy_class = None
        self.strategy = None
        self.tick = None
        self.bar = None
        self.datetime = None

        self.days = 0
        self.callback = None
        self.history_data = []

        self.limit_order_count = 0
        self.limit_orders = {}
        self.active_limit_orders = {}

        self.trade_count = 0
        self.trades = {}

        self.logs = []

        self.daily_results = {}
        self.daily_df = None

    def clear_data(self):
        """
        Clear all data of last backtesting.
        """
        self.strategy = None
        self.tick = None
        self.bar = None
        self.datetime = None

        self.limit_order_count = 0
        self.limit_orders.clear()
        self.active_limit_orders.clear()

        self.trade_count = 0
        self.trades.clear()

        self.logs.clear()
        self.daily_results.clear()

    def set_parameters(
            self,
            start: datetime,
            rate: float,
            slippage: float,
            size: float,
            pricetick: float,
            capital: int = 0,
            end: datetime = None,
    ):
        """"""
        self.rate = rate
        self.slippage = slippage
        self.size = size
        self.pricetick = pricetick
        self.start = start
        if capital:
            self.capital = capital

        if end:
            self.end = end

    def load_mongodb_his_data(self, active_vt_symbol, passive_vt_symbol, start, end):
        """载入历史数据"""
        db_client = pymongo.MongoClient('localhost', 27017)
        collection1 = db_client['VnTrader_Tick_Db'][active_vt_symbol]
        collection2 = db_client['VnTrader_Tick_Db'][passive_vt_symbol]

        self.output(u'开始载入数据')

        # 载入回测数据
        flt = {'datetime': {'$gte': start, '$lte': end}}
        backtest_cursor1 = collection1.find(flt).sort('datetime')
        backtest_cursor2 = collection2.find(flt).sort('datetime')
        # 将回测数据从查询指针中读取出，并生成列表
        for d in backtest_cursor1:
            data = BacktestTickData(gateway_name='OKEXF',
                                    symbol=active_vt_symbol.split('.')[0],
                                    exchange='.OKEX',
                                    vt_symbol=active_vt_symbol,
                                    datetime=d['datetime'],
                                    date=d['date'],
                                    time=d['time'],
                                    bid_price_1=d['bid_price_1'],
                                    ask_price_1=d['ask_price_1'],
                                    bid_volume_1=d['bid_volume_1'],
                                    ask_volume_1=d['ask_volume_1'])
            self.history_data.append(data)

        for d in backtest_cursor2:
            data = BacktestTickData(gateway_name='OKEXF',
                                    symbol=passive_vt_symbol.split('.')[0],
                                    exchange='.OKEX',
                                    vt_symbol=passive_vt_symbol,
                                    datetime=d['datetime'],
                                    date=d['date'],
                                    time=d['time'],
                                    bid_price_1=d['bid_price_1'],
                                    ask_price_1=d['ask_price_1'],
                                    bid_volume_1=d['bid_volume_1'],
                                    ask_volume_1=d['ask_volume_1'])
            self.history_data.append(data)

        self.history_data.sort(key=sort_datetime)

        self.output(u'载入完成: ')
        self.output(u'回测数据量： {}'.format(len(self.history_data)))

        self.output("策略初始化完成")

    def run_backtesting(self):
        self.output("开始回放历史数据")
        for data in self.history_data:
            self.new_tick(data)
        self.output("历史数据回放结束")

    def new_tick(self, tick: DbTickData):
        """"""
        self.tick = tick
        self.datetime = tick.datetime
        self.cross_limit_order()
        self.engines['St'].processTickEvent(tick)
        self.engines['St'].change_position()
        self.update_daily_close()

    def cross_limit_order(self):
        """
        Cross limit order with last bar/tick data.
        """
        long_cross_price = self.tick.ask_price_1
        short_cross_price = self.tick.bid_price_1
        long_best_price = long_cross_price
        short_best_price = short_cross_price

        for order in list(self.active_limit_orders.values()):
            # Push order update with status "not traded" (pending).
            if order.vt_symbol != self.tick.vt_symbol:
                continue

            # Check whether limit orders can be filled.
            buy_cross = (
                    order.direction == Direction.LONG and order.offset == Offset.OPEN
                    and order.price >= long_cross_price > 0
            )
            sell_cross = (
                    order.direction == Direction.LONG and order.offset == Offset.CLOSE
                    and 0 < order.price <= short_cross_price
            )

            short_cross = (
                    order.direction == Direction.SHORT and order.offset == Offset.OPEN
                    and 0 < order.price <= short_cross_price
            )
            cover_cross = (
                    order.direction == Direction.SHORT and order.offset == Offset.CLOSE
                    and order.price >= long_cross_price > 0
            )

            if not buy_cross and not sell_cross and not short_cross and not cover_cross:
                continue

            # Push order udpate with status "all traded" (filled).
            order.traded = order.volume
            order.status = Status.ALLTRADED

            self.active_limit_orders.pop(order.vt_client_oid)
            self.engines['St'].processOrderEvent(order)

            # Push trade update
            self.trade_count += 1

            if buy_cross:
                trade_price = min(order.price, long_best_price)
                long_qty = order.volume
                short_qty = 0

            if sell_cross:
                trade_price = max(order.price, short_best_price)
                long_qty = - order.volume
                short_qty = 0

            if short_cross:
                trade_price = max(order.price, short_best_price)
                long_qty = 0
                short_qty = order.volume

            if cover_cross:
                trade_price = min(order.price, long_best_price)
                long_qty = 0
                short_qty = - order.volume

            trade = TradeData(
                symbol=order.symbol,
                exchange=order.exchange,
                vt_client_oid=order.vt_client_oid,
                tradeid=str(self.trade_count),
                direction=order.direction,
                offset=order.offset,
                price=trade_price,
                volume=order.volume,
                long_qty=long_qty,
                short_qty=short_qty,
                time=self.datetime.strftime("%H:%M:%S"),
                gateway_name=self.gateway_name,
            )
            trade.datetime = self.datetime
            self.engines['St'].processTradeEvent(trade)
            self.trades[trade.vt_tradeid] = trade

    def send_limit_order(self, req: OrderRequest):
        """"""
        self.limit_order_count += 1
        req.status = Status.NOTTRADED
        req.gateway_name = self.gateway_name
        req.vt_client_oid = str(self.limit_order_count)

        self.active_limit_orders[req.vt_client_oid] = req
        self.limit_orders[req.vt_client_oid] = req

        return req.vt_client_oid

    def cancel_limit_order(self, vt_client_oid: str):
        """"""
        self.active_limit_orders.pop(vt_client_oid)

    def update_daily_close(self):
        """"""
        d = self.datetime.date()

        daily_result = self.daily_results.get(d, None)
        if daily_result:
            daily_result.close_price[self.tick.vt_symbol] = (self.tick.ask_price_1 + self.tick.bid_price_1) / 2
        else:
            self.daily_results[d] = DailyResult(d)

    def get_engine_type(self):
        """
        Return engine type.
        """
        return self.engine_type

    @staticmethod
    def output(msg):
        """
        Output message of backtesting engine.
        """
        print(f"{datetime.now()}\t{msg}")

    def calculate_result(self):
        """"""
        self.output("开始计算逐日盯市盈亏")
        if not self.trades:
            self.output("成交记录为空，无法计算")
            return
        # Add trade data into daily reuslt.
        for trade in self.trades.values():
            d = trade.datetime.date()
            daily_result = self.daily_results[d]
            daily_result.add_trade(trade)

        # Calculate daily result by iteration.
        pre_close = defaultdict(int)
        start_pos = defaultdict(int)

        for daily_result in self.daily_results.values():
            daily_result.calculate_pnl(
                pre_close, start_pos, self.size, self.rate, self.slippage)

            pre_close = daily_result.close_price
            start_pos = daily_result.end_pos
        # Generate dataframe
        results = defaultdict(list)
        for daily_result in self.daily_results.values():
            for key, value in daily_result.__dict__.items():
                results[key].append(value)
        self.daily_df = DataFrame.from_dict(results).set_index("date")
        self.daily_df.to_csv(r'F:\backtest_result\{}_daily_df.csv'.format(time()))
        self.output("逐日盯市盈亏计算完成")
        return self.daily_df

    def calculate_statistics(self, df: DataFrame = None):
        """"""
        self.output("开始计算策略统计指标")

        if not df:
            df = self.daily_df

        if df is None:
            # Set all statistics to 0 if no trade.
            start_date = ""
            end_date = ""
            total_days = 0
            profit_days = 0
            loss_days = 0
            end_balance = 0
            max_drawdown = 0
            max_ddpercent = 0
            total_net_pnl = 0
            daily_net_pnl = 0
            total_commission = 0
            daily_commission = 0
            total_slippage = 0
            daily_slippage = 0
            total_turnover = 0
            daily_turnover = 0
            total_trade_count = 0
            daily_trade_count = 0
            total_return = 0
            annual_return = 0
            daily_return = 0
            return_std = 0
            sharpe_ratio = 0
        else:
            # Calculate balance related time series data
            df["balance"] = df["net_pnl"].cumsum() + self.capital
            df["return"] = np.log(df["balance"] / df["balance"].shift(1)).fillna(0)
            df.iloc[0, -1] = np.log(df['balance'].tolist()[0] / self.capital)
            df["highlevel"] = (
                df["balance"].rolling(
                    min_periods=1, window=len(df), center=False).max()
            )
            df["drawdown"] = df["balance"] - df["highlevel"]
            df["ddpercent"] = df["drawdown"] / df["highlevel"] * 100

            # Calculate statistics value
            start_date = df.index[0]
            end_date = df.index[-1]

            total_days = len(df)
            profit_days = len(df[df["net_pnl"] > 0])
            loss_days = len(df[df["net_pnl"] < 0])

            end_balance = df["balance"].iloc[-1]
            max_drawdown = df["drawdown"].min()
            max_ddpercent = df["ddpercent"].min()

            total_net_pnl = df["net_pnl"].sum()
            daily_net_pnl = total_net_pnl / total_days

            total_commission = df["commission"].sum()
            daily_commission = total_commission / total_days

            total_slippage = df["slippage"].sum()
            daily_slippage = total_slippage / total_days

            total_turnover = df["turnover"].sum()
            daily_turnover = total_turnover / total_days

            total_trade_count = df["trade_count"].sum()
            daily_trade_count = total_trade_count / total_days

            total_return = (end_balance / self.capital - 1) * 100
            annual_return = total_return / total_days * 365
            daily_return = df["return"].mean() * 100
            return_std = df["return"].std() * 100

            if return_std:
                sharpe_ratio = daily_return / return_std * np.sqrt(365)
            else:
                sharpe_ratio = 0

        # Output
        self.output("-" * 30)
        self.output(f"首个交易日：\t{start_date}")
        self.output(f"最后交易日：\t{end_date}")

        self.output(f"总交易日：\t{total_days}")
        self.output(f"盈利交易日：\t{profit_days}")
        self.output(f"亏损交易日：\t{loss_days}")

        self.output(f"起始资金：\t{self.capital:,.2f}")
        self.output(f"结束资金：\t{end_balance:,.2f}")

        self.output(f"总收益率：\t{total_return:,.2f}%")
        self.output(f"年化收益：\t{annual_return:,.2f}%")
        self.output(f"最大回撤: \t{max_drawdown:,.2f}")
        self.output(f"百分比最大回撤: {max_ddpercent:,.2f}%")

        self.output(f"总盈亏：\t{total_net_pnl:,.2f}")
        self.output(f"总手续费：\t{total_commission:,.2f}")
        self.output(f"总滑点：\t{total_slippage:,.2f}")
        self.output(f"总成交金额：\t{total_turnover:,.2f}")
        self.output(f"总成交笔数：\t{total_trade_count}")

        self.output(f"日均盈亏：\t{daily_net_pnl:,.2f}")
        self.output(f"日均手续费：\t{daily_commission:,.2f}")
        self.output(f"日均滑点：\t{daily_slippage:,.2f}")
        self.output(f"日均成交金额：\t{daily_turnover:,.2f}")
        self.output(f"日均成交笔数：\t{daily_trade_count}")

        self.output(f"日均收益率：\t{daily_return:,.2f}%")
        self.output(f"收益标准差：\t{return_std:,.2f}%")
        self.output(f"Sharpe Ratio：\t{sharpe_ratio:,.2f}")

        statistics = {
            "start_date": start_date,
            "end_date": end_date,
            "total_days": total_days,
            "profit_days": profit_days,
            "loss_days": loss_days,
            "end_balance": end_balance,
            "max_drawdown": max_drawdown,
            "max_ddpercent": max_ddpercent,
            "total_net_pnl": total_net_pnl,
            "daily_net_pnl": daily_net_pnl,
            "total_commission": total_commission,
            "daily_commission": daily_commission,
            "total_slippage": total_slippage,
            "daily_slippage": daily_slippage,
            "total_turnover": total_turnover,
            "daily_turnover": daily_turnover,
            "total_trade_count": total_trade_count,
            "daily_trade_count": daily_trade_count,
            "total_return": total_return,
            "annual_return": annual_return,
            "daily_return": daily_return,
            "return_std": return_std,
            "sharpe_ratio": sharpe_ratio,
        }

        return statistics

    def save_chart(self, setting, df: DataFrame = None):
        """"""
        if not df:
            df = self.daily_df

        if df is None:
            return

        plt.figure(figsize=(10, 16))

        balance_plot = plt.subplot(4, 1, 1)
        balance_plot.set_title("Balance")
        df["balance"].plot(legend=True)

        drawdown_plot = plt.subplot(4, 1, 2)
        drawdown_plot.set_title("Drawdown")
        drawdown_plot.fill_between(range(len(df)), df["drawdown"].values)

        pnl_plot = plt.subplot(4, 1, 3)
        pnl_plot.set_title("Daily Pnl")
        df["net_pnl"].plot(kind="bar", legend=False, grid=False, xticks=[])

        distribution_plot = plt.subplot(4, 1, 4)
        distribution_plot.set_title("Daily Pnl Distribution")
        df["net_pnl"].hist(bins=50)
        file_name = 'F:\\backtest_result\\'
        for k, v in setting.items():
            file_name += str(k)
            file_name += '_'
            file_name += str(v)
        plt.savefig(file_name + '.png')

    def add_engine(self, engine_class: Any):
        """
        Add function engine.
        """
        engine = engine_class(self)
        self.engines[engine.engine_name] = engine

    def init_engines(self):
        """
        Init all engines.
        """
        self.add_engine(StEngine)


class BaseEngine(ABC):
    """
    Abstract class for implementing an function engine.
    """

    def __init__(
            self,
            backtest_main_engine: BacktestMainEngine,
            engine_name: str,
    ):
        """"""
        self.backtest_main_engine = backtest_main_engine
        self.engine_name = engine_name

    def close(self):
        """"""
        pass


class StEngine(BaseEngine):
    """价差引擎"""

    # ----------------------------------------------------------------------
    def __init__(self, backtest_main_engine: BacktestMainEngine):
        """"""
        super(StEngine, self).__init__(backtest_main_engine, "St")
        # 腿、价差相关字典
        self.algodict = {}  # spreadName:algo
        self.vt_symbol_algodict = {}  # vt_symbol:algo
        self.contracts = {}  # 保存所有合约信息
        self.orders = {}  # 保存所有订单信息
        self.week_dic = {}
        self.change_position_time = False
        self.add_function()

    # ----------------------------------------------------------------------
    def start(self):
        """开始交易"""
        # 清空合约信息
        self.close_all_contracts()
        # 创建合约、创建价差、订阅行情
        # 价差引擎初始化，同时change_position_time = False
        self.load_contracts()
        self.create_spread_algo()
        # 启动价差引擎开始交易
        for name in self.algodict:
            self.startAlgo(name)

        self.backtest_main_engine.run_backtesting()

    def add_function(self):
        """Add query function to main engine."""
        self.write_log = self.backtest_main_engine.output

    def close_all_contracts(self):
        """清空合约信息"""
        self.contracts = {}

    def load_contracts(self):
        filename = 'OKEXF_backtest_contract.csv'
        data = pd.read_csv(filename)
        for d in data.iterrows():
            d = d[1]
            contract = ContractData(
                symbol=d["instrument_id"],
                exchange=Exchange.OKEX.value,
                alias=d['alias'],
                name=d["instrument_id"],
                product=Product.FUTURES,
                pricetick=d["tick_size"],
                size=d["trade_increment"],
                underlying_index=d["underlying_index"],
                gateway_name=filename.split('_')[0],
            )
            self.contracts[contract.vt_symbol] = contract

    @staticmethod
    def get_end_datetime(start_date: datetime):
        while start_date.weekday() != 4:
            start_date += timedelta(days=1)
        start_date = start_date + timedelta(17 / 24)
        end_datetime = start_date + timedelta(7 - 1 / 24)
        # start_date = end_datetime - timedelta(1 / 24)  # 测试
        return start_date, end_datetime

    # ----------------------------------------------------------------------
    def create_spread_algo(self):
        """创建价差"""
        start = self.backtest_main_engine.start
        f = open('backtest_st_setting.json')
        l = json.load(f)
        for setting in l:
            # 检查价差重名
            if setting['name'] in self.algodict:
                self.write_log('{}价差存在重名'.format(setting['name']))
                return

            # 创建价差
            spread = StSpread()
            active_vt_symbol = setting['name'].split('+')[0]
            passive_vt_symbol = setting['name'].split('+')[1]
            setting['name'] = active_vt_symbol + '+' + passive_vt_symbol
            spread.name = setting['name']
            # spread.buy_percent = setting['buy_percent']
            # spread.short_percent = setting['short_percent']
            spread.buy_percent = -self.backtest_main_engine.setting['buy_percent']
            spread.short_percent = self.backtest_main_engine.setting['buy_percent']
            spread.sell_percent = setting['sell_percent']
            spread.cover_percent = setting['cover_percent']
            spread.maxOrderSize = setting['maxOrderSize']
            spread.maxPosSize = setting['maxPosSize']

            algo = SniperAlgo(self, spread)
            self.algodict[spread.name] = algo

            # 创建主动腿
            activeLeg = StLeg()
            activeLeg.vt_symbol = active_vt_symbol
            activeLeg.payup = float(setting['active_payup'])
            algo.spread.allLegs[activeLeg.vt_symbol] = activeLeg
            # vt_symbol和algo(spread)一一对应
            # 因为algo和spread在后面会一一对应
            self.vt_symbol_algodict[activeLeg.vt_symbol] = spread.name

            # 创建被动腿
            passiveLeg = StLeg()
            passiveLeg.vt_symbol = passive_vt_symbol
            passiveLeg.payup = float(setting['passive_payup'])
            algo.spread.allLegs[passiveLeg.vt_symbol] = passiveLeg
            self.vt_symbol_algodict[passiveLeg.vt_symbol] = spread.name

            # 初始化价差
            algo.spread.initSpread()

            # 订阅行情,即下载历史数据
            start, end = self.get_end_datetime(self.backtest_main_engine.start)
            # 设置下次开始交易时间
            self.backtest_main_engine.start = datetime(end.year, end.month, end.day)
            self.write_log('{}价差创建成功'.format(algo.spread.name))
            print('start: ', start, 'end: ', end)
            self.backtest_main_engine.load_mongodb_his_data(active_vt_symbol, passive_vt_symbol, start, end)

    # ----------------------------------------------------------------------
    def processTickEvent(self, tick):
        """处理行情推送"""
        # 检查行情是否需要处理
        # print(tick.__dict__)
        # 更新价差价格
        algo_name = self.vt_symbol_algodict[tick.vt_symbol]
        # 如果algo_name没有或已经被删除，直接返回
        if algo_name not in self.algodict:
            return
        algo = self.algodict[algo_name]
        spread = algo.spread
        leg = spread.allLegs[tick.vt_symbol]
        leg.bidPrice = tick.bid_price_1
        leg.askPrice = tick.ask_price_1
        leg.bidVolume = tick.bid_volume_1
        leg.askVolume = tick.ask_volume_1
        spread.calculatePrice()
        if not spread.bidPrice and not spread.askPrice:
            return
        # 如果是换仓时间，交易算法没有删除，主动腿平仓
        if self.change_position_time:
            algo.active_close_position()
        else:
            algo.updateSpreadTick()
        # if tick.datetime.weekday() == 4 and tick.datetime.hour + 8 == 20 and 32 < tick.datetime.minute < 36:
        if tick.datetime.weekday() == 4 and tick.datetime.hour == 15 and tick.datetime.minute > 30:
            self.change_position_time = True

    # ----------------------------------------------------------------------
    def processTradeEvent(self, trade):
        """处理成交事件"""
        # 更新价差价格
        algo_name = self.vt_symbol_algodict[trade.vt_symbol]
        # 如果algo_name没有或已经被删除，直接返回
        if algo_name not in self.algodict:
            return
        algo = self.algodict[algo_name]
        spread = algo.spread

        leg = spread.allLegs[trade.vt_symbol]

        # 更新腿持仓
        # 更新腿持仓
        leg.longPos += trade.long_qty
        leg.shortPos += trade.short_qty
        leg.netPos = leg.longPos - leg.shortPos
        spread.calculatePos()

    # ----------------------------------------------------------------------
    def processOrderEvent(self, order):
        """处理委托事件"""
        self.orders[order.vt_client_oid] = order
        algo_name = self.vt_symbol_algodict[order.vt_symbol]
        algo = self.algodict[algo_name]
        algo.updateOrder(order)

    # ----------------------------------------------------------------------
    def change_position(self):
        """"""
        if not len(self.algodict):
            # 换仓完成
            self.change_position_time = False
            self.backtest_main_engine.history_data.clear()
            gc.collect()
            if self.backtest_main_engine.start < self.backtest_main_engine.end:
                self.start()
            # if len(self.orders) < 15:
            #     self.start()

    def pop_spread_name(self, name: str):
        # 从algodict中删除该价差交易算法
        self.algodict.pop(name)

    # ----------------------------------------------------------------------
    def sendOrder(self, vt_symbol, direction, offset, price, volume, payup=0):
        """发单"""
        if vt_symbol in self.contracts:
            contract = self.contracts[vt_symbol]
        else:
            for k, v in self.contracts.items():
                if k[:3] == vt_symbol[:3]:
                    contract = self.contracts[k]
                    contract.vt_symbol = vt_symbol
                    contract.symbol = contract.vt_symbol.split('.')[0]
                    contract.name = contract.symbol
                    break
        req = OrderRequest(
            symbol=contract.symbol,
            exchange=contract.exchange,
            direction=direction,
            price_type=PriceType.LIMIT,
            volume=volume,
            price=price,
            offset=offset)

        if (direction == Direction.LONG and offset == Offset.OPEN) or (
                direction == Direction.SHORT and offset == Offset.CLOSE):
            req.price = price * (1 + payup / 100)
        else:
            req.price = price * (1 - payup / 100)
        req.price = round_to_pricetick(req.price, float(contract.pricetick))

        vt_orderid = self.backtest_main_engine.send_limit_order(req)

        return vt_orderid

    # ----------------------------------------------------------------------
    def cancel_order(self, vt_symbol, vt_client_oid):
        """撤单"""
        contract = self.contracts[vt_symbol]
        req = CancelRequest(
            vt_client_oid=vt_client_oid,
            symbol=contract.symbol,
            exchange=contract.exchange)
        self.backtest_main_engine.cancel_limit_order(req.vt_client_oid)

    # ----------------------------------------------------------------------
    def stopAll(self):
        """停止全部算法"""
        for algo in self.algodict.values():
            algo.stop()

    # ----------------------------------------------------------------------
    def startAlgo(self, spreadName):
        """启动算法"""
        algo = self.algodict[spreadName]
        algo.spread.first_query_position = True
        algoActive = algo.start()
        return algoActive

    # ----------------------------------------------------------------------
    def stopAlgo(self, spreadName):
        """停止算法"""
        algo = self.algodict[spreadName]
        algoActive = algo.stop()
        return algoActive

    # ----------------------------------------------------------------------
    def getAllAlgoParams(self):
        """获取所有算法的参数"""
        return [algo.getAlgoParams() for algo in self.algodict.values()]

    # ----------------------------------------------------------------------
    def setAlgoBuyPrice(self, spreadName, buyPrice):
        """设置算法买开价格"""
        algo = self.algodict[spreadName]
        algo.setBuyPrice(buyPrice)

    # ----------------------------------------------------------------------
    def setAlgoSellPrice(self, spreadName, sellPrice):
        """设置算法卖平价格"""
        algo = self.algodict[spreadName]
        algo.setSellPrice(sellPrice)

    # ----------------------------------------------------------------------
    def setAlgoShortPrice(self, spreadName, shortPrice):
        """设置算法卖开价格"""
        algo = self.algodict[spreadName]
        algo.setShortPrice(shortPrice)

    # ----------------------------------------------------------------------
    def setAlgoCoverPrice(self, spreadName, coverPrice):
        """设置算法买平价格"""
        algo = self.algodict[spreadName]
        algo.setCoverPrice(coverPrice)

    # ----------------------------------------------------------------------
    def setAlgoMode(self, spreadName, mode):
        """设置算法工作模式"""
        algo = self.algodict[spreadName]
        algo.setMode(mode)

    # ----------------------------------------------------------------------
    def setAlgoMaxOrderSize(self, spreadName, maxOrderSize):
        """设置算法单笔委托限制"""
        algo = self.algodict[spreadName]
        algo.setMaxOrderSize(maxOrderSize)

    # ----------------------------------------------------------------------
    def setAlgoMaxPosSize(self, spreadName, maxPosSize):
        """设置算法持仓限制"""
        algo = self.algodict[spreadName]
        algo.setMaxPosSize(maxPosSize)


class DailyResult:
    """"""

    def __init__(self, date: date):
        """"""
        self.date = date
        self.close_price = defaultdict(int)
        self.pre_close = defaultdict(int)

        self.trades = []
        self.trade_count = 0

        self.start_pos = {}
        self.end_pos = {}

        self.turnover = 0
        self.commission = 0
        self.slippage = 0

        self.trading_pnl = 0
        self.holding_pnl = 0
        self.total_pnl = 0
        self.net_pnl = 0

    def add_trade(self, trade: TradeData):
        """"""
        self.trades.append(trade)

    def calculate_pnl(
            self,
            pre_close: dict,
            start_pos: dict,
            size: int,
            rate: float,
            slippage: float,
    ):
        """"""
        self.pre_close = pre_close

        # Holding pnl is the pnl from holding position at day start
        self.start_pos = start_pos
        self.end_pos = start_pos
        for vt_symbol in self.close_price.keys():
            if self.start_pos[vt_symbol] != 0:
                self.holding_pnl += self.start_pos[vt_symbol] * \
                                    (1 / self.pre_close[vt_symbol] - 1 / self.close_price[vt_symbol]) * size

        # Trading pnl is the pnl from new trade during the day
        self.trade_count = len(self.trades)

        pos_change = defaultdict(int)
        for trade in self.trades:
            if (trade.direction == Direction.LONG and trade.offset == Offset.OPEN) or \
                    (trade.direction == Direction.SHORT and trade.offset == Offset.CLOSE):
                pos_change[trade.vt_symbol] = trade.volume
            else:
                pos_change[trade.vt_symbol] = -trade.volume

            turnover = trade.volume * size / trade.price

            self.trading_pnl += pos_change[trade.vt_symbol] * (
                    1 / trade.price - 1 / self.close_price[trade.vt_symbol]) * size
            self.end_pos[trade.vt_symbol] += pos_change[trade.vt_symbol]
            self.turnover += turnover
            self.commission += turnover * rate
            self.slippage += trade.volume * size * slippage

        # Net pnl takes account of commission and slippage cost
        self.total_pnl = self.trading_pnl + self.holding_pnl
        self.net_pnl = self.total_pnl - self.commission - self.slippage


def sort_datetime(elem):
    return elem.datetime
