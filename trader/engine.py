"""
"""

import logging
import smtplib
import time
from abc import ABC
from collections import defaultdict
from datetime import datetime, timedelta
from email.message import EmailMessage
from queue import Empty, Queue
from threading import Thread
from typing import Any
import pandas as pd
from event import Event, EventEngine
from .app import BaseApp
from .gateway import BaseGateway
from .object import CancelRequest, StLogData, OrderRequest, SubscribeRequest
from .setting import SETTINGS
from .utility import Singleton, get_temp_path

import json
from trader.utility import round_to_pricetick
from trader.event import (EVENT_TICK, EVENT_TRADE, EVENT_POSITION,
                          EVENT_TIMER, EVENT_ORDER,
                          EVENT_ACCOUNT, EVENT_CONTRACT, EVENT_LOG)

from app.spreadTrading.stBase import (StLeg, StSpread, EVENT_SPREADTRADING_ALGO)
from app.spreadTrading.stAlgo import SniperAlgo
from trader.constant import *
from trader.object import ContractData
from trader.utility import DBEngine

DB_NAME = HuobiDB.DB_NAME.value
DB_STRATEGY_POSITION = HuobiDB.DB_STRATEGY_POSITION.value

class MainEngine:
    """
    Acts as the core of VN Trader.
    """

    def __init__(self, event_engine: EventEngine = None):
        """"""
        if event_engine:
            self.event_engine = event_engine
        else:
            self.event_engine = EventEngine()
        self.event_engine.start()

        self.gateways = {}
        self.engines = {}
        self.apps = {}

        self.init_engines()

    def add_engine(self, engine_class: Any):
        """
        Add function engine.
        """
        engine = engine_class(self, self.event_engine)
        self.engines[engine.engine_name] = engine

    def add_gateway(self, gateway_class: BaseGateway):
        """
        Add gateway.
        """
        gateway = gateway_class(self.event_engine)
        self.gateways[gateway.gateway_name] = gateway

    def add_app(self, app_class: BaseApp):
        """
        Add app.
        """
        app = app_class()
        self.apps[app.app_name] = app

        self.add_engine(app.engine_class)

    def init_engines(self):
        """
        Init all engines.
        """
        self.add_engine(LogEngine)
        # self.add_engine(OmsEngine)
        # self.add_engine(EmailEngine)
        self.add_engine(StEngine)

    def write_log(self, msg: str):
        """
        Put log event with specific message.
        """
        log = StLogData(msg=msg)
        event = Event(EVENT_LOG, log)
        self.event_engine.put(event)

    def get_gateway(self, gateway_name: str):
        """
        Return gateway object by name.
        """
        gateway = self.gateways.get(gateway_name, None)
        if not gateway:
            self.write_log(f"找不到底层接口：{gateway_name}")
        return gateway

    def get_engine(self, engine_name: str):
        """
        Return engine object by name.
        """
        engine = self.engines.get(engine_name, None)
        if not engine:
            self.write_log(f"找不到引擎：{engine_name}")
        return engine

    def get_default_setting(self, gateway_name: str):
        """
        Get default setting dict of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.get_default_setting()
        return None

    def get_all_gateway_names(self):
        """
        Get all names of gatewasy added in main engine.
        """
        return list(self.gateways.keys())

    def get_all_apps(self):
        """
        Get all app objects.
        """
        return list(self.apps.values())

    def query_contract(self, gateway_name: str):
        """
        Get contract of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.query_contract()

    def query_position(self, strategy_name: str,symbol: str, gateway_name: str):
        """
        Get position of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.query_position(strategy_name,symbol)

    def connect(self, setting: dict, gateway_name: str):
        """
        Start connection of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.connect(gateway.default_setting)

    def subscribe(self, req: SubscribeRequest, gateway_name: str):
        """
        Subscribe tick data update of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.subscribe(req)

    def un_subscribe(self, req: SubscribeRequest, gateway_name: str):
        """
        unSubscribe tick data update of a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.un_subscribe(req)

    def send_order(self, req: OrderRequest, gateway_name: str):
        """
        Send new order request to a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            return gateway.send_order(req)
        else:
            return ""

    def cancel_order(self, req: CancelRequest, gateway_name: str):
        """
        Send cancel order request to a specific gateway.
        """
        gateway = self.get_gateway(gateway_name)
        if gateway:
            gateway.cancel_order(req)

    def close(self):
        """
        Make sure every gateway and app is closed properly before
        programme exit.
        """
        # Stop event engine first to prevent new timer event.
        self.event_engine.stop()

        for engine in self.engines.values():
            engine.close()

        for gateway in self.gateways.values():
            gateway.close()


class BaseEngine(ABC):
    """
    Abstract class for implementing an function engine.
    """

    def __init__(
            self,
            main_engine: MainEngine,
            event_engine: EventEngine,
            engine_name: str,
    ):
        """"""
        self.main_engine = main_engine
        self.event_engine = event_engine
        self.engine_name = engine_name

    def close(self):
        """"""
        pass


class LogEngine(BaseEngine):
    """
    Processes log event and output with logging module.
    """

    __metaclass__ = Singleton

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(LogEngine, self).__init__(main_engine, event_engine, "log")

        if not SETTINGS["log.active"]:
            return

        self.level = SETTINGS["log.level"]
        self.logger = logging.getLogger("VN Trader")
        self.formatter = logging.Formatter(
            "%(asctime)s  %(levelname)s: %(message)s"
        )

        self.add_null_handler()

        if SETTINGS["log.console"]:
            self.add_console_handler()

        if SETTINGS["log.file"]:
            self.add_file_handler()

        self.register_event()

    def add_null_handler(self):
        """
        Add null handler for logger.
        """
        null_handler = logging.NullHandler()
        self.logger.addHandler(null_handler)

    def add_console_handler(self):
        """
        Add console output of log.
        """
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.level)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)

    def add_file_handler(self):
        """
        Add file output of log.
        """
        today_date = datetime.now().strftime("%Y%m%d")
        filename = f"vt_{today_date}.log"
        file_path = get_temp_path(filename)

        file_handler = logging.FileHandler(
            file_path, mode="w", encoding="utf8"
        )
        file_handler.setLevel(self.level)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def register_event(self):
        """"""
        self.event_engine.register(EVENT_LOG, self.process_log_event)

    def process_log_event(self, event: Event):
        """
        Output log event data with logging function.
        """
        log = event.data
        # self.logger.log(log.level, log.msg)
        print('+++++++++++++++++++++++++++++++++++++++++++++++++')
        print(log.time, ' ', log.msg)


class OmsEngine(BaseEngine):
    """
    Provides order management system function for VN Trader.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(OmsEngine, self).__init__(main_engine, event_engine, "oms")

        self.ticks = {}
        self.orders = {}
        self.trades = {}
        self.positions = {}
        self.accounts = {}
        self.contracts = {}
        self.active_orders = {}

        self.add_function()
        self.register_event()

    def add_function(self):
        """Add query function to main engine."""
        self.main_engine.get_tick = self.get_tick
        self.main_engine.get_order = self.get_order
        self.main_engine.get_position = self.get_position
        self.main_engine.get_account = self.get_account
        self.main_engine.get_contract = self.get_contract
        self.main_engine.get_all_ticks = self.get_all_ticks
        self.main_engine.get_all_orders = self.get_all_orders
        self.main_engine.get_all_trades = self.get_all_trades
        self.main_engine.get_all_positions = self.get_all_positions
        self.main_engine.get_all_accounts = self.get_all_accounts
        self.main_engine.get_all_contracts = self.get_all_contracts
        self.main_engine.close_all_contracts = self.close_all_contracts
        self.main_engine.get_all_active_orders = self.get_all_active_orders

    def register_event(self):
        """"""
        pass
        # self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_ORDER, self.process_order_event)
        # self.event_engine.register(EVENT_TRADE, self.process_trade_event)
        # self.event_engine.register(EVENT_POSITION, self.process_position_event)
        # self.event_engine.register(EVENT_ACCOUNT, self.process_account_event)
        # self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)

    def process_tick_event(self, event: Event):
        """"""
        tick = event.data
        self.ticks[tick.vt_symbol] = tick

    def process_order_event(self, event: Event):
        """"""
        order = event.data
        self.orders[order.vt_client_oid] = order

        # If order is active, then update data in dict.
        if order.is_active():
            self.active_orders[order.vt_client_oid] = order
        # Otherwise, pop inactive order from in dict
        elif order.vt_client_oid in self.active_orders:
            self.active_orders.pop(order.vt_client_oid)

    def process_trade_event(self, event: Event):
        """"""
        trade = event.data
        self.trades[trade.vt_tradeid] = trade

    def process_position_event(self, event: Event):
        """"""
        position = event.data
        self.positions[position.vt_positionid] = position

    def process_account_event(self, event: Event):
        """"""
        account = event.data
        self.accounts[account.vt_accountid] = account

    def process_contract_event(self, event: Event):
        """"""
        contract = event.data
        self.contracts[contract.vt_symbol] = contract

    def get_tick(self, vt_symbol):
        """
        Get latest market tick data by vt_symbol.
        """
        return self.ticks.get(vt_symbol, None)

    def get_order(self, vt_orderid):
        """
        Get latest order data by vt_orderid.
        """
        return self.orders.get(vt_orderid, None)

    def get_trade(self, vt_tradeid):
        """
        Get trade data by vt_tradeid.
        """
        return self.trades.get(vt_tradeid, None)

    def get_position(self, vt_positionid):
        """
        Get latest position data by vt_positionid.
        """
        return self.positions.get(vt_positionid, None)

    def get_account(self, vt_accountid):
        """
        Get latest account data by vt_accountid.
        """
        return self.accounts.get(vt_accountid, None)

    def get_contract(self, vt_symbol):
        """
        Get contract data by vt_symbol.
        """
        return self.contracts.get(vt_symbol, None)

    def get_all_ticks(self):
        """
        Get all tick data.
        """
        return list(self.ticks.values())

    def get_all_orders(self):
        """
        Get all order data.
        """
        return list(self.orders.values())

    def get_all_trades(self):
        """
        Get all trade data.
        """
        return list(self.trades.values())

    def get_all_positions(self):
        """
        Get all position data.
        """
        return list(self.positions.values())

    def get_all_accounts(self):
        """
        Get all account data.
        """
        return list(self.accounts.values())

    def get_all_contracts(self):
        """
        Get all contract data.
        """
        return list(self.contracts.values())

    def close_all_contracts(self):
        """
        Close all contract data.
        """
        self.contracts = {}

    def get_all_active_orders(self, vt_symbol: str = ""):
        """
        Get all active orders by vt_symbol.

        If vt_symbol is empty, return all active orders.
        """
        if not vt_symbol:
            return list(self.active_orders.values())
        else:
            active_orders = [
                order
                for order in self.active_orders.values()
                if order.vt_symbol == vt_symbol
            ]
            return active_orders


class EmailEngine(BaseEngine):
    """
    Provides email sending function for VN Trader.
    """

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(EmailEngine, self).__init__(main_engine, event_engine, "email")

        self.thread = Thread(target=self.run)
        self.queue = Queue()
        self.active = False

        self.main_engine.send_email = self.send_email

    def send_email(self, subject: str, content: str, receiver: str = ""):
        """"""
        # Start email engine when sending first email.
        if not self.active:
            self.start()

        # Use default receiver if not specified.
        if not receiver:
            receiver = SETTINGS["email.receiver"]

        msg = EmailMessage()
        msg["From"] = SETTINGS["email.sender"]
        msg["To"] = SETTINGS["email.receiver"]
        msg["Subject"] = subject
        msg.set_content(content)

        self.queue.put(msg)

    def run(self):
        """"""
        while self.active:
            try:
                msg = self.queue.get(block=True, timeout=1)

                with smtplib.SMTP_SSL(
                        SETTINGS["email.server"], SETTINGS["email.port"]
                ) as smtp:
                    smtp.login(
                        SETTINGS["email.username"], SETTINGS["email.password"]
                    )
                    smtp.send_message(msg)
            except Empty:
                pass

    def start(self):
        """"""
        self.active = True
        self.thread.start()

    def close(self):
        """"""
        if not self.active:
            return

        self.active = False
        self.thread.join()


class StEngine(BaseEngine):
    """价差引擎"""

    # ----------------------------------------------------------------------
    def __init__(self, main_engine: MainEngine, event_engine: EventEngine):
        """"""
        super(StEngine, self).__init__(main_engine, event_engine, "St")
        # 腿、价差相关字典
        self.algodict = {}  # spreadName:algo
        self.vt_symbol_algodict = {}  # vt_symbol:algo
        self.contracts = {}  # 保存所有合约信息
        self.orders = {}     # 保存所有订单信息

        self.change_position_time = False
        self.register_event()
        self.add_function()
        self.dbEngine = DBEngine()

    # ----------------------------------------------------------------------
    def start(self):
        """开始交易"""
        # 清空合约信息
        self.close_all_contracts()
        # 查询合约，保存数据
        self.main_engine.query_contract('HUOBIF')
        # 等待10s是为了等'OKEXF_contract.csv'文件保存好，再下载合约
        time.sleep(10)
        self.load_contracts()
        # 创建合约、创建价差、订阅行情
        self.create_spread_algo()
        # 启动价差引擎开始交易
        for name in self.algodict:
            self.startAlgo(name)

    def add_function(self):
        """Add query function to main engine."""
        self.write_log = self.main_engine.write_log

    def close_all_contracts(self):
        """清空合约信息"""
        self.contracts = {}

    def load_contracts(self):
        filename = 'HUOBIF_contract.csv'
        data = pd.read_csv(filename)
        dic = {"quarter" : "_CQ", "next_week":"_NW", "this_week": "_CW"}
        for d in data.iterrows():
            d = eval(d[1]['data'])
            contract = ContractData(
                symbol=d['symbol'] + dic[d['contract_type']],
                exchange=Exchange.HUOBI.value,
                alias=d['contract_type'],
                name=d["contract_code"],
                product=Product.FUTURES,
                pricetick=d["price_tick"],
                size=d["contract_size"],
                underlying_index=d["symbol"],
                gateway_name=filename.split('_')[0],
            )
            self.contracts[contract.vt_symbol] = contract

    # ----------------------------------------------------------------------
    def get_vt_symbol(self, symbol):
        """创建当周、次周、季度合约日期"""
        dic = {"CQ": "quarter", "NW": "next_week", "CW": "this_week"}
        underlying_index = symbol.split('_')[0]
        alias = symbol.split('_')[1].split('.')[0]
        exchange = symbol.split('_')[1].split('.')[1]
        for k, v in self.contracts.items():
            if v.underlying_index == underlying_index and v.alias == dic[alias] and v.exchange == exchange:
                return k

    # ----------------------------------------------------------------------
    def create_spread_algo(self):
        """创建价差"""
        f = open('ST_setting.json')
        l = json.load(f)
        for setting in l:
            # 检查价差重名
            if setting['name'] in self.algodict:
                self.write_log('{}价差存在重名'.format(setting['name']))
                return

            # 创建价差
            spread = StSpread()
            if 'stSpread' in setting.keys():
                module = __import__('app.spreadTrading.stBase', fromlist=True)
                spread = getattr(module, setting['stSpread'])
                spread = spread()
            else:
                print('没有配置stSpread,使用默认类')
            active_vt_symbol = setting['name'].split('+')[0]
            passive_vt_symbol = setting['name'].split('+')[1]
            active_vt_symbol = self.get_vt_symbol(active_vt_symbol)
            passive_vt_symbol = self.get_vt_symbol(passive_vt_symbol)
            spread.name = setting['name']
            spread.buy_percent = setting['buy_percent']
            spread.sell_percent = setting['sell_percent']
            spread.cover_percent = setting['cover_percent']
            spread.short_percent = setting['short_percent']
            spread.maxOrderSize = setting['maxOrderSize']
            spread.maxPosSize = setting['maxPosSize']

            algo = SniperAlgo(self, spread)
            if 'stAlgo' in setting.keys():
                module = __import__('app.spreadTrading.stAlgo', fromlist=True)
                stAlgo = getattr(module, setting['stAlgo'])
                algo = stAlgo(self,spread)
            else:
                print('没有配置stAlgo,使用默认类')
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

            # 订阅行情
            self.subscribe(spread.name)

            # 查询持仓
            algo.query_position()
            self.write_log('{}价差创建成功'.format(algo.spread.name))

        self.write_log('价差配置加载完成')

    # ----------------------------------------------------------------------
    def register_event(self):
        """注册事件监听"""
        self.event_engine.register(EVENT_TICK, self.processTickEvent)
        self.event_engine.register(EVENT_ORDER, self.processOrderEvent)
        self.event_engine.register(EVENT_POSITION, self.processPosEvent)
        self.event_engine.register(EVENT_TIMER, self.processTimerEvent)

    # ----------------------------------------------------------------------
    def processTickEvent(self, event):
        """处理行情推送"""
        # 检查行情是否需要处理
        tick = event.data
        # print(tick.__dict__)
        for algo in list(self.algodict.values()):
            spread = algo.spread
            if tick.vt_symbol in spread.allLegs.keys():
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

        if tick.datetime.weekday() == 4 and tick.datetime.hour == 15 and tick.datetime.minute > 30:
            self.change_position_time = True

    # ----------------------------------------------------------------------
    def processPosEvent(self, event):
        """处理持仓推送"""
        # 检查持仓是否需要处理
        pos = event.data
        algo_name = pos.strategy_name
        # 如果algo_name没有或已经被删除，直接返回
        if algo_name not in self.algodict:
            return
        algo = self.algodict[algo_name]
        spread = algo.spread

        leg = spread.allLegs[pos.vt_symbol]

        # 更新腿持仓
        leg.longPos = pos.long_qty
        leg.shortPos = pos.short_qty
        leg.netPos = leg.longPos - leg.shortPos

        spread.calculatePos()
        flt={'strategy_name' : spread.name}
        d = {
            'strategy_name' : spread.name,
            'longPos':spread.longPos,
            'shortPos':spread.shortPos,
            'netPos':spread.netPos}
        self.dbEngine.dbUpdate(DB_NAME,DB_STRATEGY_POSITION,d,flt,True)
    # ----------------------------------------------------------------------
    def processTradeEvent(self, event):
        """处理成交事件"""
        pass

    # ----------------------------------------------------------------------
    def processOrderEvent(self, event):
        """处理委托事件"""
        order = event.data
        # print('on_order',order)
        self.orders[order.vt_client_oid] = order
        algo_name = order.strategy_name
        algo = self.algodict[algo_name]
        algo.updateOrder(order)

    # ----------------------------------------------------------------------
    def processTimerEvent(self, event):
        """"""
        for algo in list(self.algodict.values()):
            algo.updateTimer()

        if self.change_position_time and not len(self.algodict):
            dt = datetime.today()
            # if dt.weekday() == 4 and dt.hour == 20 and dt.minute >= 40:
            if dt.weekday() == 4 and dt.hour == 16 and dt.minute >= 40:
                self.change_position_time = False
                self.start()

    def subscribe(self, name: str):
        # 订阅行情
        active_vt_symbol = name.split('+')[0]
        passive_vt_symbol = name.split('+')[1]
        contract = self.contracts[active_vt_symbol]
        # 订阅主动腿行情
        req = SubscribeRequest(contract.symbol, contract.exchange)
        self.main_engine.subscribe(req, contract.gateway_name)

        contract = self.contracts[passive_vt_symbol]
        # 订阅被动腿行情
        req = SubscribeRequest(contract.symbol, contract.exchange)
        self.main_engine.subscribe(req, contract.gateway_name)

    def pop_spread_name(self, name: str):
        # 从algodict中删除该价差交易算法
        self.algodict.pop(name)

    # ----------------------------------------------------------------------
    def sendOrder(self, vt_symbol, direction, offset, price, volume, payup=0,name=''):
        """发单"""
        contract = self.contracts[vt_symbol]
        if not contract:
            return ''
        req = OrderRequest(
            symbol=contract.symbol,
            contract = contract,
            strategy_name = name,
            exchange=contract.exchange,
            direction=direction,
            price_type=PriceType.LIMIT,
            volume=volume,
            price=price,
            offset=offset,
        )

        if direction == Direction.LONG:
            req.price = price * (1 + payup / 100)
        else:
            req.price = price * (1 - payup / 100)
        req.price = round_to_pricetick(req.price, float(contract.pricetick))

        vt_orderid = self.main_engine.send_order(req, contract.gateway_name)

        return vt_orderid

    # ----------------------------------------------------------------------
    def cancel_order(self, vt_symbol, vt_client_oid):
        """撤单"""
        contract = self.contracts[vt_symbol]
        req = CancelRequest(
            vt_client_oid=vt_client_oid,
            symbol=contract.symbol,
            exchange=contract.exchange)
        self.main_engine.cancel_order(req, contract.gateway_name)

    # ----------------------------------------------------------------------
    def query_position(self,strategy_name,vt_symbol):
        """查询持仓"""
        contract = self.contracts[vt_symbol]
        self.main_engine.query_position(strategy_name,vt_symbol, contract.gateway_name)

    # ----------------------------------------------------------------------
    def putAlgoEvent(self, algo):
        """发出算法状态更新事件"""
        event = Event(EVENT_SPREADTRADING_ALGO + algo.name)
        self.event_engine.put(event)

    # ----------------------------------------------------------------------
    def stopAll(self):
        """停止全部算法"""
        for algo in self.algodict.values():
            algo.stop()

    # ----------------------------------------------------------------------
    def startAlgo(self, spreadName):
        """启动算法"""
        algo = self.algodict[spreadName]
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


