"""
General utility functions.
"""
import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
import shelve
from pathlib import Path
from typing import Callable
from decimal import Decimal
import numpy as np
# import talib
from pymongo import MongoClient, ASCENDING
from trader.object import BarData, TickData


class Singleton(type):
    """
    Singleton metaclass, 

    class A:
        __metaclass__ = Singleton

    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """"""
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(
                *args, **kwargs
            )

        return cls._instances[cls]


def get_trader_path():
    """
    Get path where trader is running in.
    """
    home_path = Path.home()
    return home_path


def get_temp_path(filename: str):
    """
    Get path for temp file with filename.
    """
    trader_path = get_trader_path()
    temp_path = trader_path.joinpath(".vntrader")

    if not temp_path.exists():
        temp_path.mkdir()

    return temp_path.joinpath(filename)


def get_icon_path(filepath: str, ico_name: str):
    """
    Get path for icon file with ico name.
    """
    ui_path = Path(filepath).parent
    icon_path = ui_path.joinpath("ico", ico_name)
    return str(icon_path)


def load_setting(filename: str):
    """
    Load setting from shelve file in temp path.
    """
    filepath = get_temp_path(filename)
    f = shelve.open(str(filepath))
    setting = dict(f)
    f.close()
    return setting


def save_setting(filename: str, setting: dict):
    """
    Save setting into shelve file in temp path.
    """
    filepath = get_temp_path(filename)
    f = shelve.open(str(filepath))
    for k, v in setting.items():
        f[k] = v
    f.close()


def round_to_pricetick(price: float, pricetick: float):
    """
    Round price to price tick value.
    """
    tickDec = Decimal(str(pricetick))
    return float((Decimal(round(price / pricetick, 0)) * tickDec))


class BarGenerator:
    """
    For: 
    1. generating 1 minute bar data from tick data
    2. generateing x minute bar data from 1 minute data
    """

    def __init__(
        self, on_bar: Callable, xmin: int = 0, on_xmin_bar: Callable = None
    ):
        """Constructor"""
        self.bar = None
        self.on_bar = on_bar

        self.xmin = xmin
        self.xmin_bar = None
        self.on_xmin_bar = on_xmin_bar

        self.last_tick = None

    def update_tick(self, tick: TickData):
        """
        Update new tick data into generator.
        """
        new_minute = False

        if not self.bar:
            self.bar = BarData()
            new_minute = True
        elif self.bar.datetime.minute != tick.datetime.minute:
            self.bar.datetime = self.bar.datetime.replace(
                second=0, microsecond=0
            )
            self.on_bar(self.bar)

            self.bar = BarData()
            new_minute = True

        if new_minute:
            self.bar.vt_symbol = tick.vt_symbol
            self.bar.symbol = tick.symbol
            self.bar.exchange = tick.exchange

            self.bar.open = tick.last_price
            self.bar.high = tick.last_price
            self.bar.low = tick.last_price
        else:
            self.bar.high = max(self.bar.high, tick.last_price)
            self.bar.low = min(self.bar.low, tick.last_price)

        self.bar.close = tick.last_price
        self.bar.datetime = tick.datetime

        if self.last_tick:
            volume_change = tick.volume - self.last_tick.volume
            self.bar.volume += max(volume_change, 0)

        self.last_tick = tick

    def update_bar(self, bar: BarData):
        """
        Update 1 minute bar into generator
        """
        if not self.xmin_bar:
            self.xmin_bar = BarData()

            self.xmin_bar.vt_symbol = bar.vt_symbol
            self.xmin_bar.symbol = bar.symbol
            self.xmin_bar.exchange = bar.exchange

            self.xmin_bar.open = bar.open
            self.xmin_bar.high = bar.high
            self.xmin_bar.low = bar.low

            self.xmin_bar.datetime = bar.datetime
        else:
            self.xmin_bar.high = max(self.xmin_bar.high, bar.high)
            self.xmin_bar.low = min(self.xmin_bar.low, bar.low)

        self.xmin_bar.close = bar.close
        self.xmin_bar.volume += int(bar.volume)

        if not (bar.datetime.minute + 1) % self.xmin:
            self.xmin_bar.datetime = self.xmin_bar.datetime.replace(
                second=0, microsecond=0
            )
            self.on_xmin_bar(self.xmin_bar)

            self.xmin_bar = None

    def generate(self):
        """
        Generate the bar data and call callback immediately.
        """
        self.on_bar(self.bar)
        self.bar = None


class ArrayManager(object):
    """
    For:
    1. time series container of bar data
    2. calculating technical indicator value
    """

    def __init__(self, size=100):
        """Constructor"""
        self.count = 0
        self.size = size
        self.inited = False

        self.open_array = np.zeros(size)
        self.high_array = np.zeros(size)
        self.low_array = np.zeros(size)
        self.close_array = np.zeros(size)
        self.volume_array = np.zeros(size)

    # def update_bar(self, bar):
    #     """
    #     Update new bar data into array manager.
    #     """
    #     self.count += 1
    #     if not self.inited and self.count >= self.size:
    #         self.inited = True
    #
    #     self.open_array[:-1] = self.open_array[1:]
    #     self.high_array[:-1] = self.high_array[1:]
    #     self.low_array[:-1] = self.low_array[1:]
    #     self.close_array[:-1] = self.close_array[1:]
    #     self.volume_array[:-1] = self.volume_array[1:]
    #
    #     self.open_array[-1] = bar.open_price
    #     self.high_array[-1] = bar.high_price
    #     self.low_array[-1] = bar.low_price
    #     self.close_array[-1] = bar.close_price
    #     self.volume_array[-1] = bar.volume
    #
    # @property
    # def open(self):
    #     """
    #     Get open price time series.
    #     """
    #     return self.open_array
    #
    # @property
    # def high(self):
    #     """
    #     Get high price time series.
    #     """
    #     return self.high_array
    #
    # @property
    # def low(self):
    #     """
    #     Get low price time series.
    #     """
    #     return self.low_array
    #
    # @property
    # def close(self):
    #     """
    #     Get close price time series.
    #     """
    #     return self.close_array
    #
    # @property
    # def volume(self):
    #     """
    #     Get trading volume time series.
    #     """
    #     return self.volume_array
    #
    # def sma(self, n, array=False):
    #     """
    #     Simple moving average.
    #     """
    #     result = talib.SMA(self.close, n)
    #     if array:
    #         return result
    #     return result[-1]
    #
    # def std(self, n, array=False):
    #     """
    #     Standard deviation
    #     """
    #     result = talib.STDDEV(self.close, n)
    #     if array:
    #         return result
    #     return result[-1]
    #
    # def cci(self, n, array=False):
    #     """
    #     Commodity Channel Index (CCI).
    #     """
    #     result = talib.CCI(self.high, self.low, self.close, n)
    #     if array:
    #         return result
    #     return result[-1]
    #
    # def atr(self, n, array=False):
    #     """
    #     Average True Range (ATR).
    #     """
    #     result = talib.ATR(self.high, self.low, self.close, n)
    #     if array:
    #         return result
    #     return result[-1]
    #
    # def rsi(self, n, array=False):
    #     """
    #     Relative Strenght Index (RSI).
    #     """
    #     result = talib.RSI(self.close, n)
    #     if array:
    #         return result
    #     return result[-1]
    #
    # def macd(self, fast_period, slow_period, signal_period, array=False):
    #     """
    #     MACD.
    #     """
    #     macd, signal, hist = talib.MACD(
    #         self.close, fast_period, slow_period, signal_period
    #     )
    #     if array:
    #         return macd, signal, hist
    #     return macd[-1], signal[-1], hist[-1]
    #
    # def adx(self, n, array=False):
    #     """
    #     ADX.
    #     """
    #     result = talib.ADX(self.high, self.low, self.close, n)
    #     if array:
    #         return result
    #     return result[-1]
    #
    # def boll(self, n, dev, array=False):
    #     """
    #     Bollinger Channel.
    #     """
    #     mid = self.sma(n, array)
    #     std = self.std(n, array)
    #
    #     up = mid + std * dev
    #     down = mid - std * dev
    #
    #     return up, down
    #
    # def keltner(self, n, dev, array=False):
    #     """
    #     Keltner Channel.
    #     """
    #     mid = self.sma(n, array)
    #     atr = self.atr(n, array)
    #
    #     up = mid + atr * dev
    #     down = mid - atr * dev
    #
    #     return up, down
    #
    # def donchian(self, n, array=False):
    #     """
    #     Donchian Channel.
    #     """
    #     up = talib.MAX(self.high, n)
    #     down = talib.MIN(self.low, n)
    #
    #     if array:
    #         return up, down
    #     return up[-1], down[-1]

# JSON配置文件路径
jsonPathDict = {}
def getJsonPath(name, moduleFile):
    """
    获取JSON配置文件的路径：
    1. 优先从当前工作目录查找JSON文件
    2. 若无法找到则前往模块所在目录查找
    """
    currentFolder = os.getcwd()
    currentJsonPath = os.path.join(currentFolder, name)
    if os.path.isfile(currentJsonPath):
        jsonPathDict[name] = currentJsonPath
        return currentJsonPath

    moduleFolder = os.path.abspath(os.path.dirname(moduleFile))
    moduleJsonPath = os.path.join(moduleFolder, '.', name)
    jsonPathDict[name] = moduleJsonPath
    return moduleJsonPath

class DBEngine(metaclass=Singleton):
    """
    """
    def __init__(self):
        """"""
        # MongoDB数据库相关
        self.dbClient = None  # MongoDB客户端对象
        self.dbConnect()
    # ----------------------------------------------------------------------
    def dbConnect(self,address=None):
        """连接MongoDB数据库"""
        if not self.dbClient:
            # 读取MongoDB的设置
            try:
                # 设置MongoDB操作的超时时间为0.5秒
                if address:
                    self.dbClient = MongoClient(address, 27017, connectTimeoutMS=500)
                else:
                    self.dbClient = MongoClient('localhost', 27017, connectTimeoutMS=500)

                # 调用server_info查询服务器状态，防止服务器异常并未连接成功
                self.dbClient.server_info()
                print('MongoDB is connected.')

            except :
                print('Failed to connect to MongoDB,address={}'.format(address))

    #----------------------------------------------------------------------
    def dbInsert(self, dbName, collectionName, d):
        """向MongoDB中插入数据，d是具体数据"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.insert_one(d)
        else:
            print('Data insert failed，please connect MongoDB first.')

    # ----------------------------------------------------------------------
    def dbQuery(self, dbName, collectionName, d, sortKey='', sortDirection=ASCENDING):
        """从MongoDB中读取数据，d是查询要求，返回的是数据库查询的指针"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            if sortKey:
                cursor = collection.find(d).sort(sortKey, sortDirection)  # 对查询出来的数据进行排序
            else:
                cursor = collection.find(d)
            if cursor:
                return list(cursor)
            else:
                return []
            return cursor
        else:
            print('query Fail')
            return []

    #----------------------------------------------------------------------
    def dbUpdate(self, dbName, collectionName, d, flt, upsert=False):
        """向MongoDB中更新数据，d是具体数据，flt是过滤条件，upsert代表若无是否要插入"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.replace_one(flt, d, upsert)
        else:
            print('Data update failed，please connect MongoDB first.')