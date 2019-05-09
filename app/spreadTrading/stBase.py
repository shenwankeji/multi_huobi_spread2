# encoding: UTF-8

from __future__ import division

from math import floor
from datetime import datetime
from trader.utility import round_to_pricetick
from trader.constant import (EMPTY_INT, EMPTY_FLOAT, EMPTY_STRING)

EVENT_SPREADTRADING_TICK = 'eSpreadTradingTick.'
EVENT_SPREADTRADING_POS = 'eSpreadTradingPos.'
EVENT_SPREADTRADING_LOG = 'eSpreadTradingLog'
EVENT_SPREADTRADING_ALGO = 'eSpreadTradingAlgo.'
EVENT_SPREADTRADING_ALGOLOG = 'eSpreadTradingAlgoLog'



########################################################################
class StLeg(object):
    """"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.vt_symbol = EMPTY_STRING  # 代码
        self.ratio = EMPTY_INT  # 实际交易时的比例
        self.multiplier = EMPTY_FLOAT  # 计算价差时的乘数
        self.payup = EMPTY_INT  # 对冲时的超价tick

        self.bidPrice = EMPTY_FLOAT
        self.askPrice = EMPTY_FLOAT
        self.bidVolume = EMPTY_INT
        self.askVolume = EMPTY_INT

        self.longPos = EMPTY_INT
        self.shortPos = EMPTY_INT
        self.netPos = EMPTY_INT


########################################################################
class StSpread(object):
    """"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.name = ''  # 名称
        self.allLegs = {}  # 所有腿
        self.first_query_position = False
        self.count = 0

        self.bidPrice = 0.0
        self.askPrice = 0.0
        self.bidVolume = 0
        self.askVolume = 0
        self.price = 0.0
        self.bid_percent = 0.0
        self.ask_percent = 0.0
        self.time = ''

        self.longPos = 0
        self.shortPos = 0
        self.netPos = 0

    # ----------------------------------------------------------------------
    def initSpread(self):
        """初始化价差"""
        # 生成价差代码
        legSymbolList = []
        for leg in self.allLegs.values():
            if leg.multiplier >= 0:
                legSymbol = '+%s*%s' % (leg.multiplier, leg.vt_symbol)
            else:
                legSymbol = '%s*%s' % (leg.multiplier, leg.vt_symbol)
            legSymbolList.append(legSymbol)

        self.symbol = ''.join(legSymbolList)

    # ----------------------------------------------------------------------
    def calculatePrice(self):
        """计算价格"""
        # 不能清空，否则
        # 清空价格和委托量数据
        self.bidPrice = EMPTY_FLOAT
        self.askPrice = EMPTY_FLOAT
        self.askVolume = EMPTY_FLOAT
        self.bidVolume = EMPTY_FLOAT

        # 计算价格
        activeVtSymbol = self.name.split('+')[0]
        passiveVtSymbol = self.name.split('+')[1]

        if self.allLegs[activeVtSymbol].bidPrice == 0 or self.allLegs[passiveVtSymbol].bidPrice == 0:
            return

        # 初始化时没有查询持仓更新，直接返回
        if not self.first_query_position:
            return

        self.bidPrice = self.allLegs[activeVtSymbol].bidPrice - self.allLegs[passiveVtSymbol].askPrice
        self.askPrice = self.allLegs[activeVtSymbol].askPrice - self.allLegs[passiveVtSymbol].bidPrice
        self.price = (self.allLegs[activeVtSymbol].bidPrice + self.allLegs[passiveVtSymbol].askPrice +
                      self.allLegs[activeVtSymbol].askPrice + self.allLegs[passiveVtSymbol].bidPrice)/4
        self.bidPrice = round_to_pricetick(self.bidPrice, 0.000001)
        self.askPrice = round_to_pricetick(self.askPrice, 0.000001)
        self.price = round_to_pricetick(self.price, 0.000001)
        self.bid_percent = self.bidPrice / self.price
        self.ask_percent = self.askPrice / self.price

        self.bidVolume = min(self.allLegs[activeVtSymbol].bidVolume, self.allLegs[passiveVtSymbol].askVolume)
        self.askVolume = min(self.allLegs[activeVtSymbol].askVolume, self.allLegs[passiveVtSymbol].bidVolume)

        # 更新时间
        self.time = datetime.now().strftime('%H:%M:%S.%f')[:-3]

    # ----------------------------------------------------------------------
    def calculatePos(self):
        """计算持仓"""
        # 清空持仓数据
        self.longPos = EMPTY_INT
        self.shortPos = EMPTY_INT
        self.netPos = EMPTY_INT

        activeVtSymbol = self.name.split('+')[0]
        passiveVtSymbol = self.name.split('+')[1]
        self.longPos = int(min(self.allLegs[activeVtSymbol].longPos, self.allLegs[passiveVtSymbol].shortPos))
        self.shortPos = int(min(self.allLegs[activeVtSymbol].shortPos, self.allLegs[passiveVtSymbol].longPos))
        self.netPos = self.longPos - self.shortPos
        # print('longPos',self.longPos,'shortPos',self.shortPos,'netPos',self.netPos)
        self.count += 1
        if self.count >= 2:
            self.first_query_position = True


########################################################################
class StSpread1(StSpread):
    """"""

    # ----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        super(StSpread1, self).__init__()