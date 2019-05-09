from collections import defaultdict
from trader.constant import (Direction, Offset, Status, StOrderType, ORDER_ST2VT, ORDER_HEDGE)
from datetime import datetime


class SniperAlgo():
    """狙击算法（市价委托）"""
    FINISHED_STATUS = [Status.ALLTRADED, Status.CANCELLED, Status.REJECTED]

    # ----------------------------------------------------------------------
    def __init__(self, stEngine, spread):
        """Constructor"""
        self.stEngine = stEngine  # 算法引擎
        self.spreadName = spread.name  # 价差名称
        self.spread = spread  # 价差对象
        self.algoName = u'Sniper'
        self.quoteInterval = 16  # 主动腿报价撤单再发前等待的时间
        self.active_quote_count = 0  # 主动腿报价计数
        self.passive_quote_count = 0  # 被动腿报价计数
        self.active = False  # 工作状态

        self.activeVtSymbol = spread.name.split('+')[0]  # 主动腿代码
        self.passiveVtSymbol = spread.name.split('+')[1]  # 被动腿代码列表

        self.legOrderDict = {self.activeVtSymbol: [], self.passiveVtSymbol: []}  # vtSymbol: list of vt_client_oid
        self.orderTradedDict = defaultdict(int)  # vt_client_oid: tradedVolume

    # ----------------------------------------------------------------------
    def start(self):
        """启动"""
        # 如果已经运行则直接返回状态
        if self.active:
            return self.active

        # 检查价格安全性
        if not self.checkPrice():
            return False

        # 启动算法
        self.quoteCount = 0
        self.active = True
        self.stEngine.write_log('{}算法启动'.format(self.spread.name))

        return self.active

    # ----------------------------------------------------------------------
    def checkPrice(self):
        """检查价格"""
        if self.spread.buy_percent >= self.spread.cover_percent:
            self.stEngine.write_log(u'启动失败，允许双向交易时buy_percent必须小于cover_percent')
            return False

        if self.spread.short_percent <= self.spread.sell_percent:
            self.stEngine.write_log(u'启动失败，允许双向交易时short_percent必须大于sell_percent')
            return False

        return True

    # ----------------------------------------------------------------------
    def updateSpreadTick(self):
        """价差行情更新"""
        spread = self.spread
        # 若算法没有启动则直接返回
        if not self.active:
            return

        # 若当前已有委托则直接返回
        if self.legOrderDict[self.activeVtSymbol] or self.legOrderDict[self.passiveVtSymbol]:
            return

        # 如果主动腿持仓超限，返回
        if spread.allLegs[self.activeVtSymbol].longPos > (spread.maxPosSize + spread.maxOrderSize) or \
                spread.allLegs[self.activeVtSymbol].shortPos > (spread.maxPosSize + spread.maxOrderSize):
            # 卖出
            if spread.netPos > 0 and spread.bid_percent >= spread.sell_percent:
                self.quoteActiveLeg(StOrderType.SELL)

            # 平空
            elif spread.netPos < 0 and spread.ask_percent <= spread.cover_percent:
                self.quoteActiveLeg(StOrderType.COVER)
            return
        # 买入
        if 0 <= spread.netPos < spread.maxPosSize and spread.ask_percent <= spread.buy_percent:
            self.quoteActiveLeg(StOrderType.BUY)

        # 卖出
        elif spread.netPos > 0 and spread.bid_percent >= spread.sell_percent:
            self.quoteActiveLeg(StOrderType.SELL)

        # 做空
        elif 0 >= spread.netPos > -spread.maxPosSize and spread.bid_percent >= spread.short_percent:
            self.quoteActiveLeg(StOrderType.SHORT)

        # 平空
        elif spread.netPos < 0 and spread.ask_percent <= spread.cover_percent:
            self.quoteActiveLeg(StOrderType.COVER)

    # ----------------------------------------------------------------------
    def quoteActiveLeg(self, order_type):
        """发出主动腿"""
        spread = self.spread

        # 计算价格和委托量
        if order_type == StOrderType.BUY or order_type == StOrderType.COVER:
            price = spread.allLegs[self.activeVtSymbol].askPrice
            volume = min(spread.askVolume, spread.maxPosSize, spread.maxOrderSize)
        else:
            price = spread.allLegs[self.activeVtSymbol].bidPrice
            volume = min(spread.bidVolume, spread.maxPosSize, spread.maxOrderSize)
        if order_type == StOrderType.SELL:
            hold_volume = spread.allLegs[self.activeVtSymbol].longPos
            volume = min(volume, hold_volume)
        elif order_type == StOrderType.COVER:
            hold_volume = spread.allLegs[self.activeVtSymbol].shortPos
            volume = min(volume, hold_volume)
        if volume <= 0:
            return
        (direction, offset) = ORDER_ST2VT[order_type]
        payup = spread.allLegs[self.activeVtSymbol].payup
        vt_client_oid = self.stEngine.sendOrder(self.activeVtSymbol, direction, offset, price, volume, payup,self.spread.name)
        self.stEngine.write_log('{}发出新的主动腿{}狙击单，方向{},{}，数量{}'.format(self.spread.name,self.activeVtSymbol, direction, offset, volume))

        # 保存到字典中,vt_client_oid为set类型，避免重复添加
        self.legOrderDict[self.activeVtSymbol].append(vt_client_oid)

        self.active_quote_count = 0  # 重置主动腿报价撤单等待计数

    # ----------------------------------------------------------------------
    def hedgePassiveLeg(self, order, volume):
        """对冲被动腿"""
        offset = order.offset
        payup = self.spread.allLegs[self.passiveVtSymbol].payup
        direction = ORDER_HEDGE[order.direction]
        # 计算委托价
        if direction == Direction.LONG:
            price = self.spread.allLegs[self.passiveVtSymbol].askPrice
        else:
            price = self.spread.allLegs[self.passiveVtSymbol].bidPrice
        if volume <= 0:
            return

        vt_client_oid = self.stEngine.sendOrder(self.passiveVtSymbol, direction, offset, price, volume, payup,self.spread.name)
        self.stEngine.write_log('{}发出新的被动腿{}对冲单，方向{},{}，数量{}'.format(self.spread.name,self.passiveVtSymbol, direction, offset, volume))

        # 保存到字典中,vt_client_oid为set类型，避免重复添加
        self.legOrderDict[self.passiveVtSymbol].append(vt_client_oid)

        self.passive_quote_count = 0  # 重置被动腿报价撤单等待计数

    # ----------------------------------------------------------------------
    def rehedge_passive_leg(self, order):
        """再对冲被动腿"""
        offset = order.offset
        payup = self.spread.allLegs[self.passiveVtSymbol].payup
        direction = order.direction
        # 计算委托价
        if direction == Direction.LONG:
            price = self.spread.allLegs[self.passiveVtSymbol].askPrice
        else:
            price = self.spread.allLegs[self.passiveVtSymbol].bidPrice
        volume = order.volume - order.traded
        if volume <= 0:
            return

        vt_client_oid = self.stEngine.sendOrder(self.passiveVtSymbol, direction, offset, price, volume, payup,self.spread.name)
        self.stEngine.write_log('{}重新发出新的被动腿{}对冲单，方向{},{}，数量{}'.format(self.spread.name,self.passiveVtSymbol, direction, offset, volume))

        # 保存到字典中,vt_client_oid为set类型，避免重复添加
        self.legOrderDict[self.passiveVtSymbol].append(vt_client_oid)

        self.passive_quote_count = 0  # 重置被动腿报价撤单等待计数

    # ----------------------------------------------------------------------
    def updateOrder(self, order):
        """委托更新"""
        if not self.active:
            return

        vt_symbol = order.vt_symbol
        if vt_symbol == self.activeVtSymbol:
            self.update_active_order(order)
        elif vt_symbol == self.passiveVtSymbol:
            self.update_passive_order(order)

    def update_active_order(self, order):
        vt_symbol = order.vt_symbol
        vt_client_oid = order.vt_client_oid
        new_traded_volume = order.traded
        last_traded_volume = self.orderTradedDict[vt_client_oid]

        # 检查是否有新的成交
        if new_traded_volume > last_traded_volume:
            self.orderTradedDict[vt_client_oid] = new_traded_volume  # 缓存委托已经成交数量
            volume = new_traded_volume - last_traded_volume  # 计算本次成交数量
            self.stEngine.write_log('主动腿{}成交，方向{},{}，数量{}'.format(vt_symbol, order.direction, order.offset, volume))
            # 发出被动腿对冲委托
            self.hedgePassiveLeg(order, volume)

        # 处理完成委托
        if order.status in self.FINISHED_STATUS:
            # 从委托列表中移除该委托
            self.legOrderDict[vt_symbol].remove(vt_client_oid)
            self.stEngine.write_log(u'主动腿{}委托已结束!,委托状态{}，委托订单号{}'.format(vt_symbol, order.status, vt_client_oid))

    def update_passive_order(self, order):
        vt_symbol = order.vt_symbol
        vt_client_oid = order.vt_client_oid
        new_traded_volume = order.traded
        last_traded_volume = self.orderTradedDict[vt_client_oid]

        # 检查是否有新的成交
        if new_traded_volume > last_traded_volume:
            self.orderTradedDict[vt_client_oid] = new_traded_volume  # 缓存委托已经成交数量
            volume = new_traded_volume - last_traded_volume  # 计算本次成交数量
            self.stEngine.write_log('被动腿{}成交，方向{},{}，数量{}'.format(vt_symbol, order.direction, order.offset, volume))

        # 处理完成委托
        if order.status in self.FINISHED_STATUS:
            # 从委托列表中移除该委托
            self.legOrderDict[vt_symbol].remove(vt_client_oid)
            self.stEngine.write_log(u'被动腿{}委托已结束!,委托状态{}，委托订单号{}'.format(vt_symbol, order.status, vt_client_oid))

            # 处理完成委托
            if order.status in (Status.CANCELLED, Status.REJECTED):
                self.rehedge_passive_leg(order)

    # ----------------------------------------------------------------------
    def updateTimer(self):
        """计时更新"""
        if not self.active:
            return

        self.active_quote_count += 1
        self.passive_quote_count += 1

        # 计时到达报价间隔后，则对尚未成交的主动腿委托全部撤单
        # 收到撤单回报后清空委托列表，等待下次价差更新再发单
        if self.active_quote_count > self.quoteInterval and self.legOrderDict[self.activeVtSymbol]:
            vt_client_oid = list(self.legOrderDict[self.activeVtSymbol])[0]
            self.stEngine.cancel_order(self.activeVtSymbol, vt_client_oid)
            self.stEngine.write_log(u'撤单主动腿{}委托{}'.format(self.activeVtSymbol, vt_client_oid))
            self.active_quote_count = 0

        # 计时到达报价间隔后，则对尚未成交的被动腿委托全部撤单
        # 收到撤单回报后清空委托列表，等待下次价差更新再发单
        if self.passive_quote_count > self.quoteInterval and self.legOrderDict[self.passiveVtSymbol]:
            vt_client_oid = list(self.legOrderDict[self.passiveVtSymbol])[0]
            self.stEngine.cancel_order(self.passiveVtSymbol, vt_client_oid)
            self.stEngine.write_log(u'撤单被动腿{}委托{}'.format(self.passiveVtSymbol, vt_client_oid))
            self.passive_quote_count = 0

    # ----------------------------------------------------------------------
    def active_close_position(self):
        """主动腿平仓，由change_position_time触发processTickEvent(TICK行情推送)"""
        """主动腿平仓，或者由processTimerHourEvent触发"""
        """价差行情更新"""
        spread = self.spread
        # 若算法没有启动则直接返回
        if not self.active:
            return

        # 若当前已有委托则直接返回
        if self.legOrderDict[self.activeVtSymbol] or self.legOrderDict[self.passiveVtSymbol]:
            return

        # 卖出平仓
        if spread.netPos > 0:
            self.stEngine.write_log(u'卖出平仓')
            self.quoteActiveLeg(StOrderType.SELL)

        # 买入平空
        elif spread.netPos < 0:
            self.stEngine.write_log(u'买入平仓')
            self.quoteActiveLeg(StOrderType.COVER)

        elif spread.netPos == 0 and self.stEngine.change_position_time:
            self.stEngine.write_log(u'{}换仓完成！'.format(spread.name))
            self.stEngine.pop_spread_name(spread.name)

    # ----------------------------------------------------------------------
    def query_position(self):
        """查询持仓"""
        self.stEngine.query_position(self.spread.name,self.activeVtSymbol)
        self.stEngine.query_position(self.spread.name,self.passiveVtSymbol)


class StAlgo1(SniperAlgo):

    # ----------------------------------------------------------------------
    def __init__(self, stEngine, spread):
        """Constructor"""
        super(StAlgo1, self).__init__(stEngine, spread)