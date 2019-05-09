"""
General constant string used in VN Trader.
"""

from enum import Enum


class StOrderType(Enum):
    BUY = "买开"
    SELL = "卖平"
    SHORT = "卖开"
    COVER = "买平"


class Direction(Enum):
    """
    Direction of order/trade/position.
    """
    LONG = "多"
    SHORT = "空"
    NET = "净"


class Offset(Enum):
    """
    Offset of order/trade.
    """
    NONE = ""
    OPEN = "开"
    CLOSE = "平"
    CLOSETODAY = "平今"
    CLOSEYESTERDAY = "平昨"


ORDER_ST2VT = {
    StOrderType.BUY: (Direction.LONG, Offset.OPEN),
    StOrderType.SELL: (Direction.SHORT, Offset.CLOSE),
    StOrderType.SHORT: (Direction.SHORT, Offset.OPEN),
    StOrderType.COVER: (Direction.LONG, Offset.CLOSE),
}

ORDER_HEDGE = {
    Direction.LONG: Direction.SHORT,
    Direction.SHORT: Direction.LONG,
}


class Status(Enum):
    """
    Order status.
    """
    SUBMITTING = "提交中"
    NOTTRADED = "未成交"
    PARTTRADED = "部分成交"
    ALLTRADED = "全部成交"
    CANCELLED = "已撤销"
    REJECTED = "拒单"


class Product(Enum):
    """
    Product class.
    """
    EQUITY = "股票"
    FUTURES = "期货"
    OPTION = "期权"
    INDEX = "指数"
    FOREX = "外汇"
    SPOT = "现货"
    ETF = "ETF"
    BOND = "债券"
    WARRANT = "权证"


class PriceType(Enum):
    """
    Order price type.
    """
    LIMIT = "限价"
    MARKET = "市价"
    FAK = "FAK"
    FOK = "FOK"


class OptionType(Enum):
    """
    Option type.
    """
    CALL = "看涨期权"
    PUT = "看跌期权"


class Exchange(Enum):
    """
    Exchange.
    """
    # Chinese
    CFFEX = "CFFEX"
    SHFE = "SHFE"
    CZCE = "CZCE"
    DCE = "DCE"
    INE = "INE"
    SSE = "SSE"
    SZSE = "SZSE"
    SGE = "SGE"

    # Global
    SMART = "SMART"
    NYMEX = "NYMEX"
    GLOBEX = "GLOBEX"
    IDEALPRO = "IDEALPRO"
    CME = "CME"
    ICE = "ICE"
    SEHK = "SEHK"
    HKFE = "HKFE"

    # CryptoCurrency
    BITMEX = "BITMEX"
    OKEX = 'OKEX'
    HUOBI = 'HUOBI'


class Currency(Enum):
    """
    Currency.
    """
    USD = "USD"
    HKD = "HKD"
    CNY = "CNY"


class Interval(Enum):
    """
    Interval of bar data.
    """
    TICK = 'tick'
    MINUTE = "1m"
    HOUR = "1h"
    DAILY = "d"
    WEEKLY = "w"

class HuobiDB(Enum):
    """
    存储火币数据库的表名
    """
    DB_NAME = 'vn_huobi_status'
    DB_ORDERID = 'apikey_order_count'
    DB_ORDER_STRATEGY = 'order_strategy'
    DB_STRATEGY_POSITION = 'strategy_position'

# 默认空值
EMPTY_STRING = ''
EMPTY_INT = 0
EMPTY_FLOAT = 0.0
