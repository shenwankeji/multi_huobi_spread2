# encoding: UTF-8
"""
"""

from __future__ import print_function

import base64
import hashlib
import hmac
import json
import sys
import time
import zlib
from copy import copy
from datetime import datetime
from urllib.parse import urlencode

import pandas as pd
import requests
from requests import ConnectionError
import dateutil.parser as dp
from api.rest import Request, RestClient
from api.websocket import WebsocketClient
from trader.constant import (
    Direction,
    Offset,
    Exchange,
    PriceType,
    Product,
    Status,
)
from trader.gateway import BaseGateway
from trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    LogData,
)

REST_HOST = 'https://www.okex.com'
WEBSOCKET_HOST = 'wss://real.okex.com:10442/ws/v3'

# REST_HOST = 'https://47.75.99.233'
# WEBSOCKET_HOST = 'https://149.129.81.70'

# REST_HOST = '47.75.99.233 www.okex.com'
# WEBSOCKET_HOST = '149.129.81.70 real.okex.com'

# 委托状态类型映射
STATUS_OKEX2VT = {}
STATUS_OKEX2VT['0'] = Status.NOTTRADED
STATUS_OKEX2VT['1'] = Status.PARTTRADED
STATUS_OKEX2VT['2'] = Status.ALLTRADED
STATUS_OKEX2VT['-1'] = Status.CANCELLED

DIRECTION_VT2OKEX = {Direction.LONG: "buy", Direction.SHORT: "sell"}
DIRECTION_OKEX2VT = {v: k for k, v in DIRECTION_VT2OKEX.items()}

# 方向和开平映射
typemap = {}
typemap[(Direction.LONG, Offset.OPEN)] = '1'
typemap[(Direction.SHORT, Offset.OPEN)] = '2'
typemap[(Direction.LONG, Offset.CLOSE)] = '3'
typemap[(Direction.SHORT, Offset.CLOSE)] = '4'
typemap_reverse = {v: k for k, v in typemap.items()}
PRICETYPE_VT2OKEX = {PriceType.LIMIT: "Limit", PriceType.MARKET: "Market"}


class OkexfGateway(BaseGateway):
    """
    VN Trader Gateway for OKEX connection.
    """

    default_setting = {
        "key": "xxx",
        "secret": "xxx",
        "passphrase": "xxx",
        "session": 3,
        "server": "REAL"
    }

    def __init__(self, event_engine):
        """Constructor"""
        super(OkexfGateway, self).__init__(event_engine, "OKEXF")

        self.rest_api = OkexfRestApi(self)
        self.ws_api = OkexfWebsocketApi(self)

    def connect(self, setting: dict):
        """"""
        key = setting["key"]
        secret = setting["secret"]
        passphrase = setting["passphrase"]
        session = setting["session"]
        server = setting["server"]

        self.rest_api.connect(key, secret, passphrase, session, server)

        self.ws_api.connect(key, secret, passphrase, server)

    def subscribe(self, req: SubscribeRequest):
        """"""
        self.ws_api.subscribe(req)

    def un_subscribe(self, req: SubscribeRequest):
        self.ws_api.un_subscribe(req)

    def send_order(self, req: OrderRequest):
        """"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        """"""
        self.rest_api.cancel_order(req)

    def query_account(self):
        """"""
        pass

    def query_contract(self):
        self.rest_api.query_contract()

    def query_position(self, symbol: str):
        """"""
        self.rest_api.query_position(symbol)

    def close(self):
        """"""
        self.rest_api.stop()
        self.ws_api.stop()


class OkexfRestApi(RestClient):
    """
    OKEX REST API
    """

    def __init__(self, gateway: BaseGateway):
        """"""
        super(OkexfRestApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.passphrase = ""
        self.order_count = 1_000_000
        self.connect_time = 0
        self.query_contract()

    def sign(self, request):
        """
        Generate OKEX signature.
        """
        # Sign
        timestamp = str(server_timestamp())
        request.data = json.dumps(request.data)

        if request.params:
            path = request.path + '?' + urlencode(request.params)
        else:
            path = request.path

        msg = timestamp + request.method + path + request.data
        signature = generateSignature(msg, self.secret)

        # 添加表头
        request.headers = {
            'OK-ACCESS-KEY': self.key,
            'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }
        return request

    def connect(
            self,
            key: str,
            secret: str,
            passphrase: str,
            session: int,
            server: str,
    ):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self.connect_time = (
                int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )

        self.init(REST_HOST)

        self.start(session)

        self.gateway.write_log("REST API启动成功")

    def query_contract(self):
        """"""
        self.add_request('GET', '/api/futures/v3/instruments',
                         callback=self.on_query_contract)

    def on_query_contract(self, data, request):
        """"""
        data = pd.DataFrame(data)
        data.to_csv('OKEXF_contract.csv', index=False)
        self.writeLog(u'合约信息查询成功')

    def query_position(self, symbol: str):
        """"""
        self.add_request('GET', '/api/futures/v3/{}/position'.format(symbol),
                         callback=self.on_query_position)

    def on_query_position(self, data, request):
        """"""
        if not data['holding']:
            long_qty = 0
            short_qty = 0
        else:
            long_qty = int(data['holding'][0]["long_qty"])
            short_qty = int(data['holding'][0]["short_qty"])
        position = PositionData(
            symbol=request.path.split('/')[-2],
            exchange=Exchange.OKEX.value,
            direction=Direction.NET,
            long_qty=long_qty,
            short_qty=short_qty,
            gateway_name=self.gateway_name
        )

        self.gateway.on_position(position)

    # ----------------------------------------------------------------------
    def writeLog(self, content):
        """发出日志"""
        self.gateway.write_log(content)

    def send_order(self, req: OrderRequest):
        """"""
        self.order_count += 1
        vt_client_oid = self.gateway_name + str(self.connect_time + self.order_count)

        data = {
            "client_oid": vt_client_oid,
            "instrument_id": req.symbol,
            "type": typemap[(req.direction, req.offset)],
            "price": req.price,
            "size": int(req.volume),
            "leverage": 10,
        }

        order = req.create_order_data(vt_client_oid, self.gateway_name)

        self.add_request(
            "POST",
            "/api/futures/v3/order",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )
        return order.vt_client_oid

    def cancel_order(self, req: CancelRequest):
        """"""
        self.add_request(
            "POST",
            "/api/futures/v3/cancel_order/{}/{}".format(req.symbol, req.vt_client_oid),
            callback=self.on_cancel_order,
            on_error=self.on_cancel_order_error,
        )

    def on_send_order_failed(self, status_code: str, request: Request):
        """
        Callback when sending order failed on server.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

    def on_send_order_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ):
        """
        Callback when sending order caused exception.
        """
        order = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_send_order(self, data, request):
        """"""
        pass

    def on_cancel_order_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ):
        """
        Callback when cancelling order failed on server.
        """
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, data, request):
        """"""
        pass

    def on_failed(self, status_code: int, request: Request):
        """
        Callback to handle request failed.
        """
        msg = f"请求失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

    def on_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ):
        """
        Callback to handler request exception.
        """
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(
            self.exception_detail(exception_type, exception_value, tb, request)
        )


class OkexfWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway):
        """"""
        super(OkexfWebsocketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.passphrase = ""

        self.callbacks = {
            # "futures/ticker": self.on_tick,
            "futures/depth5": self.on_depth,
            "futures/trade": self.on_trade,
            "futures/order": self.on_order,
            "futures/position": self.on_position,
            "futures/account": self.on_account,
            # "instrument": self.on_contract,
        }

        self.ticks = {}
        self.accounts = {}
        self.orders = {}
        self.trades = set()

    # ----------------------------------------------------------------------
    def unpack_data(self, data):
        """重载"""
        return json.loads(zlib.decompress(data, -zlib.MAX_WBITS))

    def connect(self, key: str, secret: str, passphrase: str, server: str):
        """"""
        self.key = key
        self.secret = secret
        self.passphrase = passphrase
        self.init(WEBSOCKET_HOST)
        self.start()

    def subscribe(self, req: SubscribeRequest):
        """
        Subscribe to tick data upate.
        """
        subscribeReq = {
            "op": "subscribe",
            "args": [
                # "futures/trade:{}".format(req.symbol),
                "futures/depth5:{}".format(req.symbol),
                "futures/order:{}".format(req.symbol),
                "futures/position:{}".format(req.symbol),
                "futures/account:{}".format(req.symbol[:3])
            ],
        }
        self.send_packet(subscribeReq)

        tick = TickData(
            symbol=req.symbol,
            exchange=req.exchange,
            name=req.symbol,
            datetime=datetime.now(),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol] = tick

    def un_subscribe(self, req: SubscribeRequest):
        """
        unSubscribe to tick data upate.
        """
        un_subscribeReq = {
            "op": "unsubscribe",
            "args": [
                # "futures/trade:{}".format(req.symbol),
                "futures/depth5:{}".format(req.symbol),
                "futures/order:{}".format(req.symbol),
                "futures/position:{}".format(req.symbol),
                "futures/account:{}".format(req.symbol[:3])
            ],
        }
        self.send_packet(un_subscribeReq)

    def on_connected(self):
        """"""
        self.gateway.write_log("Websocket API连接成功")
        self.authenticate()

    def on_disconnected(self):
        """"""
        self.gateway.write_log("Websocket API连接断开")

    def on_packet(self, packet: dict):
        """"""
        if "event" in packet:
            req = packet["event"]
            if req == 'error':
                self.gateway.write_log("Websocket API报错：%s" % packet["message"])

            if req == 'subscribe':
                return

            if req == "login":
                success = packet["success"]
                if success:
                    callback = self.callbacks[req]
                    callback(packet)
                    self.gateway.write_log("Websocket API验证授权成功")
                    for symbol in self.ticks:
                        req = SubscribeRequest(
                            symbol=symbol,
                            exchange=Exchange.OKEX.value)
                        self.subscribe(req)

        elif "table" in packet:
            name = packet["table"]
            callback = self.callbacks[name]

            if isinstance(packet["data"], list):
                for d in packet["data"]:
                    callback(d)
            else:
                callback(packet["data"])

    def on_error(self, exception_type: type, exception_value: Exception, tb):
        """"""
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(
            exception_type, exception_value, tb))

    def authenticate(self):
        """
        Authenticate websockey connection to subscribe private topic.
        """
        timestamp = str(server_timestamp())
        msg = str(timestamp) + 'GET' + '/users/self/verify'
        signature = generateSignature(msg, self.secret)
        login_param = {"op": "login", "args": [self.key, self.passphrase, timestamp, signature.decode("utf-8")]}
        self.send_packet(login_param)

        self.callbacks['login'] = self.onLogin

    # ----------------------------------------------------------------------
    def onLogin(self, d):
        """"""
        self.gateway.write_log(d)

    def on_tick(self, d):
        """"""
        symbol = d["instrument_id"]
        tick = self.ticks.get(symbol, None)
        if not tick:
            return

        tick.last_price = d["price"]
        tick.datetime = datetime.strptime(
            d["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        self.gateway.on_tick(copy(tick))

    def on_depth(self, d):
        """"""
        symbol = d["instrument_id"]
        tick = self.ticks.get(symbol, None)
        if not tick:
            return

        for n, buf in enumerate(d["bids"][:5]):
            price, volume = buf[:2]
            tick.__setattr__("bid_price_%s" % (n + 1), price)
            tick.__setattr__("bid_volume_%s" % (n + 1), volume)

        for n, buf in enumerate(d["asks"][:5]):
            price, volume = buf[:2]
            tick.__setattr__("ask_price_%s" % (n + 1), price)
            tick.__setattr__("ask_volume_%s" % (n + 1), volume)

        tick.datetime = datetime.strptime(
            d["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        self.gateway.on_tick(copy(tick))

    def on_trade(self, d):
        """"""
        return

    def on_order(self, d):
        """"""
        if "status" not in d:
            return

        # 接收时名称也修改为vt_client_oid
        vt_client_oid = d['client_oid']
        order = self.orders.get(vt_client_oid, None)
        if not order:
            order = OrderData(
                symbol=d["instrument_id"],
                exchange=Exchange.OKEX.value,
                vt_client_oid=vt_client_oid,
                direction=typemap_reverse[d['type']][0],
                offset=typemap_reverse[d['type']][1],
                price=float(d["price"]),
                volume=int(d["size"]),
                time=d["timestamp"][11:19],
                gateway_name=self.gateway_name,
            )
            self.orders[vt_client_oid] = order

        order.traded = int(d.get("filled_qty", order.traded))
        order.status = STATUS_OKEX2VT.get(d["status"], order.status)

        self.gateway.on_order(copy(order))

    def on_position(self, d):
        """"""
        position = PositionData(
            symbol=d["instrument_id"],
            exchange=Exchange.OKEX.value,
            direction=Direction.NET,
            long_qty=int(d["long_qty"]),
            short_qty=int(d["short_qty"]),
            gateway_name=self.gateway_name
        )

        self.gateway.on_position(position)

    def on_account(self, d):
        """"""
        accountid = list(d.keys())[0]
        v = d[accountid]
        account = self.accounts.get(accountid, None)
        if not account:
            account = AccountData(
                accountid=accountid,
                equity=float(v['equity']),
                margin=float(v['margin']),
                margin_mode=v['margin_mode'],
                margin_ratio=float(v['margin_ratio']),
                realized_pnl=float(v['realized_pnl']),
                total_avail_balance=float(v['total_avail_balance']),
                unrealized_pnl=float(v['unrealized_pnl']),
                gateway_name=self.gateway_name, )
            self.accounts[accountid] = account

        self.gateway.on_account(copy(account))

    def on_contract(self, d):
        """"""
        if "ticksize" not in d:
            return

        if not d["lotSize"]:
            return

        contract = ContractData(
            symbol=d["instrument_id"],
            exchange=Exchange.OKEX.value,
            name=d["symbol"],
            product=Product.FUTURES,
            pricetick=d["ticksize"],
            size=d["lotSize"],
            gateway_name=self.gateway_name, )

        self.gateway.on_contract(contract)


# ----------------------------------------------------------------------
def generateSignature(msg, apiSecret):
    """签名V3"""
    mac = hmac.new(bytes(apiSecret, encoding='utf8'), bytes(msg, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)
    return sign


def server_timestamp():
    server_time = get_server_time()
    # print('server_time: ', server_time)
    parsed_t = dp.parse(server_time)
    timestamp = parsed_t.timestamp()
    return timestamp


def get_server_time():
    url = "http://www.okex.com/api/general/v3/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['iso']
    else:
        return ""
