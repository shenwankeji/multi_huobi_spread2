# encoding: UTF-8
"""
"""

from __future__ import print_function

import base64
import hashlib
import hmac
import json
import sys
import zlib
from copy import copy
from datetime import datetime
from threading import Lock, Thread
import pandas as pd
from requests import ConnectionError
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
from urllib import parse
from trader.utility import DBEngine
from trader.constant import *

REST_HOST = 'https://api.hbdm.com'
WEBSOCKET_HOST = 'wss://www.hbdm.com/ws'
WEBSOCKET_HOST_TRADE = 'wss://api.hbdm.com/notification'

# 委托状态类型映射
STATUS_OKEX2VT = {}
STATUS_OKEX2VT[0] = Status.NOTTRADED
STATUS_OKEX2VT[4] = Status.PARTTRADED
STATUS_OKEX2VT[6] = Status.ALLTRADED
STATUS_OKEX2VT[7] = Status.CANCELLED
STATUS_OKEX2VT[5] = Status.CANCELLED

DIRECTION_VT2OKEX = {Direction.LONG: "buy", Direction.SHORT: "sell"}
DIRECTION_OKEX2VT = {v: k for k, v in DIRECTION_VT2OKEX.items()}

# 方向和开平映射
typemap = {}
typemap[Direction.LONG] = 'buy'
typemap[Direction.SHORT] = 'sell'
typemap[Offset.OPEN] = 'open'
typemap[Offset.CLOSE] = 'close'
typemap_reverse = {v: k for k, v in typemap.items()}
PRICETYPE_VT2OKEX = {PriceType.LIMIT: "Limit", PriceType.MARKET: "Market"}

DB_NAME = HuobiDB.DB_NAME.value
DB_ORDERID = HuobiDB.DB_ORDERID.value
DB_ORDER_STRATEGY = HuobiDB.DB_ORDER_STRATEGY.value

class HuobifGateway(BaseGateway):
    # xxx账号
    default_setting = {
        "key": "xxxxx",
        "secret": "xxxxx"
    }

    def __init__(self, event_engine):
        """Constructor"""
        super(HuobifGateway, self).__init__(event_engine,"HUOBIF")

        self.rest_api = HuobifRestApi(self)
        self.ws_api = HuobifWebsocketApi(self)
        self.ws_api_trade = HuobiTradeWebsocketApi(self)

    def connect(self, setting: dict):
        """"""
        key = setting["key"]
        secret = setting["secret"]

        self.rest_api.connect(key, secret)
        self.ws_api.connect(key, secret)
        self.ws_api_trade.connect(key, secret)

    def subscribe(self, req: SubscribeRequest):
        """"""
        self.ws_api.subscribe(req)

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

    def query_position(self, strategy_name: str,symbol: str):
        """"""
        self.rest_api.query_position(strategy_name,symbol)

    def close(self):
        """"""
        self.rest_api.stop()
        self.ws_api.stop()
        self.ws_api_trade.stop()


class HuobifRestApi(RestClient):
    """
    OKEX REST API
    """

    def __init__(self, gateway: BaseGateway):
        """"""
        super(HuobifRestApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.passphrase = ""
        self.order_count = 0
        self.connect_time = 0
        self.orders = {}
        self.query_contract()
        self.dbEngine = DBEngine()

    def sign(self, request):
        """
        Generate OKEX signature.
        """
        # Sign
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        params_to_sign = {'AccessKeyId': self.key,
                          'SignatureMethod': 'HmacSHA256',
                          'SignatureVersion': '2',
                          'Timestamp': timestamp}

        params_to_sign['Signature'] = createSign(params_to_sign, request.method,'api.hbdm.com', request.path, self.secret)
        request.data = json.dumps(request.data)

        request.path = request.path + '?' + parse.urlencode(params_to_sign)

        # 添加表头
        request.headers = {
            "Accept": "application/json",
            'Content-Type': 'application/json',
            'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'
        }
        return request

    def connect(
            self,
            key: str,
            secret: str
    ):
        """
        Initialize connection to REST server.
        """
        self.key = key
        self.secret = secret

        self.init(REST_HOST)

        self.start()
        try:
            d = {'apikey':self.key}
            list = self.dbEngine.dbQuery(DB_NAME,DB_ORDERID,d)
            if list[0]:
                self.order_count = list[0]['order_count']
        except:
            self.writeLog('查询MongoDB失败{}'.format(d))
        self.gateway.write_log("REST API启动成功")


    def query_contract(self):
        """"""
        self.add_request('GET', '/api/v1/contract_contract_info',
                         callback=self.on_query_contract)

    def on_query_contract(self, data, request):
        """"""
        data = pd.DataFrame(data)
        data.to_csv('HUOBIF_contract.csv', index=False)
        self.writeLog(u'火币合约信息查询成功')

    def query_position(self,strategy_name,symbol):
        # 初始化默认持仓为0
        long_qty = 0
        short_qty = 0
        flt = {
            'strategy_name': strategy_name,
            'vt_symbol':symbol
        }
        list = self.dbEngine.dbQuery(DB_NAME, DB_ORDER_STRATEGY, flt)
        for item in list:
            if item['status'] not in ('已撤单', '挂单中'):
                if item['direction'] == 'buy' and item['offset'] == 'open':  # 开多
                    long_qty += int(item['trade_volume'])
                if item['direction'] == 'sell' and item['offset'] == 'close':  # 平多
                    long_qty -= int(item['trade_volume'])
                if item['direction'] == 'sell' and item['offset'] == 'open':  # 开空
                    short_qty += int(item['trade_volume'])
                if item['direction'] == 'buy' and item['offset'] == 'close':  # 平空
                    short_qty -= int(item['trade_volume'])
        position = PositionData(
            symbol=symbol[:6],
            strategy_name=strategy_name,
            exchange=Exchange.HUOBI.value,
            direction=Direction.NET,
            long_qty=long_qty,
            short_qty=short_qty,
            gateway_name=self.gateway_name)
        self.gateway.on_position(position)

    # ----------------------------------------------------------------------
    def writeLog(self, content):
        """发出日志"""
        self.gateway.write_log(content)

    def send_order(self, req: OrderRequest):
        """"""
        self.order_count += 1
        contract = req.contract
        data = {
            "symbol": contract.underlying_index,
            "contract_type": contract.alias,
            # "contract_code": req.symbol,
            "client_order_id": self.order_count,
            "price": req.price,
            "volume": str(int(req.volume)),
            "direction": typemap[req.direction],
            "offset": typemap[req.offset],
            "lever_rate": 20,
            "order_price_type": 'limit'
        }

        order = req.create_order_data(self.order_count,req.strategy_name, self.gateway_name)
        self.add_request(
            "POST",
            "/api/v1/contract_order",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_failed=self.on_send_order_failed,
            on_error=self.on_send_order_error,
        )
        condition = {'apikey':self.key}
        new = {'apikey':self.key,'order_count':self.order_count}
        try:
            self.dbEngine.dbUpdate(DB_NAME,DB_ORDERID,new,condition,upsert=True)
        except:
            self.writeLog('更新MongoDB失败{}'.format(new))
        return str(self.order_count)

    def cancel_order(self, req: CancelRequest):
        """"""
        data = {'client_order_id':str(req.vt_client_oid),'symbol':req.symbol[:3]}
        self.add_request(
            "POST",
            "/api/v1/contract_cancel",
            data=data,
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

    def on_send_order(self, result, request):
        """"""
        if result['status'] == 'ok':
            self.writeLog('下单成功{}'.format(result))
            order = request.extra
            data = eval(request.data)
            time = datetime.fromtimestamp(result['ts'] / 1000)
            d = {
                'apikey': self.key,
                'strategy_name': order.strategy_name,
                'vt_symbol': order.vt_symbol,
                'order_count': data['client_order_id'],
                'order_id': result['data']['order_id'],
                'direction': data['direction'],
                'offset': data['offset'],
                'price': data['price'],
                'total_volume': data['volume'],
                'trade_volume': 0,
                'status': u'挂单中',
                'time':time,
                'trade_avg_price':0
            }
            try:
                self.dbEngine.dbInsert(DB_NAME, DB_ORDER_STRATEGY, d)
            except:
                self.writeLog('插入成交数据到MongoDB失败')
        else:
            self.writeLog('下单失败{}'.format(result))
            order = request.extra
            order.traded = 0
            order.status = Status.REJECTED
            order.vt_client_oid = str(order.vt_client_oid)
            self.gateway.on_order(copy(order))
            data = eval(request.data)
            time = datetime.fromtimestamp(result['ts'] / 1000)
            d = {
                'apikey': self.key,
                'strategy_name': order.strategy_name,
                'vt_symbol': order.vt_symbol,
                'order_count': data['client_order_id'],
                'order_id': '',
                'direction': data['direction'],
                'offset': data['offset'],
                'price': data['price'],
                'total_volume': data['volume'],
                'trade_volume': 0,
                'status': u'挂单中',
                'time':time,
                'trade_avg_price':0
            }
            try:
                self.dbEngine.dbInsert(DB_NAME, DB_ORDER_STRATEGY, d)
            except:
                self.writeLog('插入成交数据到MongoDB失败')

    def on_cancel_order_error(
            self, exception_type: type, exception_value: Exception, tb, request: Request
    ):
        """
        Callback when cancelling order failed on server.
        """
        # Record exception if not ConnectionError
        if not issubclass(exception_type, ConnectionError):
            self.on_error(exception_type, exception_value, tb, request)

    def on_cancel_order(self, result, request):
        """"""
        if result['status'] == 'ok':
            d = result['data']
            if d['successes']:
                self.writeLog('撤单成功{}'.format(result))
                data = eval(request.data)
                flt ={
                    'apikey': self.key,
                    'order_count': int(data['client_order_id'])
                }
                try:
                    list = self.dbEngine.dbQuery(DB_NAME,DB_ORDER_STRATEGY,flt)
                    if list[0]:
                        d = list[0]
                        d['status'] = '已撤单'
                    self.dbEngine.dbUpdate(DB_NAME, DB_ORDER_STRATEGY, d,flt)
                except:
                    self.writeLog('更新撤单数据到MongoDB失败')
            else:
                self.writeLog('撤单失败{}'.format(result))

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


class HuobifWebsocketApi(WebsocketClient):
    """火币合约行情订阅websocket接口"""

    def __init__(self, gateway):
        """"""
        super(HuobifWebsocketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""

        self.ticks = {}

    # ----------------------------------------------------------------------
    def unpack_data(self, data):
        """重载"""
        return json.loads(zlib.decompress(data, 47).decode('utf-8'))

    def connect(self, key: str, secret: str):
        """"""
        self.key = key
        self.secret = secret
        self.init(WEBSOCKET_HOST)
        self.start()

    def start(self):

        self._active = True
        self._worker_thread = Thread(target=self._run)
        self._worker_thread.start()

    def subscribe(self, req: SubscribeRequest):
        """
        Subscribe to tick data upate.
        """
        subscribeReq = {
            "sub": "market.{}.depth.step0".format(req.symbol),
            "id": "id1"
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

    def on_connected(self):
        """"""
        self.gateway.write_log("火币合约行情Websocket API连接成功")
        for symbol in self.ticks:
            req = SubscribeRequest(
                symbol=symbol,
                exchange=Exchange.HUOBI.value
            )
            self.subscribe(req)

    def on_disconnected(self):
        """"""
        self.gateway.write_log("Websocket API连接断开")

    def on_packet(self, packet: dict):
        """"""
        if 'ping' in packet:
            self.pong(packet)
        if 'ch' in packet:
            if 'depth' in packet['ch']:
                try:
                    self.on_depth(packet)
                except:
                    pass

    #----------------------------------------------------------------------
    def pong(self, data):
        """响应心跳"""
        req = {'pong': data['ping']}
        self.send_packet(req)

    def on_error(self, exception_type: type, exception_value: Exception, tb):
        """"""
        msg = f"触发异常，状态码：{exception_type}，信息：{exception_value}"
        self.gateway.write_log(msg)

        sys.stderr.write(self.exception_detail(
            exception_type, exception_value, tb))

    def on_depth(self, d):
        """"""
        symbol = d['ch'].split('.')[1]
        tick = self.ticks.get(symbol, None)
        if not tick:
            return

        bids = d['tick']['bids']
        for n in range(5):
            l = bids[n]
            tick.__setattr__('bid_price_' + str(n + 1), float(l[0]))
            tick.__setattr__('bid_volume_' + str(n + 1), float(l[1]))

        asks = d['tick']['asks']
        for n in range(5):
            l = asks[n]
            tick.__setattr__('ask_price_' + str(n + 1), float(l[0]))
            tick.__setattr__('ask_volume_' + str(n + 1), float(l[1]))

        tick.datetime = datetime.fromtimestamp(d['ts'] / 1000)
        self.gateway.on_tick(copy(tick))

#-----------------------------------------
#交易相关的websocket接口,订单账户授权等
class HuobiTradeWebsocketApi(WebsocketClient):
    """"""

    def __init__(self, gateway):
        """"""
        super(HuobiTradeWebsocketApi, self).__init__()

        self.gateway = gateway
        self.gateway_name = gateway.gateway_name

        self.key = ""
        self.secret = ""
        self.orders = {}
        self.dbEngine = DBEngine()

    def connect(self, key: str, secret: str):
        """"""
        self.key = key
        self.secret = secret
        self.init(WEBSOCKET_HOST_TRADE)
        self.start()

    def start(self):
        self._active = True
        self._worker_thread = Thread(target=self._run)
        self._worker_thread.start()

    def on_connected(self):
        """"""
        self.gateway.write_log("火币合约交易Websocket API连接成功")
        self.authenticate()

    def authenticate(self):
        """
        Authenticate websockey connection to subscribe private topic.
        """
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        # 发送的authData数据
        authdata = [
            self.secret.encode('utf-8'),
            {
                "op": "auth",
                "type": "api",
                "AccessKeyId": self.key,
                "SignatureMethod": "HmacSHA256",
                "SignatureVersion": "2",
                "Timestamp": timestamp
            }
        ]
        # 获取需要签名的信息
        authenticaton_data = authdata[1]

        # 获取 secretkey
        _accessKeySecret = authdata[0]
        # 计算签名Signature
        authenticaton_data['Signature'] = generateSignature(authenticaton_data, _accessKeySecret)

        self.send_packet(authenticaton_data)

    def on_packet(self, packet: dict):
        """"""
        # print('packet: ',packet)
        if 'ping' in packet['op']:
            self.pong(packet)
        if 'auth' in packet['op']:
            if packet['err-code'] == 0:
                self.gateway.write_log("火币合约交易Websocket 鉴权成功")
                self.subOrder()
            else:
                self.gateway.write_log("火币合约交易Websocket 鉴权失败，{}".format(packet))
        if 'sub' in packet['op']:
            if packet['err-code'] == 0:
                self.gateway.write_log("订阅合约订单成交数据成功，{}".format(packet))
            else:
                self.gateway.write_log("订阅合约订单成交数据失败，{}".format(packet))
        if 'notify' in packet['op']:
            # print('packet:{}'.format(packet))
            d = packet
            # 接收时名称也修改为vt_client_oid
            vt_client_oid = d['client_order_id']
            flt = {
                'apikey': self.key,
                'order_count': int(vt_client_oid)
            }
            list = self.dbEngine.dbQuery(DB_NAME, DB_ORDER_STRATEGY, flt)
            if list and list[0]:
                record = list[0]
                order = self.orders.get(vt_client_oid, None)
                if not order:
                    dic = {"quarter": "_CQ", "next_week": "_NW", "this_week": "_CW"}
                    symbol = d['symbol'] + dic[d['contract_type']]
                    order = OrderData(
                        symbol=symbol,
                        strategy_name=record['strategy_name'],
                        exchange=Exchange.HUOBI.value,
                        vt_client_oid=str(vt_client_oid),
                        direction=typemap_reverse[d['direction']],
                        offset=typemap_reverse[d['offset']],
                        price=float(d["price"]),
                        volume=int(d["volume"]),
                        time=d["ts"],
                        gateway_name=self.gateway_name
                    )
                    self.orders[vt_client_oid] = order

                order.traded = int(d.get("trade_volume", order.traded))
                order.status = STATUS_OKEX2VT.get(d["status"], order.status)

                status_dict = {3:'挂单中',4:'部分成交',5:'部分成交已撤单',6:'全部成交',7:'已撤单'}
                record['status'] = status_dict[d["status"]]
                record['trade_avg_price'] = d['trade_avg_price']
                record['trade_volume'] = order.traded
                self.dbEngine.dbUpdate(DB_NAME, DB_ORDER_STRATEGY, record, flt)

                # 初始化默认持仓为0
                long_qty = 0
                short_qty = 0
                flt = {
                    'apikey': self.key,
                    'strategy_name': order.strategy_name,
                    'vt_symbol': order.vt_symbol
                }
                list = self.dbEngine.dbQuery(DB_NAME, DB_ORDER_STRATEGY, flt)
                for item in list:
                    if item['status'] not in ('已撤单', '挂单中'):
                        # print('item:{}'.format(item))
                        if item['direction'] == 'buy' and item['offset'] == 'open': #开多
                            long_qty += int(item['trade_volume'])
                        if item['direction'] == 'sell' and item['offset'] == 'close': #平多
                            long_qty -= int(item['trade_volume'])
                        if item['direction'] == 'sell' and item['offset'] == 'open':  #开空
                            short_qty += int(item['trade_volume'])
                        if item['direction'] == 'buy' and item['offset'] == 'close':  # 平空
                            short_qty -= int(item['trade_volume'])
                position = PositionData(
                    symbol=order.symbol,
                    strategy_name=order.strategy_name,
                    exchange=Exchange.HUOBI.value,
                    direction=Direction.NET,
                    long_qty=long_qty,
                    short_qty=short_qty,
                    gateway_name=self.gateway_name)
                # print('position:{}'.format(position.__dict__))
                self.gateway.on_position(position)
                self.gateway.on_order(copy(order))

            """
            {'op': 'notify', 'topic': 'orders.eos', 'ts': 1553833818283, 'symbol': 'EOS', 'contract_type': 'quarter', 'contract_code': 'EOS190628', 'volume': 1, 'price': 4.344, 'order_price_type': 'limit', 'direction': 'sell', 'offset': 'open', 'status': 3, 'lever_rate': 20, 'order_id': 2, 'client_order_id': None, 'order_source': 'web', 'order_type': 1, 'created_at': 1553833818069, 'trade_volume': 0, 'trade_turnover': 0, 'fee': 0, 'trade_avg_price': 0.0, 'margin_frozen': 0.1151012891344383, 'profit': 0, 'trade': []}
            {'op': 'notify', 'topic': 'orders.eos', 'ts': 1553833850504, 'symbol': 'EOS', 'contract_type': 'quarter', 'contract_code': 'EOS190628', 'volume': 1, 'price': 4.344, 'order_price_type': 'limit', 'direction': 'sell', 'offset': 'open', 'status': 6, 'lever_rate': 20, 'order_id': 2, 'client_order_id': None, 'order_source': 'web', 'order_type': 1, 'created_at': 1553833818069, 'trade_volume': 1, 'trade_turnover': 10.0, 'fee': -0.000460405156537753, 'trade_avg_price': 4.344, 'margin_frozen': 0.0, 'profit': 0, 'trade': [{'trade_id': 2761194183, 'trade_volume': 1, 'trade_price': 4.344, 'trade_fee': -0.000460405156537753, 'trade_turnover': 10.0, 'created_at': 1553833850394}]}
            """

    #----------------------------------------------------------------------
    def subOrder(self):
        """订阅订单成交"""
        sub = {'op':'sub','topic':'orders.*'}
        self.send_packet(sub)

    #----------------------------------------------------------------------
    def pong(self, data):
        """响应心跳"""
        req = {'pong': data['ts']}
        self.send_packet(req)

    # ----------------------------------------------------------------------
    def unpack_data(self, data):
        """重载"""
        return json.loads(zlib.decompress(data, 47).decode('utf-8'))

# ----------------------------------------------------------------------
def generateSignature(param=None, _accessKeySecret=None):
    # 签名参数:
    params = {}
    params['SignatureMethod'] = param.get('SignatureMethod') if type(param.get('SignatureMethod')) == type(
        'a') else '' if param.get('SignatureMethod') else ''
    params['SignatureVersion'] = param.get('SignatureVersion') if type(param.get('SignatureVersion')) == type(
        'a') else '' if param.get('SignatureVersion') else ''
    params['AccessKeyId'] = param.get('AccessKeyId') if type(param.get('AccessKeyId')) == type(
        'a') else '' if param.get('AccessKeyId') else ''
    params['Timestamp'] = param.get('Timestamp') if type(param.get('Timestamp')) == type('a') else '' if param.get(
        'Timestamp') else ''
    # 对参数进行排序:
    keys = sorted(params.keys())
    # 加入&
    qs = '&'.join(['%s=%s' % (key, _encode(params[key])) for key in keys])
    # 请求方法，域名，路径，参数 后加入`\n`
    _host = 'api.hbdm.com'
    path = '/notification'
    payload = '%s\n%s\n%s\n%s' % ('GET', _host, path, qs)
    dig = hmac.new(_accessKeySecret, msg=payload.encode('utf-8'), digestmod=hashlib.sha256).digest()
    # 进行base64编码
    return base64.b64encode(dig).decode()

# ----------------------------------------------------------------------
#进行编码
def _encode(s):
    # return urllib.pathname2url(s)
    return parse.quote(s, safe='')

#对火币http请求进行签名
def createSign(pParams, method, host_url, request_path, secret_key):
    sorted_params = sorted(pParams.items(), key=lambda d: d[0], reverse=False)
    encode_params = parse.urlencode(sorted_params)
    payload = [method, host_url, request_path, encode_params]
    payload = '\n'.join(payload)
    payload = payload.encode(encoding='UTF8')
    secret_key = secret_key.encode(encoding='UTF8')
    digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
    signature = base64.b64encode(digest)
    signature = signature.decode()
    return signature

