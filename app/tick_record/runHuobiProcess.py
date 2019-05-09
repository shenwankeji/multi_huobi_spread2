import os
from dbUtil.vtObject import *
import json
from datetime import datetime, timedelta
from dateutil.parser import parse
import traceback
from dbUtil.dbEngine import DBEngine
from pymongo.errors import DuplicateKeyError

TICK_DB_NAME = 'VnTrader_Tick_Db'

# ----------------------------------------------------------------------
"""处理每一个行情数据"""
def processDate(data):

    # 创建Tick对象
    tick = VtTickData()
    tick.gatewayName = 'HUOBI'
    tick.symbol = data['ch'].split('.')[1]
    tick.exchange = 'HUOBI'
    tick.vtSymbol = '.'.join([tick.symbol, tick.exchange])

    tick.datetime = datetime.fromtimestamp(data['ts'] / 1000)
    tick.date = tick.datetime.strftime('%Y%m%d')
    tick.time = tick.datetime.strftime('%H:%M:%S.%f')

    bids = data['tick']['bids']
    for n in range(5):
        l = bids[n]
        tick.__setattr__('bidPrice' + str(n + 1), float(l[0]))
        tick.__setattr__('bidVolume' + str(n + 1), float(l[1]))

    asks = data['tick']['asks']
    for n in range(5):
        l = asks[n]
        tick.__setattr__('askPrice' + str(n + 1), float(l[0]))
        tick.__setattr__('askVolume' + str(n + 1), float(l[1]))

    # 使用insert模式更新数据，可能存在时间戳重复的情况，需要用户自行清洗
    try:
        dbEngine.dbInsert(TICK_DB_NAME, tick.vtSymbol, tick)
    except DuplicateKeyError:
        print(u'键值重复插入失败，报错信息：%s' % traceback.format_exc())

if __name__ == "__main__":
    dbEngine = DBEngine()
    dbEngine.dbConnect()
    path = os.listdir(os.getcwd())
    for p in path:
        if '.txt' in p:
            print('开始处理文件:{}'.format(p))
            fopen=open(p,'r')
            lines = fopen.readlines()
            for line in lines:
                jsonObeject = json.loads(line)
                try:
                    processDate(jsonObeject)
                except:
                    pass
            print('处理完成：{}'.format(p))