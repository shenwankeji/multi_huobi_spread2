import os
import json
from datetime import datetime, timedelta
from dateutil.parser import parse
import traceback
from app.tick_record.dbEngine import DBEngine
from pymongo.errors import DuplicateKeyError
from trader.object import *

TICK_DB_NAME = 'VnTrader_Tick_Db'

# ----------------------------------------------------------------------
"""处理每一个行情数据"""


def processDate(data):
    # 创建Tick对象
    tick = DbTickData(
        symbol=data[0]['instrument_id'],
        datetime=parse(data[0]['timestamp']) + timedelta(hours=8),
        exchange='OKEX',
        gateway_name='OKEXF'
    )

    for n, buf in enumerate(data[0]['bids'][:5]):
        price, volume = buf[:2]
        tick.__setattr__("bid_price_%s" % (n + 1), price)
        tick.__setattr__("bid_volume_%s" % (n + 1), volume)

    for n, buf in enumerate(data[0]['asks'][:5]):
        price, volume = buf[:2]
        tick.__setattr__("ask_price_%s" % (n + 1), price)
        tick.__setattr__("ask_volume_%s" % (n + 1), volume)

    tick.date = tick.datetime.strftime('%Y%m%d')
    tick.time = tick.datetime.strftime('%H:%M:%S.%f')

    # 使用insert模式更新数据，可能存在时间戳重复的情况，需要用户自行清洗
    try:
        dbEngine.dbInsert(TICK_DB_NAME, tick.vt_symbol, tick)
    except DuplicateKeyError:
        print(u'键值重复插入失败，报错信息：%s' % traceback.format_exc())


if __name__ == "__main__":
    dbEngine = DBEngine()
    dbEngine.dbConnect()
    for d in ['20190301', '20190302', '20190303', '20190304', '20190305']:
        dir_path = r'F:\yjl_download_data\okexFutures{}\\'.format(d)
        path = os.listdir(dir_path)
        for p in path:
            if '.txt' in p:
                print('开始处理文件:{}'.format(p))
                fopen = open(dir_path + p, 'r')
                lines = fopen.readlines()
                for line in lines:
                    jsonObeject = json.loads(line)
                    processDate(jsonObeject)
                print('处理完成：{}'.format(p))
