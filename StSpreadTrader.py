# 杨切 20180830 创建策略

# encoding: UTF-8

import multiprocessing
from time import sleep
from datetime import datetime, time

from event import engine
from trader.event import EVENT_LOG, EVENT_ERROR
from trader.engine import MainEngine
from gateway.okexf import OkexfGateway
from gateway.huobi.huobifGateway import HuobifGateway
# from vnpy.gateway.ib import IbGateway
# from vnpy.gateway.ib import IbGateway
# from vnpy.gateway import okexfGateway
from trader.gateway import BaseGateway
from app.spreadTrading import *

EVENT_SPREADTRADING_TICK = 'eSpreadTradingTick.'
EVENT_SPREADTRADING_POS = 'eSpreadTradingPos.'
EVENT_SPREADTRADING_LOG = 'eSpreadTradingLog'
EVENT_SPREADTRADING_ALGO = 'eSpreadTradingAlgo.'
EVENT_SPREADTRADING_ALGOLOG = 'eSpreadTradingAlgoLog'


# ----------------------------------------------------------------------
def processErrorEvent(event):
    """
    处理错误事件
    错误信息在每次登陆后，会将当日所有已产生的均推送一遍，所以不适合写入日志
    """
    error = event.dict_['data']
    print(u'错误代码：%s，错误信息：%s' % (error.errorID, error.errorMsg))


# ----------------------------------------------------------------------
def runChildProcess():
    """子进程运行函数"""
    print('-' * 20)
    # le = LogEngine()
    # le.setLogLevel(le.LEVEL_INFO)
    # le.setLogLevel(le.LEVEL_DEBUG)
    # le.addConsoleHandler()
    # le.addFileHandler()
    # le.info(u'启动HUOBI策略运行子进程')
    ee = engine.EventEngine()
    me = MainEngine(ee)
    ee.register(EVENT_LOG, me.engines['log'].process_log_event)
    me.add_gateway(HuobifGateway)
    me.connect({}, 'HUOBIF')
    # me.add_gateway(OkexfGateway)
    # me.connect({}, 'OKEXF')
    me.engines['St'].start()   # 启动价差引擎
    # print('ok')
    # arb = StEngine(MainEngine(ee), ee)
    # ee.register(EVENT_ERROR, processErrorEvent)

    # ee.register(EVENT_SPREADTRADING_LOG, le.processLogEvent)
    # ee.register(EVENT_SPREADTRADING_ALGOLOG, le.processLogEvent)
    # arb.mainEngine.add_gateway(OkexfGateway)
    # arb.mainEngine.addGateway(huobiGateway)
    # arb.mainEngine.addGateway(okexGateway)
    # arb.eventEngine.register(EVENT_ERROR, processErrorEvent)
    # arb.mainEngine.connect({}, 'OKEXF')
    # arb.mainEngine.connect('HUOBI')
    # arb.mainEngine.connect('OKEX')
    # arb.init()


# ----------------------------------------------------------------------
# def runParentProcess():
#     """父进程运行函数"""
#     # 创建日志引擎
#     le = LogEngine()
#     le.setLogLevel(le.LEVEL_INFO)
#     le.addConsoleHandler()
#
#     le.info(u'启动ARB策略守护父进程')
#
#     DAY_START = time(8, 45)  # 日盘启动和停止时间
#     DAY_END = time(15, 30)
#
#     NIGHT_START = time(20, 45)  # 夜盘启动和停止时间
#     NIGHT_END = time(2, 45)
#
#     p = None  # 子进程句柄
#
#     while True:
#         currentTime = datetime.now().time()
#         recording = False
#
#         # 判断当前处于的时间段
#         if ((currentTime >= DAY_START and currentTime <= DAY_END) or
#                 (currentTime >= NIGHT_START) or
#                 (currentTime <= NIGHT_END)):
#             recording = True
#
#         # 记录时间则需要启动子进程
#         if recording and p is None:
#             le.info(u'启动子进程')
#             p = multiprocessing.Process(target=runChildProcess)
#             p.start()
#             le.info(u'子进程启动成功')
#
#         # 非记录时间则退出子进程
#         if not recording and p is not None:
#             le.info(u'关闭子进程')
#             p.terminate()
#             p.join()
#             p = None
#             le.info(u'子进程关闭成功')
#
#         sleep(5)


if __name__ == '__main__':
    runChildProcess()

    # 尽管同样实现了无人值守，但强烈建议每天启动时人工检查，为自己的PNL负责
    # runParentProcess()
