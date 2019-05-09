# encoding: UTF-8

"""
展示如何执行策略回测。
"""

# from __future__ import division
from os import sys
from datetime import datetime
from time import time
from pandas import DataFrame
from trader.backtest_engine import BacktestMainEngine, OptimizationSetting
import multiprocessing
import gc


def optimize(
        target_name: str,
        setting: dict,
        start: datetime,
        end: datetime,
        rate: float,
        slippage: float,
        size: float,
        pricetick: float,
        capital: int,
):
    """
    Function for running in multiprocessing.pool
    """
    backtest_me = BacktestMainEngine()
    backtest_me.set_parameters(
        start=start,
        end=end,
        rate=rate,
        slippage=slippage,
        size=size,
        pricetick=pricetick,
        capital=capital)
    backtest_me.setting = setting
    # 启动价差引擎
    backtest_me.engines['St'].start()
    # 显示回测结果
    df = backtest_me.calculate_result()
    statistics = backtest_me.calculate_statistics()
    backtest_me.save_chart(setting)
    for k, v in setting.items():
        statistics[k] = v
    # statistics['start'] = start.date()
    # statistics['end'] = end.date()
    # statistics['rate'] = rate
    # statistics['slippage'] = slippage
    # statistics['size'] = size
    # statistics['pricetick'] = pricetick
    statistics['capital'] = capital
    statistics['capital'] = capital
    if statistics['max_ddpercent']:
        statistics['收益回撤比'] = statistics['total_return'] / statistics['max_ddpercent']
    if statistics['profit_days'] + statistics['loss_days']:
        statistics['胜率'] = statistics['profit_days'] / (statistics['profit_days'] + statistics['loss_days'])
    return statistics


if __name__ == '__main__':
    # 创建回测引擎
    # ee = engine.EventEngine()
    # backtest_me = BacktestMainEngine(ee)
    #
    # # 设置引擎的回测模式为K线
    # backtest_me.set_parameters(start=datetime(2019, 1, 20),
    #                            end=datetime(2019, 2, 5),
    #                            # end=datetime(2019, 3, 14),
    #                            rate=3 / 10000,
    #                            slippage=0,
    #                            size=10,
    #                            pricetick=0.001)
    #
    # backtest_me.engines['St'].start()  # 启动价差引擎
    #
    # # 显示回测结果
    # df = backtest_me.calculate_result()
    # backtest_me.calculate_statistics()
    # # backtest_me.show_chart()
    #
    # # 退出
    # sys.exit()

    # Use multiprocessing pool for running backtesting with different setting
    # pool = multiprocessing.Pool(multiprocessing.cpu_count())
    pool = multiprocessing.Pool(2)
    optimization_setting = OptimizationSetting()
    optimization_setting.add_parameter(name="buy_percent", start=0.006, end=0.01, step=0.002)
    # optimization_setting.add_parameter(name="buy_percent", start=0.025, end=0.056, step=0.005)
    optimization_setting.target_name = 'sharpe_ratio'
    settings = optimization_setting.generate_setting()
    target_name = optimization_setting.target_name

    results = []
    for setting in settings:
        result = (pool.apply_async(optimize, (target_name,
                                              setting,
                                              datetime(2019, 1, 20),
                                              # datetime(2019, 2, 5),  # 测试
                                              datetime(2019, 3, 14),
                                              3 / 10000,
                                              0,
                                              100,
                                              0.01,
                                              1)))
        gc.collect()
        results.append(result)

    pool.close()
    pool.join()
    # Sort results and output
    result_values = [result.get() for result in results]

    result_df = DataFrame(result_values).T
    result_df.to_excel(r'F:\backtest_result\{}.xlsx'.format(time()))
    # 退出
    sys.exit()
