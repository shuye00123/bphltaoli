#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
优化版套利引擎模块

实现资金费率套利策略的核心逻辑，包含以下优化：
1. 重试机制
2. 统一订单处理
3. 持仓管理
4. 增强异常处理
5. 性能统计
"""

import asyncio
import logging
import time
import os
import json
import functools
from typing import Dict, List, Optional, Any, Tuple, Union

# 导入模块
try:
    from funding_arbitrage_bot.exchanges.backpack_api import BackpackAPI
    from funding_arbitrage_bot.exchanges.hyperliquid_api import HyperliquidAPI
    from funding_arbitrage_bot.exchanges.binance_api import BinanceAPI
    from funding_arbitrage_bot.exchanges.okx_api import OKXAPI
    from funding_arbitrage_bot.core.data_manager import DataManager
    from funding_arbitrage_bot.utils.helpers import (
        calculate_funding_diff,
        get_backpack_symbol,
        get_hyperliquid_symbol,
        get_binance_symbol,
        get_okx_symbol
    )
except ImportError:
    from ..exchanges.backpack_api import BackpackAPI
    from ..exchanges.hyperliquid_api import HyperliquidAPI
    from ..exchanges.binance_api import BinanceAPI
    from ..exchanges.okx_api import OKXAPI
    from ..core.data_manager import DataManager
    from ..utils.helpers import (
        calculate_funding_diff,
        get_backpack_symbol,
        get_hyperliquid_symbol,
        get_binance_symbol,
        get_okx_symbol
    )

def retry_api(max_retries=3, delay=1):
    """API重试装饰器"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay * (attempt + 1))
            raise last_exception
        return wrapper
    return decorator

class PositionManager:
    """统一持仓管理"""
    def __init__(self, engine):
        self.engine = engine
        self.positions = {}

    async def sync_positions(self):
        """同步所有交易所持仓"""
        try:
            # 为所有已注册交易所创建持仓查询任务
            tasks = [
                (name, api.get_positions())
                for name, api in self.engine.exchanges.items()
                if hasattr(api, 'get_positions')
            ]
            
            # 并行执行所有查询
            results = await asyncio.gather(
                *(task for _, task in tasks),
                return_exceptions=True
            )
            
            # 处理结果
            self.positions = {}
            for (name, _), result in zip(tasks, results):
                if isinstance(result, Exception):
                    self.engine.logger.error(f"获取{name}持仓失败: {result}")
                else:
                    self.positions[name] = result
            
            return len(self.positions) > 0
        except Exception as e:
            self.engine.logger.error(f"同步持仓失败: {e}")
            return False

    def get_position(self, exchange: str, symbol: str) -> Optional[float]:
        """获取指定持仓"""
        return self.positions.get(exchange, {}).get(symbol)

    def get_all_positions(self) -> Dict[str, Dict[str, float]]:
        """获取所有交易所的持仓"""
        return self.positions

class ArbitrageEngine:
    """优化版套利引擎"""

    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.stats = {
            'total_trades': 0,
            'successful_trades': 0,
            'failed_trades': 0,
            'total_profit': 0.0,
            'exchanges': {}  # 各交易所统计
        }
        self.exchanges = {}  # 交易所API实例字典
        self._init_exchanges(config)
        self.position_manager = PositionManager(self)


    def _init_exchanges(self, config: Dict):
        """初始化所有配置的交易所"""
        if 'backpack' in config:
            self.exchanges['backpack'] = BackpackAPI
            self.stats['exchanges']['backpack'] = {'trades': 0}
        if 'hyperliquid' in config:
            self.exchanges['hyperliquid'] = HyperliquidAPI
            self.stats['exchanges']['hyperliquid'] = {'trades': 0}
        
        # 添加Binance和OKX交易所初始化
        if 'binance' in config:
            self.exchanges['binance'] = BinanceAPI
            self.stats['exchanges']['binance'] = {'trades': 0}
        if 'okx' in config:
            self.exchanges['okx'] = OKXAPI
            self.stats['exchanges']['okx'] = {'trades': 0}

    def register_exchange(self, name: str, api_instance: Any):
        """动态注册新交易所"""
        if name in self.exchanges:
            raise ValueError(f"交易所 {name} 已存在")
        self.exchanges[name] = api_instance
        self.stats['exchanges'][name] = {'trades': 0}

    @retry_api()
    async def _place_exchange_order(self, exchange: str, symbol: str,
                                  side: str, size: float, price: float) -> Dict:
        """统一订单处理方法"""
        try:
            if exchange not in self.exchanges:
                raise ValueError(f"未注册的交易所: {exchange}")
            
            api = self.exchanges[exchange]
            if not hasattr(api, 'place_order'):
                raise ValueError(f"交易所 {exchange} 不支持下单")
                
            result = await api.place_order(symbol, side, size, price)
            self.stats['exchanges'][exchange]['trades'] += 1
            return result
            
        except Exception as e:
            self.logger.error(f"在{exchange}下单失败: {e}")
            raise

    async def _calculate_slippage(self, exchange: str, symbol: str,
                                side: str, size: float) -> float:
        """计算预估滑点"""
        orderbook = await self._get_orderbook(exchange, symbol)
        return self._estimate_slippage(orderbook, side, size)

    async def start(self):
        """启动套利引擎主循环"""
        self.logger.info("启动套利引擎")
        try:
            while True:
                await self._run_cycle()
                await asyncio.sleep(self.config['interval'])
        except asyncio.CancelledError:
            self.logger.info("套利引擎正常停止")
        except Exception as e:
            self.logger.error(f"套利引擎异常停止: {e}")
            raise

    async def _run_cycle(self):
        """执行一个完整的套利周期"""
        try:
            # 同步持仓状态
            await self.position_manager.sync_positions()
            
            # 获取套利机会
            opportunities = await self._find_arbitrage_opportunities()
            
            # 执行套利
            for opp in opportunities:
                await self._execute_arbitrage(opp)
                
        except Exception as e:
            self.stats['failed_trades'] += 1
            self.logger.error(f"套利周期执行失败: {e}")

    async def _find_arbitrage_opportunities(self) -> List[Dict]:
        """寻找套利机会"""
        # 获取所有交易所的资金费率
        funding_rates = {}
        for exchange_name, api in self.exchanges.items():
            if hasattr(api, 'get_funding_rates'):
                try:
                    rates = await api.get_funding_rates()
                    funding_rates[exchange_name] = rates
                except Exception as e:
                    self.logger.error(f"获取{exchange_name}资金费率失败: {e}")
        
        if len(funding_rates) < 2:
            self.logger.warning("可用交易所数量不足，无法进行套利")
            return []
        
        opportunities = []
        # 遍历所有交易所对组合
        exchange_pairs = [(a, b) for a in funding_rates.keys() for b in funding_rates.keys() if a < b]
        
        for symbol in self.config['symbols']:
            for exchange_a, exchange_b in exchange_pairs:
                # 获取两个交易所的资金费率
                rate_a = funding_rates[exchange_a].get(symbol, 0)
                rate_b = funding_rates[exchange_b].get(symbol, 0)
                
                # 计算资金费率差
                rate_diff = calculate_funding_diff(rate_a, rate_b)
                
                if abs(rate_diff[0]) > self.config['min_rate_diff']:
                    opportunities.append({
                        'symbol': symbol,
                        'rate_diff': rate_diff,
                        'direction': 'long' if rate_diff[1] > 0 else 'short',
                        'exchanges': {
                            'long': exchange_a if rate_diff[1] > 0 else exchange_b,
                            'short': exchange_b if rate_diff[1] > 0 else exchange_a
                        }
                    })
        
        # 按资金费率差的绝对值排序，优先处理差异最大的机会
        opportunities.sort(key=lambda x: abs(x['rate_diff'][0]), reverse=True)
        return opportunities

    async def _execute_arbitrage(self, opportunity: Dict):
        """执行套利交易"""
        symbol = opportunity['symbol']
        direction = opportunity['direction']
        
        # 获取交易所信息
        long_exchange = opportunity['exchanges']['long']
        short_exchange = opportunity['exchanges']['short']
        
        try:
            # 计算开仓数量
            size = self._calculate_position_size(symbol)
            
            # 获取最优价格
            long_price = await self._get_best_price(long_exchange, symbol, 'buy')
            short_price = await self._get_best_price(short_exchange, symbol, 'sell')
            
            # 计算预期收益
            expected_profit = self._calculate_expected_profit(
                'arbitrage', size, long_price, short_price
            )
            
            if expected_profit > self.config['min_profit']:
                # 执行对冲交易
                self.logger.info(f"执行套利交易: {symbol} - 做多{long_exchange}，做空{short_exchange}")
                
                # 并行执行两个交易所的订单
                await asyncio.gather(
                    self._place_exchange_order(long_exchange, symbol, 'buy', size, long_price),
                    self._place_exchange_order(short_exchange, symbol, 'sell', size, short_price)
                )
                
                self.stats['successful_trades'] += 1
                self.stats['total_profit'] += expected_profit
                self.logger.info(f"成功执行套利交易: {symbol} {direction} 预期收益: {expected_profit:.4f}")
                
                # 记录交易详情
                trade_details = {
                    'timestamp': time.time(),
                    'symbol': symbol,
                    'long_exchange': long_exchange,
                    'short_exchange': short_exchange,
                    'size': size,
                    'long_price': long_price,
                    'short_price': short_price,
                    'expected_profit': expected_profit,
                    'funding_rate_diff': opportunity['rate_diff']
                }
                
                # 如果配置了数据管理器，保存交易记录
                if hasattr(self, 'data_manager') and self.data_manager:
                    await self.data_manager.save_trade(trade_details)
        
        except Exception as e:
            self.stats['failed_trades'] += 1
            self.logger.error(f"套利交易执行失败: {symbol} {direction} - {e}")
            raise

    async def _get_best_price(self, exchange: str, symbol: str, side: str) -> float:
        """获取交易所最优价格"""
        orderbook = await self._get_orderbook(exchange, symbol)
        return orderbook['bids'][0][0] if side == 'sell' else orderbook['asks'][0][0]

    @retry_api()
    async def _get_orderbook(self, exchange: str, symbol: str) -> Dict:
        """获取订单簿数据"""
        if exchange not in self.exchanges:
            raise ValueError(f"未注册的交易所: {exchange}")
            
        api = self.exchanges[exchange]
        if not hasattr(api, 'get_orderbook'):
            raise ValueError(f"交易所 {exchange} 不支持获取订单簿")
            
        return await api.get_orderbook(symbol)

    def _calculate_position_size(self, symbol: str) -> float:
        """计算开仓数量"""
        max_size = self.config['max_position_size'].get(symbol, 1.0)
        return min(max_size, self.config['default_position_size'])

    def _calculate_expected_profit(self, direction: str, size: float,
                                 long_price: float, short_price: float) -> float:
        """计算预期收益"""
        if direction == 'long':
            return size * (short_price - long_price)
        else:
            return size * (long_price - short_price)

    def _estimate_slippage(self, orderbook: Dict, side: str, size: float) -> float:
        """估算滑点"""
        levels = orderbook['bids'] if side == 'sell' else orderbook['asks']
        remaining = size
        slippage = 0.0
        for price, amount in levels:
            if remaining <= 0:
                break
            taken = min(remaining, amount)
            slippage += taken * price
            remaining -= taken
        return slippage / size if size > 0 else 0.0

    def get_stats(self) -> Dict:
        """获取交易统计数据"""
        return {
            **self.stats,
            'success_rate': self.stats['successful_trades'] / self.stats['total_trades'] 
                          if self.stats['total_trades'] > 0 else 0.0,
            'avg_profit': self.stats['total_profit'] / self.stats['successful_trades']
                        if self.stats['successful_trades'] > 0 else 0.0
        }

    async def shutdown(self):
        """关闭套利引擎"""
        self.logger.info("正在关闭套利引擎...")
        
        # 关闭所有已注册的交易所连接
        close_tasks = []
        for name, api in self.exchanges.items():
            if hasattr(api, 'close'):
                close_tasks.append(api.close())
        
        if close_tasks:
            await asyncio.gather(*close_tasks)
        self.logger.info("套利引擎已关闭")
