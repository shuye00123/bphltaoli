#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
优化版套利引擎测试脚本
"""

import asyncio
import logging
from funding_arbitrage_bot.core.arbitrage_engine_optimized import ArbitrageEngine

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# 测试配置
TEST_CONFIG = {
    'backpack': {
        'api_key': 'test_key',
        'api_secret': 'test_secret'
    },
    'hyperliquid': {
        'wallet_address': 'test_address',
        'private_key': 'test_key'
    },
    'symbols': ['BTC', 'ETH'],
    'min_rate_diff': 0.0005,
    'min_profit': 10.0,
    'default_position_size': 0.1,
    'max_position_size': {
        'BTC': 0.5,
        'ETH': 5.0
    },
    'interval': 60
}

async def test_engine():
    """测试优化版套利引擎"""
    engine = ArbitrageEngine(TEST_CONFIG)
    try:
        # 测试运行3个周期
        for _ in range(3):
            await engine._run_cycle()
            print("当前统计:", engine.get_stats())
    finally:
        await engine.shutdown()

if __name__ == '__main__':
    asyncio.run(test_engine())
