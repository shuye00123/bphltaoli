#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
多交易所套利机会识别测试
"""

import unittest
from unittest.mock import patch, MagicMock
import asyncio
import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from funding_arbitrage_bot.core.arbitrage_engine_optimized import ArbitrageEngine


class TestArbitrageOpportunities(unittest.TestCase):
    """测试套利机会识别"""

    def setUp(self):
        """设置测试环境"""
        self.config = {
            'backpack': {
                'api_key': 'test_key',
                'api_secret': 'test_secret'
            },
            'hyperliquid': {
                'wallet_pk': 'test_wallet_pk'
            },
            'binance': {
                'api_key': 'test_key',
                'api_secret': 'test_secret'
            },
            'okx': {
                'api_key': 'test_key',
                'api_secret': 'test_secret',
                'passphrase': 'test_passphrase'
            },
            'min_rate_diff': 0.001,  # 0.1%
            'default_position_size': 1000,
            'max_position_size': {'BTC': 1.0, 'ETH': 10.0, 'SOL': 100.0},
            'min_profit': 0.5,
            'symbols': ['BTC', 'ETH', 'SOL'],
            'interval': 60
        }

        # 创建一个事件循环用于运行异步测试
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        """清理测试环境"""
        self.loop.close()

    @patch('funding_arbitrage_bot.exchanges.backpack_api.BackpackAPI')
    @patch('funding_arbitrage_bot.exchanges.hyperliquid_api.HyperliquidAPI')
    @patch('funding_arbitrage_bot.exchanges.binance_api.BinanceAPI')
    @patch('funding_arbitrage_bot.exchanges.okx_api.OKXAPI')
    def test_find_arbitrage_opportunities(self, mock_okx, mock_binance, mock_hl, mock_bp):
        """测试识别套利机会"""
        # 模拟交易所API返回的资金费率数据
        mock_bp_instance = MagicMock()
        mock_bp_instance.get_funding_rates = asyncio.coroutine(lambda: {
            'BTC_USDC_PERP': 0.01,    # 1%
            'ETH_USDC_PERP': 0.005,   # 0.5%
            'SOL_USDC_PERP': -0.002   # -0.2%
        })
        mock_bp.return_value = mock_bp_instance

        mock_hl_instance = MagicMock()
        mock_hl_instance.get_funding_rates = asyncio.coroutine(lambda: {
            'BTC': 0.001,    # 0.1% (8小时等效: 0.8%)
            'ETH': 0.0008,   # 0.08% (8小时等效: 0.64%)
            'SOL': 0.0005    # 0.05% (8小时等效: 0.4%)
        })
        mock_hl.return_value = mock_hl_instance

        mock_binance_instance = MagicMock()
        mock_binance_instance.get_funding_rates = asyncio.coroutine(lambda: {
            'BTCUSDT': 0.008,    # 0.8%
            'ETHUSDT': 0.006,    # 0.6%
            'SOLUSDT': 0.003     # 0.3%
        })
        mock_binance.return_value = mock_binance_instance

        mock_okx_instance = MagicMock()
        mock_okx_instance.get_funding_rates = asyncio.coroutine(lambda: {
            'BTC-USDT-SWAP': 0.009,    # 0.9%
            'ETH-USDT-SWAP': 0.007,    # 0.7%
            'SOL-USDT-SWAP': -0.001    # -0.1%
        })
        mock_okx.return_value = mock_okx_instance

        # 创建套利引擎实例
        engine = ArbitrageEngine(self.config)

        # 模拟初始化交易所
        engine.exchanges = {
            'backpack': mock_bp_instance,
            'hyperliquid': mock_hl_instance,
            'binance': mock_binance_instance,
            'okx': mock_okx_instance
        }

        # 运行套利机会识别
        opportunities = self.loop.run_until_complete(engine._find_arbitrage_opportunities())

        # 验证结果
        self.assertTrue(len(opportunities) > 0, "应该找到至少一个套利机会")

        # 检查是否找到了预期的套利机会
        for opp in opportunities:
            symbol = opp['symbol']
            rate_diff = opp['rate_diff']
            direction = opp['direction']
            exchanges = opp['exchanges']

            # 验证结构是否正确
            self.assertIn(symbol, ['BTC', 'ETH', 'SOL'])
            self.assertIsInstance(rate_diff[0], float)
            self.assertIn(direction, ['long', 'short'])
            self.assertIn('long', exchanges)
            self.assertIn('short', exchanges)

            # 验证交易所是否正确
            self.assertIn(exchanges['long'], ['backpack', 'hyperliquid', 'binance', 'okx'])
            self.assertIn(exchanges['short'], ['backpack', 'hyperliquid', 'binance', 'okx'])
            self.assertNotEqual(exchanges['long'], exchanges['short'])

            # 验证资金费率差异是否足够大
            self.assertGreaterEqual(rate_diff[0], self.config['min_rate_diff'])


if __name__ == '__main__':
    unittest.main()
