#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
多交易所套利功能测试
"""

import unittest
from funding_arbitrage_bot.utils.helpers import (
    calculate_funding_diff,
    get_backpack_symbol,
    get_hyperliquid_symbol,
    get_binance_symbol,
    get_okx_symbol
)


class TestFundingRateCalculation(unittest.TestCase):
    """测试资金费率差异计算"""

    def test_same_period_calculation(self):
        """测试相同周期的资金费率差异计算"""
        # 两个交易所都是8小时周期
        funding1 = 0.01  # 1%
        funding2 = 0.005  # 0.5%
        diff, sign = calculate_funding_diff(funding1, funding2, period1=8, period2=8)

        self.assertAlmostEqual(diff, 0.005)  # 差异应该是0.5%
        self.assertEqual(sign, 1)  # funding1 > funding2，所以符号是1

        # 反向测试
        diff, sign = calculate_funding_diff(funding2, funding1, period1=8, period2=8)
        self.assertAlmostEqual(diff, 0.005)  # 差异仍然是0.5%
        self.assertEqual(sign, -1)  # funding2 < funding1，所以符号是-1

    def test_different_period_calculation(self):
        """测试不同周期的资金费率差异计算"""
        # Backpack (8小时) vs Hyperliquid (1小时)
        bp_funding = 0.008  # 0.8%
        hl_funding = 0.001  # 0.1%

        # 期望Hyperliquid的资金费率被调整为8小时周期: 0.001 * 8 = 0.008
        # 差异应该是0
        diff, sign = calculate_funding_diff(bp_funding, hl_funding, period1=8, period2=1)

        self.assertAlmostEqual(diff, 0.0)
        self.assertEqual(sign, 0)

        # 测试不同值
        bp_funding = 0.01  # 1%
        hl_funding = 0.001  # 0.1%

        # 期望Hyperliquid的资金费率被调整为8小时周期: 0.001 * 8 = 0.008
        # 差异应该是0.01 - 0.008 = 0.002
        diff, sign = calculate_funding_diff(bp_funding, hl_funding, period1=8, period2=1)

        self.assertAlmostEqual(diff, 0.002)
        self.assertEqual(sign, 1)

        # 反向测试
        diff, sign = calculate_funding_diff(hl_funding, bp_funding, period1=1, period2=8)

        # 期望Hyperliquid的资金费率保持不变，Backpack的资金费率被调整为1小时周期: 0.01 / 8 = 0.00125
        # 差异应该是0.001 - 0.00125 = 0.00025
        self.assertAlmostEqual(diff, 0.00025)
        self.assertEqual(sign, -1)

    def test_negative_funding_rates(self):
        """测试负资金费率的情况"""
        # Backpack负费率，Hyperliquid正费率
        bp_funding = -0.008  # -0.8%
        hl_funding = 0.001  # 0.1%

        # 期望Hyperliquid的资金费率被调整为8小时周期: 0.001 * 8 = 0.008
        # 差异应该是-0.008 - 0.008 = -0.016
        diff, sign = calculate_funding_diff(bp_funding, hl_funding, period1=8, period2=1)

        self.assertAlmostEqual(diff, 0.016)
        self.assertEqual(sign, -1)

        # 两个交易所都是负费率
        bp_funding = -0.008  # -0.8%
        hl_funding = -0.002  # -0.2%

        # 期望Hyperliquid的资金费率被调整为8小时周期: -0.002 * 8 = -0.016
        # 差异应该是-0.008 - (-0.016) = 0.008
        diff, sign = calculate_funding_diff(bp_funding, hl_funding, period1=8, period2=1)

        self.assertAlmostEqual(diff, 0.008)
        self.assertEqual(sign, 1)


class TestSymbolConversion(unittest.TestCase):
    """测试交易所符号转换"""

    def test_backpack_symbol_conversion(self):
        """测试Backpack符号转换"""
        self.assertEqual(get_backpack_symbol("BTC"), "BTC_USDC_PERP")
        self.assertEqual(get_backpack_symbol("ETH"), "ETH_USDC_PERP")
        self.assertEqual(get_backpack_symbol("SOL"), "SOL_USDC_PERP")

    def test_hyperliquid_symbol_conversion(self):
        """测试Hyperliquid符号转换"""
        self.assertEqual(get_hyperliquid_symbol("BTC"), "BTC")
        self.assertEqual(get_hyperliquid_symbol("ETH"), "ETH")
        self.assertEqual(get_hyperliquid_symbol("SOL"), "SOL")

    def test_binance_symbol_conversion(self):
        """测试Binance符号转换"""
        self.assertEqual(get_binance_symbol("BTC"), "BTCUSDT")
        self.assertEqual(get_binance_symbol("ETH"), "ETHUSDT")
        self.assertEqual(get_binance_symbol("SOL"), "SOLUSDT")

    def test_okx_symbol_conversion(self):
        """测试OKX符号转换"""
        self.assertEqual(get_okx_symbol("BTC"), "BTC-USDT-SWAP")
        self.assertEqual(get_okx_symbol("ETH"), "ETH-USDT-SWAP")
        self.assertEqual(get_okx_symbol("SOL"), "SOL-USDT-SWAP")


if __name__ == '__main__':
    unittest.main()
