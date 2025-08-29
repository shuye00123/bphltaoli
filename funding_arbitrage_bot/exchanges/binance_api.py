import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from typing import Dict, List, Optional, Tuple, Union

import aiohttp
import pandas as pd
import websockets
from websockets.exceptions import ConnectionClosed

from tenacity import retry, stop_after_attempt, wait_fixed

class BinanceAPI:
    """
    Binance交易所API实现
    支持期货交易、资金费率获取和WebSocket连接
    """

    def __init__(self, api_key: str = "", api_secret: str = "", testnet: bool = False):
        """
        初始化Binance API

        Args:
            api_key: Binance API密钥
            api_secret: Binance API密钥
            testnet: 是否使用测试网络
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet

        # 设置基础URL
        if testnet:
            self.base_url = "https://testnet.binancefuture.com"
            self.ws_base_url = "wss://stream.binancefuture.com/ws"
        else:
            self.base_url = "https://fapi.binance.com"
            self.ws_base_url = "wss://fstream.binance.com/ws"

        self.logger = logging.getLogger("BinanceAPI")
        self.session = None
        self.ws = None
        self.ws_connected = False
        self.ws_subscriptions = set()
        self.last_price_update = {}
        self.last_funding_rate = {}
        self.funding_rate_cache = {}
        self.funding_rate_cache_time = {}
        self.funding_rate_cache_expiry = 60  # 缓存过期时间（秒）
        self.price_cache = {}
        self.price_cache_time = {}
        self.price_cache_expiry = 5  # 缓存过期时间（秒）
        self.position_cache = {}
        self.position_cache_time = 0
        self.position_cache_expiry = 5  # 缓存过期时间（秒）
        self.balance_cache = {}
        self.balance_cache_time = 0
        self.balance_cache_expiry = 5  # 缓存过期时间（秒）
        self.market_info_cache = {}
        self.market_info_cache_time = 0
        self.market_info_cache_expiry = 3600  # 缓存过期时间（秒）
        self.ws_task = None
        self.ws_reconnect_interval = 5  # WebSocket重连间隔（秒）
        self.ws_ping_interval = 30  # WebSocket ping间隔（秒）
        self.ws_last_ping_time = 0
        self.ws_last_pong_time = 0
        self.ws_ping_timeout = 10  # WebSocket ping超时（秒）
        self.ws_reconnect_count = 0
        self.ws_max_reconnect_count = 10
        self.ws_reconnect_delay = 5  # WebSocket重连延迟（秒）
        self.ws_reconnect_delay_max = 60  # WebSocket最大重连延迟（秒）
        self.ws_reconnect_delay_factor = 2  # WebSocket重连延迟因子
        self.ws_ping_task = None
        self.ws_message_handlers = {}
        self.ws_error_handlers = []
        self.ws_close_handlers = []
        self.ws_open_handlers = []
        self.ws_reconnect_handlers = []
        self.ws_ping_handlers = []
        self.ws_pong_handlers = []
        self.ws_timeout_handlers = []
        self.ws_error_count = 0
        self.ws_max_error_count = 10
        self.ws_error_count_reset_interval = 60  # WebSocket错误计数重置间隔（秒）
        self.ws_last_error_time = 0
        self.ws_error_delay = 5  # WebSocket错误延迟（秒）
        self.ws_error_delay_max = 60  # WebSocket最大错误延迟（秒）
        self.ws_error_delay_factor = 2  # WebSocket错误延迟因子
        self.ws_message_count = 0
        self.ws_last_message_time = 0
        self.ws_message_timeout = 60  # WebSocket消息超时（秒）
        self.ws_message_timeout_handlers = []
        self.ws_message_count_reset_interval = 60  # WebSocket消息计数重置间隔（秒）
        self.ws_last_message_count_reset_time = 0
        self.ws_message_count_reset_handlers = []
        self.ws_message_count_reset_task = None
        self.ws_message_timeout_task = None
        self.ws_error_count_reset_task = None
        self.ws_ping_pong_timeout_task = None
        self.ws_ping_pong_timeout_handlers = []
        self.ws_ping_pong_timeout = 10  # WebSocket ping-pong超时（秒）
        self.ws_ping_pong_timeout_count = 0
        self.ws_max_ping_pong_timeout_count = 3
        self.ws_ping_pong_timeout_count_reset_interval = 60  # WebSocket ping-pong超时计数重置间隔（秒）
        self.ws_last_ping_pong_timeout_count_reset_time = 0
        self.ws_ping_pong_timeout_count_reset_task = None
        self.ws_ping_pong_timeout_count_reset_handlers = []
        self.ws_ping_pong_timeout_count_reset_delay = 5  # WebSocket ping-pong超时计数重置延迟（秒）
        self.ws_ping_pong_timeout_count_reset_delay_max = 60  # WebSocket ping-pong超时计数重置最大延迟（秒）
        self.ws_ping_pong_timeout_count_reset_delay_factor = 2  # WebSocket ping-pong超时计数重置延迟因子
        self.ws_ping_pong_timeout_count_reset_error_count = 0
        self.ws_max_ping_pong_timeout_count_reset_error_count = 10
        self.ws_ping_pong_timeout_count_reset_error_delay = 5  # WebSocket ping-pong超时计数重置错误延迟（秒）
        self.ws_ping_pong_timeout_count_reset_error_delay_max = 60  # WebSocket ping-pong超时计数重置最大错误延迟（秒）
        self.ws_ping_pong_timeout_count_reset_error_delay_factor = 2  # WebSocket ping-pong超时计数重置错误延迟因子
        self.ws_ping_pong_timeout_count_reset_error_handlers = []
        self.ws_ping_pong_timeout_count_reset_error_task = None
        self.ws_ping_pong_timeout_count_reset_error_timeout = 60  # WebSocket ping-pong超时计数重置错误超时（秒）
        self.ws_ping_pong_timeout_count_reset_error_timeout_handlers = []
        self.ws_ping_pong_timeout_count_reset_error_timeout_task = None
        self.ws_ping_pong_timeout_count_reset_error_timeout_count = 0
        self.ws_max_ping_pong_timeout_count_reset_error_timeout_count = 10
        self.ws_ping_pong_timeout_count_reset_error_timeout_count_reset_interval = 60  # WebSocket ping-pong超时计数重置错误超时计数重置间隔（秒）
        self.ws_last_ping_pong_timeout_count_reset_error_timeout_count_reset_time = 0
        self.ws_ping_pong_timeout_count_reset_error_timeout_count_reset_task = None
        self.ws_ping_pong_timeout_count_reset_error_timeout_count_reset_handlers = []
        self.ws_ping_pong_timeout_count_reset_error_timeout_count_reset_delay = 5  # WebSocket ping-pong超时计数重置错误超时计数重置延迟（秒）
        self.ws_ping_pong_timeout_count_reset_error_timeout_count_reset_delay_max = 60  # WebSocket ping-pong超时计数重置错误超时计数重置最大延迟（秒）
        self.ws_ping_pong_timeout_count_reset_error_timeout_count_reset_delay_factor = 2  # WebSocket ping-pong超时计数重置错误超时计数重置延迟因子

    async def init_session(self):
        """初始化HTTP会话"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close_session(self):
        """关闭HTTP会话"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    def _generate_signature(self, query_string: str) -> str:
        """
        生成API请求签名

        Args:
            query_string: 查询字符串

        Returns:
            签名字符串
        """
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    async def _make_request(self, method: str, endpoint: str, params: dict = None,
                           data: dict = None, signed: bool = False,
                           api_version: str = "v1") -> dict:
        """
        发送API请求

        Args:
            method: HTTP方法 (GET, POST, DELETE, PUT)
            endpoint: API端点
            params: URL参数
            data: 请求体数据
            signed: 是否需要签名
            api_version: API版本

        Returns:
            API响应
        """
        await self.init_session()

        url = f"{self.base_url}/fapi/{api_version}/{endpoint}"
        headers = {"X-MBX-APIKEY": self.api_key} if self.api_key else {}

        if params is None:
            params = {}

        if signed:
            # 添加时间戳
            params['timestamp'] = int(time.time() * 1000)

            # 构建查询字符串
            query_string = '&'.join([f"{k}={v}" for k, v in sorted(params.items())])

            # 生成签名
            signature = self._generate_signature(query_string)
            params['signature'] = signature

        try:
            if method == "GET":
                async with self.session.get(url, params=params, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method == "POST":
                async with self.session.post(url, params=params, json=data, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method == "DELETE":
                async with self.session.delete(url, params=params, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method == "PUT":
                async with self.session.put(url, params=params, json=data, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
        except aiohttp.ClientResponseError as e:
            self.logger.error(f"API request error: {e.status} {e.message}")
            raise
        except aiohttp.ClientError as e:
            self.logger.error(f"API connection error: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_exchange_info(self) -> dict:
        """
        获取交易所信息

        Returns:
            交易所信息
        """
        # 检查缓存
        current_time = time.time()
        if (self.market_info_cache and
            current_time - self.market_info_cache_time < self.market_info_cache_expiry):
            return self.market_info_cache

        response = await self._make_request("GET", "exchangeInfo")

        # 更新缓存
        self.market_info_cache = response
        self.market_info_cache_time = current_time

        return response

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_funding_rates(self, symbol: str = None) -> List[Dict]:
        """
        获取资金费率

        Args:
            symbol: 交易对名称，如果为None则获取所有交易对

        Returns:
            资金费率列表
        """
        # 检查缓存
        current_time = time.time()
        if symbol and symbol in self.funding_rate_cache:
            if current_time - self.funding_rate_cache_time.get(symbol, 0) < self.funding_rate_cache_expiry:
                return self.funding_rate_cache[symbol]

        params = {}
        if symbol:
            params['symbol'] = symbol

        response = await self._make_request("GET", "fundingRate", params=params)

        # 更新缓存
        if symbol:
            self.funding_rate_cache[symbol] = response
            self.funding_rate_cache_time[symbol] = current_time

        return response

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_ticker_price(self, symbol: str = None) -> Union[Dict, List[Dict]]:
        """
        获取最新价格

        Args:
            symbol: 交易对名称，如果为None则获取所有交易对的最新价格

        Returns:
            单个交易对的最新价格字典 或 所有交易对的最新价格列表
        """
        # 检查缓存
        current_time = time.time()
        if symbol and symbol in self.ticker_price_cache:
            if current_time - self.ticker_price_cache_time.get(symbol, 0) < self.ticker_price_cache_expiry:
                return self.ticker_price_cache[symbol]

        params = {}
        if symbol:
            params['symbol'] = symbol

        response = await self._make_request("GET", "ticker/price", params=params)

        # 更新缓存
        if symbol:
            self.ticker_price_cache[symbol] = response
            self.ticker_price_cache_time[symbol] = current_time

        return response

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_order_book(self, symbol: str, limit: int = 100) -> dict:
        """
        Get order book for a trading pair

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            limit: Maximum number of entries to return (default: 100, max: 1000)

        Returns:
            Order book dictionary
        """
        params = {'symbol': symbol, 'limit': min(limit, 1000)}
        response = await self._make_request("GET", "depth", params=params)
        return response

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_recent_trades(self, symbol: str, limit: int = 100) -> List[dict]:
        """
        Get recent trades for a trading pair

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            limit: Maximum number of trades to return (default: 100, max: 1000)

        Returns:
            List of recent trade dictionaries
        """
        params = {'symbol': symbol, 'limit': min(limit, 1000)}
        response = await self._make_request("GET", "trades", params=params)
        return response

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_historical_klines(self, symbol: str, interval: str,
                                    start_time: int, end_time: int,
                                    limit: int = 1000) -> List[List]:
        """
        Get historical klines (candlestick) data

        Args:
            symbol: Trading pair symbol (e.g., "BTCUSDT")
            interval: Kline interval (e.g., "1m", "1h", "1d")
            start_time: Start time in milliseconds since epoch
            end_time: End time in milliseconds since epoch
            limit: Maximum number of klines to return (default: 1000, max: 1000)

        Returns:
            List of kline lists (each containing [open_time, open, high, low, close, volume, etc.])
        """
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': min(limit, 1000)
        }
        response = await self._make_request("GET", "klines", params=params)
        return response
