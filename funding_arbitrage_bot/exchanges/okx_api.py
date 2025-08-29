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


class OKXAPI:
    """
    OKX交易所API实现
    支持期货交易、资金费率获取和WebSocket连接
    """

    def __init__(self, api_key: str = "", api_secret: str = "", passphrase: str = "", testnet: bool = False, logger=None, config=None):
        """
        初始化OKX API
        
        Args:
            api_key: OKX API密钥
            api_secret: OKX API密钥
            passphrase: OKX API密码
            testnet: 是否使用测试网络
            logger: 日志记录器
            config: 配置字典
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.testnet = testnet
        self.logger = logger or logging.getLogger("OKXAPI")
        self.config = config or {}
        
        # 设置基础URL
        if testnet:
            self.base_url = "https://www.okx.com/api/v5/mock"
            self.ws_base_url = "wss://wspap.okx.com:8443/ws/v5/business/mock"
        else:
            self.base_url = "https://www.okx.com/api/v5"
            self.ws_base_url = "wss://ws.okx.com:8443/ws/v5/business"
        
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
        
    async def init_session(self):
        """初始化HTTP会话"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self):
        """关闭HTTP会话和WebSocket连接"""
        if self.ws:
            await self.ws.close()
            self.ws = None
            self.ws_connected = False
            
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None

    def _generate_signature(self, timestamp: str, method: str, request_path: str, body: str = "") -> Tuple[str, str]:
        """
        生成API请求签名
        
        Args:
            timestamp: ISO格式的时间戳
            method: HTTP方法 (GET, POST, DELETE)
            request_path: 请求路径
            body: 请求体，默认为空字符串
            
        Returns:
            签名和时间戳
        """
        if body is None:
            body = ""
            
        message = timestamp + method + request_path + body
        
        mac = hmac.new(
            bytes(self.api_secret, encoding='utf8'),
            bytes(message, encoding='utf-8'),
            digestmod='sha256'
        )
        
        signature = base64.b64encode(mac.digest()).decode('utf-8')
        return signature, timestamp

    async def _make_request(self, method: str, endpoint: str, params: dict = None, 
                           data: dict = None) -> dict:
        """
        发送API请求
        
        Args:
            method: HTTP方法 (GET, POST, DELETE)
            endpoint: API端点
            params: URL参数
            data: 请求体数据
            
        Returns:
            API响应
        """
        await self.init_session()
        
        url = f"{self.base_url}/{endpoint}"
        
        # 准备请求头
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime())
        body = json.dumps(data) if data else ""
        
        signature, timestamp = self._generate_signature(
            timestamp, 
            method, 
            f"/{endpoint}" + (f"?{self._build_query_string(params)}" if params else ""), 
            body
        )
        
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json"
        }
        
        # 添加测试网络标志
        if self.testnet:
            headers["x-simulated-trading"] = "1"
        
        try:
            if method == "GET":
                async with self.session.get(url, params=params, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method == "POST":
                async with self.session.post(url, json=data, headers=headers) as response:
                    response.raise_for_status()
                    return await response.json()
            elif method == "DELETE":
                async with self.session.delete(url, params=params, headers=headers) as response:
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

    def _build_query_string(self, params: dict) -> str:
        """
        构建查询字符串
        
        Args:
            params: 参数字典
            
        Returns:
            查询字符串
        """
        if not params:
            return ""
        
        return "&".join([f"{k}={v}" for k, v in sorted(params.items())])

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_instruments(self, instType: str = "SWAP") -> List[Dict]:
        """
        获取交易对信息
        
        Args:
            instType: 产品类型，SWAP表示永续合约
            
        Returns:
            交易对信息列表
        """
        # 检查缓存
        cache_key = f"instruments_{instType}"
        current_time = time.time()
        if (cache_key in self.market_info_cache and 
            current_time - self.market_info_cache_time < self.market_info_cache_expiry):
            return self.market_info_cache[cache_key]
        
        params = {"instType": instType}
        response = await self._make_request("GET", "public/instruments", params=params)
        
        if response.get("code") == "0":
            # 更新缓存
            self.market_info_cache[cache_key] = response.get("data", [])
            self.market_info_cache_time = current_time
            return response.get("data", [])
        else:
            error_msg = f"Failed to get instruments: {response.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_funding_rates(self, instId: str = None) -> List[Dict]:
        """
        获取资金费率
        
        Args:
            instId: 产品ID，如果为None则获取所有产品
            
        Returns:
            资金费率列表
        """
        # 检查缓存
        current_time = time.time()
        if instId and instId in self.funding_rate_cache:
            if current_time - self.funding_rate_cache_time.get(instId, 0) < self.funding_rate_cache_expiry:
                return self.funding_rate_cache[instId]
        
        params = {}
        if instId:
            params["instId"] = instId
            
        response = await self._make_request("GET", "public/funding-rate", params=params)
        
        if response.get("code") == "0":
            result = response.get("data", [])
            
            # 更新缓存
            if instId:
                self.funding_rate_cache[instId] = result
                self.funding_rate_cache_time[instId] = current_time
                
            return result
        else:
            error_msg = f"Failed to get funding rates: {response.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_ticker(self, instId: str = None) -> Union[Dict, List[Dict]]:
        """
        获取最新价格
        
        Args:
            instId: 产品ID，如果为None则获取所有产品
            
        Returns:
            价格信息
        """
        # 检查缓存
        current_time = time.time()
        if instId and instId in self.price_cache:
            if current_time - self.price_cache_time.get(instId, 0) < self.price_cache_expiry:
                return self.price_cache[instId]
        
        params = {}
        if instId:
            params["instId"] = instId
            
        response = await self._make_request("GET", "market/ticker", params=params)
        
        if response.get("code") == "0":
            result = response.get("data", [])
            
            # 更新缓存
            if instId and result:
                self.price_cache[instId] = result[0]
                self.price_cache_time[instId] = current_time
                return result[0]
                
            return result
        else:
            error_msg = f"Failed to get ticker: {response.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_positions(self, instType: str = "SWAP", instId: str = None) -> List[Dict]:
        """
        获取持仓信息
        
        Args:
            instType: 产品类型，SWAP表示永续合约
            instId: 产品ID，如果为None则获取所有产品
            
        Returns:
            持仓信息列表
        """
        # 检查缓存
        current_time = time.time()
        cache_key = f"positions_{instType}_{instId or 'all'}"
        if (cache_key in self.position_cache and 
            current_time - self.position_cache_time < self.position_cache_expiry):
            return self.position_cache[cache_key]
        
        params = {"instType": instType}
        if instId:
            params["instId"] = instId
            
        response = await self._make_request("GET", "account/positions", params=params)
        
        if response.get("code") == "0":
            result = response.get("data", [])
            
            # 更新缓存
            self.position_cache[cache_key] = result
            self.position_cache_time = current_time
                
            return result
        else:
            error_msg = f"Failed to get positions: {response.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_balance(self, ccy: str = None) -> Dict:
        """
        获取账户余额
        
        Args:
            ccy: 币种，如果为None则获取所有币种
            
        Returns:
            账户余额信息
        """
        # 检查缓存
        current_time = time.time()
        cache_key = f"balance_{ccy or 'all'}"
        if (cache_key in self.balance_cache and 
            current_time - self.balance_cache_time < self.balance_cache_expiry):
            return self.balance_cache[cache_key]
        
        params = {}
        if ccy:
            params["ccy"] = ccy
            
        response = await self._make_request("GET", "account/balance", params=params)
        
        if response.get("code") == "0":
            result = response.get("data", [])
            
            # 更新缓存
            self.balance_cache[cache_key] = result
            self.balance_cache_time = current_time
                
            return result
        else:
            error_msg = f"Failed to get balance: {response.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def place_order(self, instId: str, tdMode: str, side: str, ordType: str, 
                         sz: str, px: str = None, posSide: str = None) -> Dict:
        """
        下单
        
        Args:
            instId: 产品ID
            tdMode: 交易模式，isolated(逐仓)，cross(全仓)
            side: 订单方向，buy(买)，sell(卖)
            ordType: 订单类型，market(市价)，limit(限价)，post_only(只做maker)，fok(全部成交或立即取消)，ioc(立即成交并取消剩余)
            sz: 委托数量
            px: 委托价格，仅适用于limit、post_only、fok、ioc类型的订单
            posSide: 持仓方向，long(做多)，short(做空)，在开平仓模式下必填
            
        Returns:
            订单信息
        """
        data = {
            "instId": instId,
            "tdMode": tdMode,
            "side": side,
            "ordType": ordType,
            "sz": sz
        }
        
        if px:
            data["px"] = px
            
        if posSide:
            data["posSide"] = posSide
            
        response = await self._make_request("POST", "trade/order", data=data)
        
        if response.get("code") == "0":
            return response.get("data", [{}])[0]
        else:
            error_msg = f"Failed to place order: {response.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def cancel_order(self, instId: str, ordId: str = None, clOrdId: str = None) -> Dict:
        """
        取消订单
        
        Args:
            instId: 产品ID
            ordId: 订单ID
            clOrdId: 客户自定义订单ID
            
        Returns:
            取消结果
        """
        if not ordId and not clOrdId:
            raise ValueError("Either ordId or clOrdId must be provided")
            
        data = {"instId": instId}
        
        if ordId:
            data["ordId"] = ordId
            
        if clOrdId:
            data["clOrdId"] = clOrdId
            
        response = await self._make_request("POST", "trade/cancel-order", data=data)
        
        if response.get("code") == "0":
            return response.get("data", [{}])[0]
        else:
            error_msg = f"Failed to cancel order: {response.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    async def get_order_details(self, instId: str, ordId: str = None, clOrdId: str = None) -> Dict:
        """
        获取订单详情
        
        Args:
            instId: 产品ID
            ordId: 订单ID
            clOrdId: 客户自定义订单ID
            
        Returns:
            订单详情
        """
        if not ordId and not clOrdId:
            raise ValueError("Either ordId or clOrdId must be provided")
            
        params = {"instId": instId}
        
        if ordId:
            params["ordId"] = ordId
            
        if clOrdId:
            params["clOrdId"] = clOrdId
            
        response = await self._make_request("GET", "trade/order", params=params)
        
        if response.get("code") == "0":
            return response.get("data", [{}])[0]
        else:
            error_msg = f"Failed to get order details: {response.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def connect_websocket(self):
        """
        连接WebSocket
        """
        if self.ws_connected:
            return
            
        try:
            self.ws = await websockets.connect(self.ws_base_url)
            self.ws_connected = True
            self.logger.info("WebSocket connected")
            
            # 启动WebSocket任务
            self.ws_task = asyncio.create_task(self._handle_websocket_messages())
            
            # 登录WebSocket
            if self.api_key and self.api_secret:
                await self._login_websocket()
                
            # 重新订阅之前的频道
            for subscription in self.ws_subscriptions:
                await self._send_websocket_message(subscription)
                
        except Exception as e:
            self.logger.error(f"WebSocket connection error: {str(e)}")
            self.ws_connected = False
            self.ws = None
            raise

    async def _login_websocket(self):
        """
        登录WebSocket
        """
        timestamp = str(int(time.time()))
        signature, _ = self._generate_signature(timestamp, "GET", "/users/self/verify", "")
        
        login_message = {
            "op": "login",
            "args": [{
                "apiKey": self.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": signature
            }]
        }
        
        await self._send_websocket_message(login_message)
        
        # 等待登录响应
        response = await self.ws.recv()
        response_data = json.loads(response)
        
        if response_data.get("event") == "login" and response_data.get("code") == "0":
            self.logger.info("WebSocket login successful")
        else:
            error_msg = f"WebSocket login failed: {response_data.get('msg', 'Unknown error')}"
            self.logger.error(error_msg)
            raise Exception(error_msg)

    async def _handle_websocket_messages(self):
        """
        处理WebSocket消息
        """
        try:
            while self.ws_connected:
                try:
                    message = await self.ws.recv()
                    await self._process_websocket_message(message)
                except ConnectionClosed:
                    self.logger.warning("WebSocket connection closed")
                    self.ws_connected = False
                    break
                except Exception as e:
                    self.logger.error(f"Error processing WebSocket message: {str(e)}")
        finally:
            self.ws_connected = False
            self.logger.info("WebSocket message handler stopped")
            
            # 尝试重新连接
            if not self.ws_connected:
                self.logger.info("Attempting to reconnect WebSocket")
                await asyncio.sleep(self.ws_reconnect_interval)
                asyncio.create_task(self.connect_websocket())

    async def _process_websocket_message(self, message: str):
        """
        处理WebSocket消息
        
        Args:
            message: WebSocket消息
        """
        try:
            data = json.loads(message)
            
            # 处理ping消息
            if "event" in data and data["event"] == "ping":
                await self._send_websocket_message({"op": "pong"})
                return
                
            # 处理订阅确认
            if "event" in data and data["event"] == "subscribe":
                self.logger.info(f"Successfully subscribed to channel: {data.get('arg', {})}")
                return
                
            # 处理数据更新
            if "data" in data:
                channel = data.get("arg", {}).get("channel")
                
                if channel == "tickers":
                    # 处理价格更新
                    for ticker in data["data"]:
                        inst_id = ticker.get("instId")
                        if inst_id:
                            self.last_price_update[inst_id] = ticker
                            
                elif channel == "funding-rate":
                    # 处理资金费率更新
                    for rate in data["data"]:
                        inst_id = rate.get("instId")
                        if inst_id:
                            self.last_funding_rate[inst_id] = rate
                            
        except json.JSONDecodeError:
            self.logger.error(f"Failed to parse WebSocket message: {message}")
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message: {str(e)}")

    async def _send_websocket_message(self, message: dict):
        """
        发送WebSocket消息
        
        Args:
            message: 消息字典
        """
        if not self.ws_connected:
            await self.connect_websocket()
            
        await self.ws.send(json.dumps(message))

    async def subscribe_ticker(self, instId: str):
        """
        订阅价格更新
        
        Args:
            instId: 产品ID
        """
        subscription = {
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": instId
            }]
        }
        
        self.ws_subscriptions.add(json.dumps(subscription))
        await self._send_websocket_message(subscription)

    async def subscribe_funding_rate(self, instId: str):
        """
        订阅资金费率更新
        
        Args:
            instId: 产品ID
        """
        subscription = {
            "op": "subscribe",
            "args": [{
                "channel": "funding-rate",
                "instId": instId
            }]
        }
        
        self.ws_subscriptions.add(json.dumps(subscription))
        await self._send_websocket_message(subscription)

    async def unsubscribe_ticker(self, instId: str):
        """
        取消订阅价格更新
        
        Args:
            instId: 产品ID
        """
        subscription = {
            "op": "unsubscribe",
            "args": [{
                "channel": "tickers",
                "instId": instId
            }]
        }
        
        subscription_str = json.dumps({
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": instId
            }]
        })
        
        if subscription_str in self.ws_subscriptions:
            self.ws_subscriptions.remove(subscription_str)
            
        await self._send_websocket_message(subscription)

    async def unsubscribe_funding_rate(self, instId: str):
        """
        取消订阅资金费率更新
        
        Args:
            instId: 产品ID
        """
        subscription = {
            "op": "unsubscribe",
            "args": [{
                "channel": "funding-rate",
                "instId": instId
            }]
        }
        
        subscription_str = json.dumps({
            "op": "subscribe",
            "args": [{
                "channel": "funding-rate",
                "instId": instId
            }]
        })
        
        if subscription_str in self.ws_subscriptions:
            self.ws_subscriptions.remove(subscription_str)
            
        await self._send_websocket_message(subscription)