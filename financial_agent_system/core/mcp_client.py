from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional

import httpx

from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class McpClient:
    """MCP客户端 - 连接Stock MCP服务"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: Optional[httpx.AsyncClient] = None
        self.base_url = f"http://{settings.stock_mcp_host}:{settings.stock_mcp_port}"
        self.mcp_endpoint = f"{self.base_url}/mcp"
    
    async def connect(self):
        """连接Stock MCP服务"""
        logger.info(f"连接Stock MCP服务: {self.base_url}")
        
        try:
            self.client = httpx.AsyncClient(timeout=30.0)
            
            # 测试连接
            response = await self.client.get(f"{self.base_url}/health")
            if response.status_code == 200:
                logger.info("Stock MCP服务连接成功")
            else:
                logger.warning(f"Stock MCP服务连接失败: {response.status_code}")
        except Exception as e:
            logger.error(f"Stock MCP服务连接失败: {e}")
    
    async def disconnect(self):
        """断开Stock MCP服务连接"""
        if self.client:
            await self.client.aclose()
            logger.info("Stock MCP服务连接已断开")
    
    async def call_mcp_tool(self, tool_name: str, params: Dict) -> Dict:
        """调用MCP工具"""
        if not self.client:
            return {"error": "MCP客户端未初始化"}
        
        try:
            payload = {
                "jsonrpc": "2.0",
                "method": tool_name,
                "params": params,
                "id": 1
            }
            
            response = await self.client.post(self.mcp_endpoint, json=payload)
            return response.json()
        except Exception as e:
            logger.error(f"调用MCP工具失败: {e}")
            return {"error": str(e)}
    
    async def get_market_data(self, symbol: str) -> Dict:
        """获取市场数据"""
        return await self.call_mcp_tool("get_real_time_price", {"symbol": symbol})
    
    async def get_historical_data(self, symbol: str, period: str = "30d", interval: str = "1d") -> Dict:
        """获取历史数据"""
        return await self.call_mcp_tool("get_historical_prices", {
            "symbol": symbol,
            "period": period,
            "interval": interval
        })
