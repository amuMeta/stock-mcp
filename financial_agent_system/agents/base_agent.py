from __future__ import annotations

import logging
from typing import Dict, List, Optional

from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class BaseAgent:
    """基础代理类"""
    
    def __init__(self, settings: Settings, harness):
        self.settings = settings
        self.harness = harness
        self.name = "base_agent"
        self.description = "基础代理"
        self.capabilities: List[str] = []
        self.is_initialized = False
    
    async def initialize(self):
        """初始化代理"""
        logger.info(f"初始化代理: {self.name}")
        self.is_initialized = True
    
    async def shutdown(self):
        """关闭代理"""
        logger.info(f"关闭代理: {self.name}")
        self.is_initialized = False
    
    async def execute(self, task: Dict) -> Dict:
        """执行任务"""
        raise NotImplementedError("子类必须实现execute方法")
    
    async def _get_market_data(self, symbol: str) -> Dict:
        """获取市场数据"""
        if self.harness.mcp_client:
            return await self.harness.mcp_client.get_market_data(symbol)
        return {"error": "MCP客户端未初始化"}
    
    async def _get_historical_data(self, symbol: str, period: str = "30d", interval: str = "1d") -> Dict:
        """获取历史数据"""
        if self.harness.mcp_client:
            return await self.harness.mcp_client.get_historical_data(symbol, period, interval)
        return {"error": "MCP客户端未初始化"}
