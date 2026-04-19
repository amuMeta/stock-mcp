from __future__ import annotations

import logging
import pandas as pd
from typing import Dict, List

from financial_agent_system.agents.base_agent import BaseAgent
from financial_agent_system.core.analysis_engine import AnalysisEngine
from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class FinancialAnalyzerAgent(BaseAgent):
    """金融分析代理"""
    
    def __init__(self, settings: Settings, harness):
        super().__init__(settings, harness)
        self.name = "financial_analyzer"
        self.description = "金融分析代理，负责市场数据分析和财务分析"
        self.capabilities = [
            "market_analysis",
            "financial_statement_analysis",
            "sector_analysis",
            "technical_analysis"
        ]
        self.analysis_engine = AnalysisEngine(settings)
    
    async def execute(self, task: Dict) -> Dict:
        """执行金融分析任务"""
        analysis_type = task.get('analysis_type')
        symbol = task.get('symbol')
        
        logger.info(f"执行金融分析: {analysis_type} for {symbol}")
        
        if analysis_type == 'market_analysis':
            return await self._market_analysis(symbol)
        elif analysis_type == 'financial_statement_analysis':
            return await self._financial_statement_analysis(symbol)
        elif analysis_type == 'sector_analysis':
            return await self._sector_analysis(task.get('sector'))
        elif analysis_type == 'technical_analysis':
            return await self._technical_analysis(symbol)
        else:
            return {"error": f"不支持的分析类型: {analysis_type}"}
    
    async def _market_analysis(self, symbol: str) -> Dict:
        """市场分析"""
        # 获取市场数据
        market_data = await self._get_market_data(symbol)
        
        # 分析市场数据
        analysis = {
            "symbol": symbol,
            "market_data": market_data,
            "analysis": "市场分析示例"
        }
        
        return analysis
    
    async def _financial_statement_analysis(self, symbol: str) -> Dict:
        """财务报表分析"""
        # 这里可以实现财务报表分析逻辑
        analysis = {
            "symbol": symbol,
            "analysis": "财务报表分析示例"
        }
        
        return analysis
    
    async def _sector_analysis(self, sector: str) -> Dict:
        """行业分析"""
        # 这里可以实现行业分析逻辑
        analysis = {
            "sector": sector,
            "analysis": "行业分析示例"
        }
        
        return analysis
    
    async def _technical_analysis(self, symbol: str) -> Dict:
        """技术分析"""
        # 获取历史数据
        historical_data = await self._get_historical_data(symbol, "90d", "1d")
        
        # 模拟创建DataFrame
        data = {
            'close': [100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110]
        }
        df = pd.DataFrame(data)
        
        # 使用分析引擎计算技术指标
        indicators = self.analysis_engine.calculate_technical_indicators(df)
        trend_analysis = self.analysis_engine.analyze_trend(df)
        
        # 分析历史数据
        analysis = {
            "symbol": symbol,
            "historical_data": historical_data,
            "technical_indicators": indicators,
            "trend_analysis": trend_analysis,
            "analysis": "技术分析示例"
        }
        
        return analysis
