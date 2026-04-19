from __future__ import annotations

import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Optional

from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class AnalysisEngine:
    """智能分析引擎 - 处理金融数据分析和决策支持"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
    
    def calculate_technical_indicators(self, data: pd.DataFrame) -> Dict:
        """计算技术指标"""
        indicators = {}
        
        # 计算移动平均线
        if 'close' in data.columns:
            # 简单移动平均线
            data['SMA_20'] = data['close'].rolling(window=20).mean()
            data['SMA_50'] = data['close'].rolling(window=50).mean()
            
            # 指数移动平均线
            data['EMA_20'] = data['close'].ewm(span=20, adjust=False).mean()
            
            # RSI指标
            delta = data['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            data['RSI'] = 100 - (100 / (1 + rs))
            
            # MACD指标
            exp1 = data['close'].ewm(span=12, adjust=False).mean()
            exp2 = data['close'].ewm(span=26, adjust=False).mean()
            data['MACD'] = exp1 - exp2
            data['MACD_signal'] = data['MACD'].ewm(span=9, adjust=False).mean()
            
            indicators['SMA_20'] = data['SMA_20'].iloc[-1] if not data['SMA_20'].isnull().iloc[-1] else None
            indicators['SMA_50'] = data['SMA_50'].iloc[-1] if not data['SMA_50'].isnull().iloc[-1] else None
            indicators['EMA_20'] = data['EMA_20'].iloc[-1] if not data['EMA_20'].isnull().iloc[-1] else None
            indicators['RSI'] = data['RSI'].iloc[-1] if not data['RSI'].isnull().iloc[-1] else None
            indicators['MACD'] = data['MACD'].iloc[-1] if not data['MACD'].isnull().iloc[-1] else None
            indicators['MACD_signal'] = data['MACD_signal'].iloc[-1] if not data['MACD_signal'].isnull().iloc[-1] else None
        
        return indicators
    
    def analyze_trend(self, data: pd.DataFrame) -> Dict:
        """分析趋势"""
        trend_analysis = {}
        
        if 'close' in data.columns and len(data) > 50:
            # 计算价格趋势
            price_change = (data['close'].iloc[-1] - data['close'].iloc[0]) / data['close'].iloc[0] * 100
            trend_analysis['price_change'] = price_change
            
            # 计算移动平均线趋势
            if 'SMA_20' in data.columns and 'SMA_50' in data.columns:
                if data['SMA_20'].iloc[-1] > data['SMA_50'].iloc[-1]:
                    trend_analysis['ma_trend'] = 'bullish'
                else:
                    trend_analysis['ma_trend'] = 'bearish'
            
            # 计算RSI趋势
            if 'RSI' in data.columns:
                rsi = data['RSI'].iloc[-1]
                if rsi > 70:
                    trend_analysis['rsi_signal'] = 'overbought'
                elif rsi < 30:
                    trend_analysis['rsi_signal'] = 'oversold'
                else:
                    trend_analysis['rsi_signal'] = 'neutral'
        
        return trend_analysis
    
    def portfolio_optimization(self, returns: pd.DataFrame, risk_free_rate: float = 0.02) -> Dict:
        """投资组合优化"""
        # 计算预期收益率
        expected_returns = returns.mean() * 252  # 年化收益率
        
        # 计算协方差矩阵
        cov_matrix = returns.cov() * 252  # 年化协方差
        
        # 计算夏普比率
        def calculate_sharpe_ratio(weights):
            portfolio_return = np.sum(expected_returns * weights)
            portfolio_volatility = np.sqrt(np.dot(weights.T, np.dot(cov_matrix, weights)))
            sharpe_ratio = (portfolio_return - risk_free_rate) / portfolio_volatility
            return sharpe_ratio
        
        # 简单的等权重配置
        n_assets = len(expected_returns)
        equal_weights = np.array([1 / n_assets] * n_assets)
        equal_sharpe = calculate_sharpe_ratio(equal_weights)
        
        optimization_result = {
            "expected_returns": expected_returns.to_dict(),
            "covariance_matrix": cov_matrix.to_dict(),
            "equal_weights": equal_weights.tolist(),
            "equal_sharpe_ratio": equal_sharpe
        }
        
        return optimization_result
    
    def risk_assessment(self, portfolio: Dict) -> Dict:
        """风险评估"""
        risk_assessment = {
            "risk_score": 0.0,
            "risk_level": "",
            "recommendations": []
        }
        
        # 简单的风险评估逻辑
        total_weight = sum(portfolio.values())
        if total_weight != 1.0:
            risk_assessment["recommendations"].append("投资组合权重总和应为100%")
        
        # 计算风险分数
        for asset, weight in portfolio.items():
            if asset.lower() in ['stock', 'equity']:
                risk_assessment["risk_score"] += weight * 0.8
            elif asset.lower() in ['bond', 'fixed_income']:
                risk_assessment["risk_score"] += weight * 0.3
            elif asset.lower() in ['cash', 'money_market']:
                risk_assessment["risk_score"] += weight * 0.1
            elif asset.lower() in ['crypto', 'cryptocurrency']:
                risk_assessment["risk_score"] += weight * 1.0
        
        # 确定风险等级
        if risk_assessment["risk_score"] < 0.3:
            risk_assessment["risk_level"] = "低风险"
        elif risk_assessment["risk_score"] < 0.6:
            risk_assessment["risk_level"] = "中等风险"
        else:
            risk_assessment["risk_level"] = "高风险"
            risk_assessment["recommendations"].append("考虑增加低风险资产的配置")
        
        return risk_assessment
    
    def generate_investment_advice(self, risk_profile: str, goals: List[str]) -> Dict:
        """生成投资建议"""
        advice = {
            "risk_profile": risk_profile,
            "goals": goals,
            "asset_allocation": {},
            "recommendations": []
        }
        
        # 根据风险偏好生成资产配置建议
        if risk_profile == "保守":
            advice["asset_allocation"] = {
                "cash": 0.2,
                "bonds": 0.6,
                "stocks": 0.2
            }
            advice["recommendations"].append("优先考虑资本保全，适当配置高评级债券")
        elif risk_profile == "中等":
            advice["asset_allocation"] = {
                "cash": 0.1,
                "bonds": 0.4,
                "stocks": 0.5
            }
            advice["recommendations"].append("均衡配置，适当增加股票比例以提高收益")
        else:  # 激进
            advice["asset_allocation"] = {
                "cash": 0.05,
                "bonds": 0.25,
                "stocks": 0.7
            }
            advice["recommendations"].append("追求高收益，可考虑增加新兴市场和成长股的配置")
        
        # 根据投资目标添加建议
        if "退休规划" in goals:
            advice["recommendations"].append("建议定期投资，利用复利效应")
        if "财富增长" in goals:
            advice["recommendations"].append("考虑长期持有成长型资产")
        if "风险管理" in goals:
            advice["recommendations"].append("定期再平衡投资组合，控制风险")
        
        return advice
