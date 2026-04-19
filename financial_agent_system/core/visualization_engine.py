from __future__ import annotations

import logging
import json
import pandas as pd
from typing import Dict, List, Optional

from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class VisualizationEngine:
    """可视化引擎 - 处理数据可视化和报告生成"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
    
    def generate_price_chart_data(self, data: pd.DataFrame) -> Dict:
        """生成价格图表数据"""
        if 'close' not in data.columns:
            return {"error": "数据中没有收盘价列"}
        
        # 提取价格数据
        chart_data = {
            "labels": data.index.tolist() if hasattr(data, 'index') else list(range(len(data))),
            "datasets": [
                {
                    "label": "收盘价",
                    "data": data['close'].tolist(),
                    "borderColor": "#3498db",
                    "backgroundColor": "rgba(52, 152, 219, 0.1)",
                    "borderWidth": 2,
                    "tension": 0.3
                }
            ]
        }
        
        return chart_data
    
    def generate_technical_indicators_chart(self, data: pd.DataFrame) -> Dict:
        """生成技术指标图表数据"""
        datasets = []
        
        # 添加价格数据
        if 'close' in data.columns:
            datasets.append({
                "label": "收盘价",
                "data": data['close'].tolist(),
                "borderColor": "#3498db",
                "backgroundColor": "rgba(52, 152, 219, 0.1)",
                "borderWidth": 2,
                "yAxisID": "y"
            })
        
        # 添加移动平均线
        if 'SMA_20' in data.columns:
            datasets.append({
                "label": "SMA 20",
                "data": data['SMA_20'].tolist(),
                "borderColor": "#27ae60",
                "backgroundColor": "transparent",
                "borderWidth": 2,
                "borderDash": [5, 5],
                "yAxisID": "y"
            })
        
        if 'SMA_50' in data.columns:
            datasets.append({
                "label": "SMA 50",
                "data": data['SMA_50'].tolist(),
                "borderColor": "#e74c3c",
                "backgroundColor": "transparent",
                "borderWidth": 2,
                "borderDash": [5, 5],
                "yAxisID": "y"
            })
        
        # 添加RSI
        if 'RSI' in data.columns:
            datasets.append({
                "label": "RSI",
                "data": data['RSI'].tolist(),
                "borderColor": "#f39c12",
                "backgroundColor": "rgba(243, 156, 18, 0.1)",
                "borderWidth": 2,
                "yAxisID": "y1"
            })
        
        chart_data = {
            "labels": data.index.tolist() if hasattr(data, 'index') else list(range(len(data))),
            "datasets": datasets,
            "options": {
                "scales": {
                    "y": {
                        "type": "linear",
                        "display": True,
                        "position": "left",
                        "title": {
                            "display": True,
                            "text": "价格"
                        }
                    },
                    "y1": {
                        "type": "linear",
                        "display": True,
                        "position": "right",
                        "title": {
                            "display": True,
                            "text": "RSI"
                        },
                        "min": 0,
                        "max": 100,
                        "grid": {
                            "drawOnChartArea": False
                        }
                    }
                }
            }
        }
        
        return chart_data
    
    def generate_portfolio_pie_chart(self, portfolio: Dict) -> Dict:
        """生成投资组合饼图数据"""
        labels = list(portfolio.keys())
        data = list(portfolio.values())
        
        chart_data = {
            "labels": labels,
            "datasets": [
                {
                    "data": data,
                    "backgroundColor": [
                        "#3498db",
                        "#27ae60",
                        "#f39c12",
                        "#e74c3c",
                        "#9b59b6"
                    ],
                    "borderWidth": 1
                }
            ]
        }
        
        return chart_data
    
    def generate_risk_heatmap_data(self, risk_data: Dict) -> Dict:
        """生成风险热力图数据"""
        # 简单的风险热力图数据结构
        heatmap_data = {
            "labels": list(risk_data.keys()),
            "datasets": [
                {
                    "label": "风险分数",
                    "data": list(risk_data.values()),
                    "backgroundColor": "rgba(231, 76, 60, 0.7)"
                }
            ]
        }
        
        return heatmap_data
    
    def generate_report(self, analysis_result: Dict) -> Dict:
        """生成分析报告"""
        report = {
            "title": "金融分析报告",
            "sections": [],
            "summary": ""
        }
        
        # 添加市场分析部分
        if 'market_data' in analysis_result:
            report["sections"].append({
                "title": "市场数据",
                "content": analysis_result['market_data']
            })
        
        # 添加技术分析部分
        if 'technical_indicators' in analysis_result:
            report["sections"].append({
                "title": "技术指标",
                "content": analysis_result['technical_indicators']
            })
        
        # 添加趋势分析部分
        if 'trend_analysis' in analysis_result:
            report["sections"].append({
                "title": "趋势分析",
                "content": analysis_result['trend_analysis']
            })
        
        # 添加风险评估部分
        if 'risk_assessment' in analysis_result:
            report["sections"].append({
                "title": "风险评估",
                "content": analysis_result['risk_assessment']
            })
        
        # 添加投资建议部分
        if 'investment_advice' in analysis_result:
            report["sections"].append({
                "title": "投资建议",
                "content": analysis_result['investment_advice']
            })
        
        # 生成摘要
        report["summary"] = "这是一份金融分析报告，包含市场数据、技术指标、趋势分析、风险评估和投资建议。"
        
        return report
    
    def export_report_to_json(self, report: Dict, filename: str) -> bool:
        """导出报告为JSON文件"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            logger.info(f"报告已导出到: {filename}")
            return True
        except Exception as e:
            logger.error(f"导出报告失败: {e}")
            return False
