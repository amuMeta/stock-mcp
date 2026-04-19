from __future__ import annotations

import logging
import time
from typing import Dict, List, Optional

from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class MonitoringSystem:
    """监控系统 - 处理系统监控和日志"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.metrics: Dict[str, List] = {}
        self.start_time = time.time()
    
    def record_metric(self, name: str, value: float):
        """记录指标"""
        if name not in self.metrics:
            self.metrics[name] = []
        self.metrics[name].append({
            "timestamp": time.time(),
            "value": value
        })
        
        # 限制每个指标的记录数量
        if len(self.metrics[name]) > 1000:
            self.metrics[name] = self.metrics[name][-1000:]
    
    def get_metrics(self, name: Optional[str] = None) -> Dict:
        """获取指标"""
        if name:
            return {name: self.metrics.get(name, [])}
        return self.metrics
    
    def get_system_health(self) -> Dict:
        """获取系统健康状态"""
        uptime = time.time() - self.start_time
        
        health = {
            "status": "healthy",
            "uptime": uptime,
            "metrics": {
                "requests": len(self.metrics.get("request_count", [])),
                "errors": len(self.metrics.get("error_count", []))
            }
        }
        
        # 检查错误率
        total_requests = len(self.metrics.get("request_count", []))
        total_errors = len(self.metrics.get("error_count", []))
        
        if total_requests > 0:
            error_rate = total_errors / total_requests
            if error_rate > 0.1:
                health["status"] = "degraded"
        
        return health
    
    def log_request(self, method: str, path: str, status_code: int, response_time: float):
        """记录请求"""
        self.record_metric("request_count", 1)
        self.record_metric("response_time", response_time)
        
        if status_code >= 500:
            self.record_metric("error_count", 1)
            logger.error(f"Request failed: {method} {path} {status_code} {response_time:.3f}s")
        else:
            logger.info(f"Request: {method} {path} {status_code} {response_time:.3f}s")
    
    def log_error(self, error: Exception, context: Dict):
        """记录错误"""
        self.record_metric("error_count", 1)
        logger.error(f"Error: {str(error)}", extra=context, exc_info=True)
    
    def get_uptime(self) -> float:
        """获取系统运行时间"""
        return time.time() - self.start_time
