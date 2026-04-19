from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Dict, List, Optional


class TaskRequest(BaseModel):
    """任务请求"""
    task_type: str = Field(..., description="任务类型")
    analysis_type: Optional[str] = Field(None, description="分析类型")
    risk_type: Optional[str] = Field(None, description="风险类型")
    advice_type: Optional[str] = Field(None, description="建议类型")
    symbol: Optional[str] = Field(None, description="股票代码")
    sector: Optional[str] = Field(None, description="行业")
    portfolio: Optional[Dict] = Field(None, description="投资组合")
    risk_profile: Optional[str] = Field(None, description="风险偏好")
    goals: Optional[List[str]] = Field(None, description="投资目标")
    strategy_type: Optional[str] = Field(None, description="策略类型")
    entity: Optional[str] = Field(None, description="实体")


class TaskResponse(BaseModel):
    """任务响应"""
    result: Dict = Field(..., description="任务执行结果")


class AgentInfo(BaseModel):
    """代理信息"""
    name: str = Field(..., description="代理名称")
    description: str = Field(..., description="代理描述")
    capabilities: List[str] = Field(..., description="代理能力")


class SkillInfo(BaseModel):
    """技能信息"""
    name: str = Field(..., description="技能名称")
    description: str = Field(..., description="技能描述")


class HealthResponse(BaseModel):
    """健康检查响应"""
    status: str = Field(..., description="服务状态")
    service: str = Field(..., description="服务名称")
    version: str = Field(..., description="服务版本")
