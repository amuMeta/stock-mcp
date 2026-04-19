from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import APIRouter, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import Dict, List
import os

from financial_agent_system.api.schemas import (
    TaskRequest,
    TaskResponse,
    AgentInfo,
    SkillInfo,
    HealthResponse,
)

# 创建API路由器
router = APIRouter()


# Lazy import to avoid circular dependency
def _get_harness():
    from financial_agent_system.main import harness

    return harness


@router.post("/task", response_model=TaskResponse)
async def execute_task(task: TaskRequest, agent_harness=Depends(_get_harness)) -> Dict:
    """执行任务"""
    try:
        result = await agent_harness.execute_task(task.model_dump())
        return TaskResponse(result=result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/agents", response_model=List[AgentInfo])
async def get_agents(agent_harness=Depends(_get_harness)) -> List[Dict]:
    """获取可用的代理列表"""
    try:
        agents = await agent_harness.get_available_agents()
        return agents
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/skills", response_model=List[SkillInfo])
async def get_skills(agent_harness=Depends(_get_harness)) -> List[Dict]:
    """获取可用的技能列表"""
    try:
        skills = await agent_harness.get_available_skills()
        return skills
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=HealthResponse)
async def health_check() -> Dict:
    """健康检查"""
    h = _get_harness()
    status = "healthy"
    if not h or not h.is_initialized:
        status = "unhealthy"

    # 获取系统健康状态
    health_details = {}
    if h and h.is_initialized:
        health_details = await h.get_system_health()

    return {
        "status": status,
        "service": "金融智能体系统",
        "version": "1.0.0",
        "details": health_details,
    }


@router.get("/metrics")
async def get_metrics() -> Dict:
    """获取系统指标"""
    h = _get_harness()
    if not h or not h.is_initialized:
        return {"error": "服务未初始化"}

    # 这里可以添加更多指标
    metrics = {
        "uptime": h.monitoring.get_uptime() if hasattr(h, "monitoring") else 0,
        "status": "healthy" if h.is_initialized else "unhealthy",
    }

    return metrics


def setup_routes(app):
    """设置API路由"""
    # 注册API路由
    app.include_router(router, prefix="/api/v1")

    # 注册静态文件服务
    static_dir = os.path.join(os.path.dirname(__file__), "static")
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    # 注册根路径
    @app.get("/")
    async def root():
        return FileResponse(os.path.join(static_dir, "index.html"))
