from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from financial_agent_system.core.harness import SuperAgentHarness
from financial_agent_system.api.routes import setup_routes
from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import setup_logger

# 配置日志
setup_logger()
logger = logging.getLogger(__name__)

# 加载设置
settings = Settings()

# 创建FastAPI应用
app = FastAPI(
    title="金融智能体系统",
    description="基于DeerFlow 2.0架构的金融智能体系统，连接Stock MCP提供金融服务",
    version="1.0.0",
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应该设置具体的域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局SuperAgentHarness实例
harness: Optional[SuperAgentHarness] = None


async def startup_event():
    """应用启动事件"""
    global harness
    logger.info("🚀 启动金融智能体系统...")

    # 初始化SuperAgent Harness
    harness = SuperAgentHarness(settings)
    await harness.initialize()

    logger.info("✅ 金融智能体系统启动完成")


async def shutdown_event():
    """应用关闭事件"""
    global harness
    if harness:
        await harness.shutdown()
    logger.info("🛑 金融智能体系统已关闭")


# 注册事件处理
app.add_event_handler("startup", startup_event)
app.add_event_handler("shutdown", shutdown_event)

# 设置API路由
setup_routes(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app", host=settings.host, port=settings.port, reload=settings.debug
    )
