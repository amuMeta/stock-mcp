from __future__ import annotations

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """系统设置"""

    # 服务配置
    host: str = Field("0.0.0.0", description="服务主机")
    port: int = Field(8000, description="服务端口")
    debug: bool = Field(False, description="调试模式")

    # Stock MCP配置
    stock_mcp_host: str = Field("127.0.0.1", description="Stock MCP主机")
    stock_mcp_port: int = Field(9898, description="Stock MCP端口")

    # 沙箱配置
    sandbox_enabled: bool = Field(True, description="是否启用沙箱")
    sandbox_image: str = Field("python:3.11-slim", description="沙箱镜像")

    # 代理配置
    max_agents: int = Field(10, description="最大代理数量")
    agent_timeout: int = Field(30, description="代理超时时间（秒）")

    # 技能配置
    skills_dir: str = Field("skills", description="技能目录")

    # 日志配置
    log_level: str = Field("INFO", description="日志级别")
    log_file: str = Field("financial_agent.log", description="日志文件")

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"
