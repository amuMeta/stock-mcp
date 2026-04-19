from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional

import docker
from docker.models.containers import Container

from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class SandboxManager:
    """沙箱管理器 - 管理Docker沙箱环境"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: Optional[docker.DockerClient] = None
        self.containers: Dict[str, Container] = {}
    
    async def initialize(self):
        """初始化沙箱管理器"""
        logger.info("初始化沙箱管理器...")
        
        try:
            # 连接Docker客户端
            self.client = docker.from_env()
            # 测试连接
            self.client.ping()
            logger.info("Docker客户端连接成功")
        except Exception as e:
            logger.warning(f"Docker客户端连接失败: {e}")
            logger.warning("沙箱功能将不可用")
    
    async def shutdown(self):
        """关闭沙箱管理器"""
        logger.info("关闭沙箱管理器...")
        
        # 清理所有容器
        for container in self.containers.values():
            try:
                container.stop()
                container.remove()
            except Exception as e:
                logger.error(f"清理容器失败: {e}")
        
        if self.client:
            self.client.close()
        
        self.containers.clear()
    
    async def create_sandbox(self, sandbox_id: str) -> Optional[Container]:
        """创建沙箱容器"""
        if not self.client:
            logger.warning("Docker客户端未初始化，无法创建沙箱")
            return None
        
        try:
            # 创建容器
            container = self.client.containers.run(
                "python:3.11-slim",
                "tail -f /dev/null",
                detach=True,
                name=f"financial-agent-sandbox-{sandbox_id}",
                network_mode="bridge"
            )
            
            self.containers[sandbox_id] = container
            logger.info(f"创建沙箱容器: {sandbox_id}")
            return container
        except Exception as e:
            logger.error(f"创建沙箱容器失败: {e}")
            return None
    
    async def execute_in_sandbox(self, sandbox_id: str, command: str) -> Dict:
        """在沙箱中执行命令"""
        container = self.containers.get(sandbox_id)
        if not container:
            return {"error": f"沙箱不存在: {sandbox_id}"}
        
        try:
            exit_code, output = container.exec_run(command)
            return {
                "exit_code": exit_code,
                "output": output.decode('utf-8')
            }
        except Exception as e:
            logger.error(f"在沙箱中执行命令失败: {e}")
            return {"error": str(e)}
    
    async def destroy_sandbox(self, sandbox_id: str):
        """销毁沙箱容器"""
        container = self.containers.get(sandbox_id)
        if container:
            try:
                container.stop()
                container.remove()
                del self.containers[sandbox_id]
                logger.info(f"销毁沙箱容器: {sandbox_id}")
            except Exception as e:
                logger.error(f"销毁沙箱容器失败: {e}")
