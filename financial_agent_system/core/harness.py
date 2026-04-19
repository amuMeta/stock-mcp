from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Set

from financial_agent_system.core.agent_manager import AgentManager
from financial_agent_system.core.skill_manager import SkillManager
from financial_agent_system.core.sandbox_manager import SandboxManager
from financial_agent_system.core.mcp_client import McpClient
from financial_agent_system.core.monitoring import MonitoringSystem
from financial_agent_system.core.security import SecuritySystem
from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class SuperAgentHarness:
    """SuperAgent Harness - 金融智能体系统的核心协调器"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.agent_manager: Optional[AgentManager] = None
        self.skill_manager: Optional[SkillManager] = None
        self.sandbox_manager: Optional[SandboxManager] = None
        self.mcp_client: Optional[McpClient] = None
        self.monitoring: Optional[MonitoringSystem] = None
        self.security: Optional[SecuritySystem] = None
        self.is_initialized = False
    
    async def initialize(self):
        """初始化SuperAgent Harness"""
        logger.info("初始化SuperAgent Harness...")
        
        # 初始化监控系统
        self.monitoring = MonitoringSystem(self.settings)
        
        # 初始化安全系统
        self.security = SecuritySystem(self.settings)
        
        # 初始化MCP客户端，连接Stock MCP
        self.mcp_client = McpClient(self.settings)
        await self.mcp_client.connect()
        
        # 初始化沙箱管理器
        self.sandbox_manager = SandboxManager(self.settings)
        await self.sandbox_manager.initialize()
        
        # 初始化技能管理器
        self.skill_manager = SkillManager(self.settings)
        await self.skill_manager.initialize()
        
        # 初始化代理管理器
        self.agent_manager = AgentManager(self.settings, self)
        await self.agent_manager.initialize()
        
        self.is_initialized = True
        logger.info("SuperAgent Harness初始化完成")
    
    async def shutdown(self):
        """关闭SuperAgent Harness"""
        logger.info("关闭SuperAgent Harness...")
        
        if self.agent_manager:
            await self.agent_manager.shutdown()
        
        if self.skill_manager:
            await self.skill_manager.shutdown()
        
        if self.sandbox_manager:
            await self.sandbox_manager.shutdown()
        
        if self.mcp_client:
            await self.mcp_client.disconnect()
        
        self.is_initialized = False
        logger.info("SuperAgent Harness已关闭")
    
    async def execute_task(self, task: Dict) -> Dict:
        """执行任务"""
        if not self.is_initialized:
            raise RuntimeError("SuperAgent Harness未初始化")
        
        start_time = asyncio.get_event_loop().time()
        logger.info(f"执行任务: {task.get('task_type', 'unknown')}")
        
        try:
            # 任务分解和调度
            if self.agent_manager:
                result = await self.agent_manager.execute_task(task)
                
                # 记录任务执行时间
                if self.monitoring:
                    execution_time = asyncio.get_event_loop().time() - start_time
                    self.monitoring.record_metric("task_execution_time", execution_time)
                
                return result
            
            return {"error": "代理管理器未初始化"}
        except Exception as e:
            # 记录错误
            if self.monitoring:
                self.monitoring.log_error(e, {"task": task})
            raise
    
    async def get_available_agents(self) -> List[Dict]:
        """获取可用的代理列表"""
        if not self.agent_manager:
            return []
        return await self.agent_manager.get_available_agents()
    
    async def get_available_skills(self) -> List[Dict]:
        """获取可用的技能列表"""
        if not self.skill_manager:
            return []
        return await self.skill_manager.get_available_skills()
    
    async def get_system_health(self) -> Dict:
        """获取系统健康状态"""
        if self.monitoring:
            return self.monitoring.get_system_health()
        return {"status": "unknown"}
    
    def validate_request(self, headers: Dict) -> bool:
        """验证请求"""
        if self.security:
            return self.security.validate_request(headers)
        return True  # 默认允许所有请求，实际应用中应该更严格
