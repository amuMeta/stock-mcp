from __future__ import annotations

import logging
from typing import Dict, List, Optional, Type

from financial_agent_system.agents.base_agent import BaseAgent
from financial_agent_system.utils.logger import logger


class AgentRegistry:
    """代理注册表 - 管理代理的注册和发现"""
    
    def __init__(self):
        self.agent_classes: Dict[str, Type[BaseAgent]] = {}
        self.agent_instances: Dict[str, BaseAgent] = {}
    
    def register_agent(self, agent_class: Type[BaseAgent]):
        """注册代理类"""
        agent_name = agent_class.__name__.replace('Agent', '').lower()
        self.agent_classes[agent_name] = agent_class
        logger.info(f"注册代理类: {agent_name}")
    
    def get_agent_class(self, agent_name: str) -> Optional[Type[BaseAgent]]:
        """获取代理类"""
        return self.agent_classes.get(agent_name)
    
    def get_available_agent_classes(self) -> List[str]:
        """获取可用的代理类列表"""
        return list(self.agent_classes.keys())
    
    def add_agent_instance(self, agent: BaseAgent):
        """添加代理实例"""
        self.agent_instances[agent.name] = agent
        logger.info(f"添加代理实例: {agent.name}")
    
    def get_agent_instance(self, agent_name: str) -> Optional[BaseAgent]:
        """获取代理实例"""
        return self.agent_instances.get(agent_name)
    
    def get_available_agent_instances(self) -> List[BaseAgent]:
        """获取可用的代理实例列表"""
        return list(self.agent_instances.values())
    
    def remove_agent_instance(self, agent_name: str):
        """移除代理实例"""
        if agent_name in self.agent_instances:
            del self.agent_instances[agent_name]
            logger.info(f"移除代理实例: {agent_name}")


# 全局代理注册表
agent_registry = AgentRegistry()
