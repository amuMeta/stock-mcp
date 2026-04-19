from __future__ import annotations

import asyncio
import logging
from typing import Dict, List, Optional, Type

from financial_agent_system.agents.base_agent import BaseAgent
from financial_agent_system.agents.financial_analyzer import FinancialAnalyzerAgent
from financial_agent_system.agents.risk_manager import RiskManagerAgent
from financial_agent_system.agents.investment_advisor import InvestmentAdvisorAgent
from financial_agent_system.core.agent_registry import agent_registry
from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class AgentManager:
    """代理管理器 - 管理和调度金融垂类agent"""
    
    def __init__(self, settings: Settings, harness):
        self.settings = settings
        self.harness = harness
        self.agents: Dict[str, BaseAgent] = {}
    
    async def initialize(self):
        """初始化代理管理器"""
        logger.info("初始化代理管理器...")
        
        # 注册内置代理类
        agent_registry.register_agent(FinancialAnalyzerAgent)
        agent_registry.register_agent(RiskManagerAgent)
        agent_registry.register_agent(InvestmentAdvisorAgent)
        
        # 初始化内置代理
        agents = [
            FinancialAnalyzerAgent(self.settings, self.harness),
            RiskManagerAgent(self.settings, self.harness),
            InvestmentAdvisorAgent(self.settings, self.harness)
        ]
        
        for agent in agents:
            await agent.initialize()
            self.agents[agent.name] = agent
            agent_registry.add_agent_instance(agent)
        
        logger.info(f"代理初始化完成，共加载 {len(self.agents)} 个代理")
    
    async def shutdown(self):
        """关闭代理管理器"""
        logger.info("关闭代理管理器...")
        
        for agent in self.agents.values():
            await agent.shutdown()
            agent_registry.remove_agent_instance(agent.name)
        
        self.agents.clear()
    
    async def execute_task(self, task: Dict) -> Dict:
        """执行任务"""
        task_type = task.get('task_type')
        
        # 根据任务类型选择合适的代理
        if task_type == 'financial_analysis':
            agent = self.agents.get('financial_analyzer')
        elif task_type == 'risk_management':
            agent = self.agents.get('risk_manager')
        elif task_type == 'investment_advice':
            agent = self.agents.get('investment_advisor')
        else:
            # 默认使用金融分析代理
            agent = self.agents.get('financial_analyzer')
        
        if agent:
            result = await agent.execute(task)
            return result
        else:
            return {"error": f"没有可用的代理处理任务类型: {task_type}"}
    
    async def execute_tasks_in_parallel(self, tasks: List[Dict]) -> List[Dict]:
        """并行执行多个任务"""
        logger.info(f"并行执行 {len(tasks)} 个任务")
        
        # 创建任务列表
        coroutines = []
        for task in tasks:
            coroutines.append(self.execute_task(task))
        
        # 并行执行
        results = await asyncio.gather(*coroutines)
        return results
    
    async def get_available_agents(self) -> List[Dict]:
        """获取可用的代理列表"""
        return [
            {
                "name": agent.name,
                "description": agent.description,
                "capabilities": agent.capabilities
            }
            for agent in self.agents.values()
        ]
    
    async def register_external_agent(self, agent_class: Type[BaseAgent]):
        """注册外部代理"""
        agent_registry.register_agent(agent_class)
        
        # 创建代理实例
        agent = agent_class(self.settings, self.harness)
        await agent.initialize()
        self.agents[agent.name] = agent
        agent_registry.add_agent_instance(agent)
        
        logger.info(f"注册外部代理: {agent.name}")
