from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Dict, List, Optional

from financial_agent_system.config.settings import Settings
from financial_agent_system.utils.logger import logger


class SkillManager:
    """技能管理器 - 管理Markdown技能系统"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.skills: Dict[str, Dict] = {}
        self.skills_dir = os.path.join(os.path.dirname(__file__), "..", "skills")
    
    async def initialize(self):
        """初始化技能管理器"""
        logger.info("初始化技能管理器...")
        
        # 加载内置技能
        await self._load_skills()
        
        logger.info(f"技能初始化完成，共加载 {len(self.skills)} 个技能")
    
    async def shutdown(self):
        """关闭技能管理器"""
        logger.info("关闭技能管理器...")
        self.skills.clear()
    
    async def _load_skills(self):
        """加载技能"""
        # 检查技能目录是否存在
        if not os.path.exists(self.skills_dir):
            logger.warning(f"技能目录不存在: {self.skills_dir}")
            return
        
        # 遍历技能目录
        for skill_file in os.listdir(self.skills_dir):
            if skill_file.endswith(".md"):
                skill_name = skill_file[:-3]
                skill_path = os.path.join(self.skills_dir, skill_file)
                
                try:
                    with open(skill_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # 解析技能内容
                    skill_info = self._parse_skill_content(content)
                    skill_info["name"] = skill_name
                    skill_info["content"] = content
                    skill_info["path"] = skill_path
                    
                    self.skills[skill_name] = skill_info
                    logger.info(f"加载技能: {skill_name}")
                except Exception as e:
                    logger.error(f"加载技能 {skill_name} 失败: {e}")
    
    def _parse_skill_content(self, content: str) -> Dict:
        """解析技能内容"""
        skill_info = {
            "description": "",
            "input_parameters": [],
            "execution_steps": [],
            "output_format": ""
        }
        
        # 解析描述
        description_match = re.search(r'## 描述\n(.*?)\n\n', content, re.DOTALL)
        if description_match:
            skill_info["description"] = description_match.group(1).strip()
        
        # 解析输入参数
        input_match = re.search(r'## 输入参数\n(.*?)\n\n', content, re.DOTALL)
        if input_match:
            params_text = input_match.group(1).strip()
            for line in params_text.split('\n'):
                if line.strip():
                    param = line.strip().split(': ')
                    if len(param) == 2:
                        skill_info["input_parameters"].append({
                            "name": param[0],
                            "description": param[1]
                        })
        
        # 解析执行步骤
        steps_match = re.search(r'## 执行步骤\n(.*?)\n\n', content, re.DOTALL)
        if steps_match:
            steps_text = steps_match.group(1).strip()
            for line in steps_text.split('\n'):
                if line.strip():
                    step = line.strip().lstrip('1234567890. ')
                    skill_info["execution_steps"].append(step)
        
        # 解析输出格式
        output_match = re.search(r'## 输出格式\n```json\n(.*?)\n```', content, re.DOTALL)
        if output_match:
            skill_info["output_format"] = output_match.group(1).strip()
        
        return skill_info
    
    async def get_skill(self, skill_name: str) -> Optional[Dict]:
        """获取技能"""
        return self.skills.get(skill_name)
    
    async def execute_skill(self, skill_name: str, context: Dict) -> Dict:
        """执行技能"""
        skill = self.skills.get(skill_name)
        if not skill:
            return {"error": f"技能不存在: {skill_name}"}
        
        logger.info(f"执行技能: {skill_name}")
        
        # 模拟技能执行
        result = {
            "skill_name": skill_name,
            "status": "success",
            "message": f"技能 {skill_name} 执行成功",
            "input": context,
            "output": {
                "skill_description": skill.get("description", ""),
                "execution_steps": skill.get("execution_steps", []),
                "result": "技能执行结果"
            }
        }
        
        return result
    
    async def get_available_skills(self) -> List[Dict]:
        """获取可用的技能列表"""
        return [
            {
                "name": skill["name"],
                "description": skill.get("description", f"技能: {skill['name']}")
            }
            for skill in self.skills.values()
        ]
