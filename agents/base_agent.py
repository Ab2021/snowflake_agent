from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

class BaseAgent(ABC):
    """Base class for all GenBI agents"""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.logger = logging.getLogger(f"genbi.agents.{name}")
        self.tools = {}
        self.context = {}
        self.created_at = datetime.now()
    
    @abstractmethod
    def execute(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the agent's primary task"""
        pass
    
    def register_tool(self, tool_name: str, tool_instance):
        """Register a tool for this agent to use"""
        self.tools[tool_name] = tool_instance
        self.logger.info(f"Registered tool: {tool_name}")
    
    def use_tool(self, tool_name: str, **kwargs) -> Any:
        """Use a registered tool"""
        if tool_name not in self.tools:
            raise ValueError(f"Tool '{tool_name}' not registered for agent '{self.name}'")
        
        tool = self.tools[tool_name]
        self.logger.info(f"Using tool: {tool_name}")
        
        try:
            result = tool.execute(**kwargs)
            self.logger.info(f"Tool '{tool_name}' executed successfully")
            return result
        except Exception as e:
            self.logger.error(f"Tool '{tool_name}' failed: {str(e)}")
            raise
    
    def update_context(self, key: str, value: Any):
        """Update agent context"""
        self.context[key] = value
        self.logger.debug(f"Updated context: {key}")
    
    def get_context(self, key: str, default: Any = None) -> Any:
        """Get value from agent context"""
        return self.context.get(key, default)
    
    def validate_input(self, task: Dict[str, Any]) -> bool:
        """Validate input task format"""
        required_fields = self.get_required_fields()
        for field in required_fields:
            if field not in task:
                self.logger.error(f"Missing required field: {field}")
                return False
        return True
    
    def get_required_fields(self) -> List[str]:
        """Return list of required fields for this agent"""
        return []
    
    def get_capabilities(self) -> List[str]:
        """Return list of agent capabilities"""
        return []
    
    def get_status(self) -> Dict[str, Any]:
        """Return agent status information"""
        return {
            'name': self.name,
            'status': 'active',
            'tools_count': len(self.tools),
            'context_keys': list(self.context.keys()),
            'created_at': self.created_at.isoformat(),
            'capabilities': self.get_capabilities()
        }