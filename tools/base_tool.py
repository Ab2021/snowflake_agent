from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
from datetime import datetime

class BaseTool(ABC):
    """Base class for all GenBI tools"""
    
    def __init__(self, name: str, config: Dict[str, Any] = None):
        self.name = name
        self.config = config or {}
        self.logger = logging.getLogger(f"genbi.tools.{name}")
        self.usage_count = 0
        self.created_at = datetime.now()
        self.last_used = None
    
    @abstractmethod
    def execute(self, **kwargs) -> Any:
        """Execute the tool's primary function"""
        pass
    
    def _pre_execute(self, **kwargs):
        """Pre-execution hook"""
        self.usage_count += 1
        self.last_used = datetime.now()
        self.logger.info(f"Executing tool: {self.name}")
    
    def _post_execute(self, result: Any, **kwargs):
        """Post-execution hook"""
        self.logger.info(f"Tool {self.name} completed successfully")
        return result
    
    def _handle_error(self, error: Exception, **kwargs):
        """Error handling hook"""
        self.logger.error(f"Tool {self.name} failed: {str(error)}")
        raise error
    
    def validate_inputs(self, **kwargs) -> bool:
        """Validate input parameters"""
        required_params = self.get_required_parameters()
        for param in required_params:
            if param not in kwargs or kwargs[param] is None:
                self.logger.error(f"Missing required parameter: {param}")
                return False
        return True
    
    def get_required_parameters(self) -> list:
        """Return list of required parameters"""
        return []
    
    def get_optional_parameters(self) -> list:
        """Return list of optional parameters"""
        return []
    
    def get_description(self) -> str:
        """Return tool description"""
        return f"Tool: {self.name}"
    
    def get_status(self) -> Dict[str, Any]:
        """Return tool status information"""
        return {
            'name': self.name,
            'usage_count': self.usage_count,
            'created_at': self.created_at.isoformat(),
            'last_used': self.last_used.isoformat() if self.last_used else None,
            'required_parameters': self.get_required_parameters(),
            'optional_parameters': self.get_optional_parameters(),
            'description': self.get_description()
        }
    
    def reset_stats(self):
        """Reset usage statistics"""
        self.usage_count = 0
        self.last_used = None
        self.logger.info(f"Reset statistics for tool: {self.name}")