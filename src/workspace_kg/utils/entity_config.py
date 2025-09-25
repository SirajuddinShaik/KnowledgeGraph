#!/usr/bin/env python3
"""
Centralized Entity Configuration for Knowledge Graph
Defines entity schemas, LLM prompt templates, field mappings, and merge strategies.
"""

from typing import Dict, List, Any, Optional
from enum import Enum
import yaml
import os


class MergeStrategy(Enum):
    """Strategies for merging different field types"""
    PRESERVE_EXISTING = "preserve_existing"     # Never overwrite existing values
    APPEND_UNIQUE = "append_unique"             # Add to array if unique
    REPLACE_IF_BETTER = "replace_if_better"     # Replace if new value is better
    REPLACE_ALWAYS = "replace_always"           # Always use new value
    AGENT_ONLY = "agent_only"                   # Only set by cleanup agents

class EntityConfig:
    """Centralized configuration for entities, fields, and merge strategies"""
    
    def __init__(self, config_file: str = "entity_config.yaml"):
        self.config_file = config_file
        self.entity_schemas: Dict[str, Dict[str, Any]] = {}
        self.field_mappings: Dict[str, Any] = {}
        self.systematic_merge: Dict[str, Any] = {}
        self.load_config()
    
    def load_config(self):
        """Load configuration from YAML file"""
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                config_data = yaml.safe_load(f)
                self.entity_schemas = config_data.get("entity_schemas", {})
                self.field_mappings = config_data.get("field_mappings", {})
                self.systematic_merge = config_data.get("systematic_merge", {})
        else:
            raise FileNotFoundError(f"Entity config file not found: {self.config_file}")
    
    def get_entity_types(self) -> List[str]:
        """Get list of all entity types"""
        return list(self.entity_schemas.keys())
    
    def get_llm_fields(self, entity_type: str) -> List[str]:
        """Get LLM extractable fields for an entity type"""
        if entity_type in self.entity_schemas:
            return self.entity_schemas[entity_type].get("llm_fields", [])
        return []
    
    def get_db_fields(self, entity_type: str) -> Dict[str, Any]:
        """Get database mappings for an entity type"""
        if entity_type in self.entity_schemas:
            return self.entity_schemas[entity_type].get("mappings", {})
        return {}
    
    def get_all_fields(self, entity_type: str) -> Dict[str, Any]:
        """Get all fields (LLM + DB) for an entity type"""
        mappings = self.get_db_fields(entity_type)
        return mappings
    
    def get_target_field(self, entity_type: str, llm_field: str) -> str:
        """Get the target database field for an LLM-generated field"""
        mappings = self.get_db_fields(entity_type)
        
        # Check if mappings exist and are not None
        if mappings:
            # Look for a mapping where the "mapping" value matches our llm_field
            for db_field, config in mappings.items():
                if config and config.get("mapping") == llm_field:
                    return db_field
        
        # Default to the same field name
        return llm_field
    
    def get_merge_strategy(self, entity_type: str, field_name: str) -> str:
        """Get merge strategy for a field"""
        mappings = self.get_db_fields(entity_type)
        if mappings and field_name in mappings:
            return mappings[field_name].get("merge_strategy", "replace_if_better")
        
        return "replace_if_better"
    
    def get_field_priority(self, entity_type: str, field_name: str) -> int:
        """Get priority for a field"""
        mappings = self.get_db_fields(entity_type)
        if mappings and field_name in mappings:
            return mappings[field_name].get("priority", 3)
        
        return 3
    
    def should_merge_field(self, entity_type: str, field_name: str, is_from_agent: bool = False) -> bool:
        """Determine if a field should be merged"""
        strategy = self.get_merge_strategy(entity_type, field_name)
        
        if strategy == "agent_only" and not is_from_agent:
            return False
        
        return True
    
    def get_array_fields(self, entity_type: str = None) -> List[str]:
        """Get list of array fields"""
        if entity_type:
            # Get entity-specific array fields
            array_fields = []
            mappings = self.get_db_fields(entity_type)
            if mappings:  # Check if mappings is not None or empty
                for field_name, config in mappings.items():
                    if config and config.get("type", "").endswith("[]"):
                        array_fields.append(field_name)
            return array_fields
        else:
            # Get global array fields
            return self.field_mappings.get("array_fields", [])
    
    def get_critical_fields(self) -> List[str]:
        """Get list of critical fields"""
        return self.field_mappings.get("critical_fields", [])
    
    def get_timestamp_fields(self) -> List[str]:
        """Get list of timestamp fields"""
        return self.field_mappings.get("timestamp_fields", [])
    
    def get_systematic_merge_rules(self, entity_type: str) -> List[Dict[str, Any]]:
        """Get systematic merge matching rules for an entity type"""
        if self.systematic_merge and "matching_rules" in self.systematic_merge:
            matching_rules = self.systematic_merge["matching_rules"]
            return matching_rules.get(entity_type, [])
        return []
    
    def get_systematic_merge_fields(self) -> Dict[str, List[str]]:
        """Get fields to merge in systematic merge"""
        if self.systematic_merge and "merge_fields" in self.systematic_merge:
            return self.systematic_merge["merge_fields"]
        return {
            "string_fields": ["name", "email", "worksAt", "industry", "domain", "url"],
            "array_fields": ["rawDescriptions", "sources", "aliases", "role", "permissions"]
        }
    
    def transform_value(self, entity_type: str, llm_field: str, value: Any, target_field: str = None) -> Any:
        """Transform a value based on field configuration"""
        # For description field, always convert to array
        if llm_field == "description":
            if isinstance(value, list):
                return value
            else:
                return [value] if value else []
        
        return value
    
    def get_entity_array_fields(self, entity_type: str) -> List[str]:
        """Get array fields for a specific entity type"""
        return self.get_array_fields(entity_type)
    
    def get_prompt_fields(self, entity_type: str) -> List[str]:
        """Get fields that should be included in LLM prompts"""
        return self.get_llm_fields(entity_type)
    
    def generate_prompt_template(self, entity_type: str) -> str:
        """Generate prompt template for an entity type based on configuration"""
        llm_fields = self.get_llm_fields(entity_type)
        
        # Build simple template
        template_parts = [f"**{entity_type}**"]
        template_parts.append(f"[{', '.join(llm_fields)}]")
        
        return ": ".join(template_parts)

# Global instance
entity_config = EntityConfig()

# Backward compatibility aliases
merge_config = entity_config