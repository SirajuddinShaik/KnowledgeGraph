#!/usr/bin/env python3
"""
Merge Configuration for Knowledge Graph
Defines mapping between LLM-generated fields and database schema,
priority levels, and merge strategies.
"""

from typing import Dict, List, Any, Optional
from enum import Enum
import yaml
import os

class MergePriority(Enum):
    """Priority levels for field merging"""
    CRITICAL = 1    # Never overwrite (e.g., entity_id, primary keys)
    HIGH = 2        # Prefer existing unless new is significantly better
    MEDIUM = 3      # Merge arrays, prefer non-null values
    LOW = 4         # Always prefer new values
    GENERATED = 5   # Only set by cleanup agents, never from LLM

class MergeStrategy(Enum):
    """Strategies for merging different field types"""
    PRESERVE_EXISTING = "preserve_existing"     # Never overwrite existing values
    APPEND_UNIQUE = "append_unique"             # Add to array if unique
    REPLACE_IF_BETTER = "replace_if_better"     # Replace if new value is better
    REPLACE_ALWAYS = "replace_always"           # Always use new value
    AGENT_ONLY = "agent_only"                   # Only set by cleanup agents

class MergeConfig:
    """Configuration for field mapping and merge strategies"""
    
    def __init__(self, config_file: str = "merge_config.yaml"):
        self.config_file = config_file
        self.field_mappings: Dict[str, Dict[str, Any]] = {}
        self.entity_configs: Dict[str, Dict[str, Any]] = {}
        self.load_config()
    
    def load_config(self):
        """Load configuration from YAML file or create default"""
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                config_data = yaml.safe_load(f)
                self.field_mappings = config_data.get('field_mappings', {})
                self.entity_configs = config_data.get('entity_configs', {})
        else:
            self.create_default_config()
            self.save_config()
    
    def create_default_config(self):
        """Create default configuration for all entity types"""
        
        # Common field mappings that apply to all entities
        common_mappings = {
            # LLM field -> DB field mapping with strategy
            "description": {
                "target_field": "rawDescriptions",
                "strategy": MergeStrategy.APPEND_UNIQUE.value,
                "priority": MergePriority.MEDIUM.value,
                "transform": "append_to_array"
            },
            "name": {
                "target_field": "name", 
                "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                "priority": MergePriority.HIGH.value
            },
            "aliases": {
                "target_field": "aliases",
                "strategy": MergeStrategy.APPEND_UNIQUE.value,
                "priority": MergePriority.MEDIUM.value
            },
            "sources": {
                "target_field": "sources",
                "strategy": MergeStrategy.APPEND_UNIQUE.value,
                "priority": MergePriority.MEDIUM.value
            }
        }
        
        # Entity-specific configurations
        self.entity_configs = {
            "Person": {
                **common_mappings,
                "email": {
                    "target_field": "email",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.CRITICAL.value
                },
                "role": {
                    "target_field": "role",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value
                },
                "worksAt": {
                    "target_field": "worksAt",
                    "strategy": MergeStrategy.REPLACE_IF_BETTER.value,
                    "priority": MergePriority.MEDIUM.value
                },
                "cleanDescription": {
                    "target_field": "cleanDescription",
                    "strategy": MergeStrategy.AGENT_ONLY.value,
                    "priority": MergePriority.GENERATED.value
                }
            },
            
            "Organization": {
                **common_mappings,
                "domain": {
                    "target_field": "domain",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.CRITICAL.value
                },
                "industry": {
                    "target_field": "industry",
                    "strategy": MergeStrategy.REPLACE_IF_BETTER.value,
                    "priority": MergePriority.MEDIUM.value
                },
                "location": {
                    "target_field": "location",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value
                }
            },
            
            "Repository": {
                **common_mappings,
                "url": {
                    "target_field": "url",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.CRITICAL.value
                },
                "language": {
                    "target_field": "language",
                    "strategy": MergeStrategy.REPLACE_IF_BETTER.value,
                    "priority": MergePriority.MEDIUM.value
                }
            },
            
            "Issue": {
                **common_mappings,
                "id": {
                    "target_field": "id",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.CRITICAL.value
                },
                "title": {
                    "target_field": "name",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value
                },
                "status": {
                    "target_field": "status",
                    "strategy": MergeStrategy.REPLACE_ALWAYS.value,
                    "priority": MergePriority.LOW.value
                },
                "labels": {
                    "target_field": "labels",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value
                }
            },
            
            "CodeChangeRequest": {
                **common_mappings,
                "id": {
                    "target_field": "id",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.CRITICAL.value
                },
                "title": {
                    "target_field": "title",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value
                },
                "status": {
                    "target_field": "status",
                    "strategy": MergeStrategy.REPLACE_ALWAYS.value,
                    "priority": MergePriority.LOW.value
                },
                "author": {
                    "target_field": "author",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value
                },
                "reviewers": {
                    "target_field": "reviewers",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value
                },
                "createdAt": {
                    "target_field": "createdAt",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value,
                    "transform": "parse_timestamp"
                }
            },
            
            "Project": {
                **common_mappings,
                "status": {
                    "target_field": "status",
                    "strategy": MergeStrategy.REPLACE_ALWAYS.value,
                    "priority": MergePriority.LOW.value
                },
                "tags": {
                    "target_field": "tags",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value
                }
            },
            
            "Branch": {
                **common_mappings,
                "repo": {
                    "target_field": "repo",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.CRITICAL.value
                },
                "createdBy": {
                    "target_field": "createdBy",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value
                }
            },
            
            "Team": {
                "description": {
                    "target_field": "rawDescriptions",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value,
                    "transform": "append_to_array"
                },
                "name": {
                    "target_field": "name", 
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value
                },
                "aliases": {
                    "target_field": "aliases",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value
                }
                # Note: Team doesn't have sources field in schema
            },
            
            "Event": {
                **common_mappings,
                "id": {
                    "target_field": "id",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.CRITICAL.value
                },
                "title": {
                    "target_field": "name",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value
                },
                "type": {
                    "target_field": "type",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value
                },
                "startTime": {
                    "target_field": "startTime",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.HIGH.value,
                    "transform": "parse_timestamp"
                }
            },
            
            "Topic": {
                **common_mappings,
                "id": {
                    "target_field": "id",
                    "strategy": MergeStrategy.PRESERVE_EXISTING.value,
                    "priority": MergePriority.CRITICAL.value
                },
                "keywords": {
                    "target_field": "keywords",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value
                },
                "relatedThreads": {
                    "target_field": "relatedThreads",
                    "strategy": MergeStrategy.APPEND_UNIQUE.value,
                    "priority": MergePriority.MEDIUM.value
                }
            }
        }
        
        # Entity-specific array fields (only fields that exist in schema)
        entity_array_fields = {
            "Person": ["rawDescriptions", "sources", "role", "aliases"],
            "Team": ["rawDescriptions", "aliases"],  # Team doesn't have sources field
            "Organization": ["rawDescriptions", "sources", "aliases", "location"],
            "Project": ["rawDescriptions", "sources", "aliases", "tags"],
            "Repository": ["rawDescriptions", "sources"],
            "Branch": ["rawDescriptions", "sources"],
            "CodeChangeRequest": ["rawDescriptions", "sources", "reviewers"],
            "Issue": ["rawDescriptions", "sources", "assignees", "labels"],
            "Event": ["rawDescriptions", "sources"],
            "Topic": ["rawDescriptions", "sources", "aliases", "keywords", "relatedThreads"]
        }
        
        # Global field mappings (apply to all entities if not overridden)
        self.field_mappings = {
            "always_preserve": ["entity_id", "lastUpdated", "createdAt", "embedding"],
            "agent_only": ["cleanDescription"],
            "entity_array_fields": entity_array_fields,
            "critical_fields": ["entity_id", "email", "domain", "url", "id"],
            "timestamp_fields": ["createdAt", "mergedAt", "closedAt", "startTime"]
        }
    
    def save_config(self):
        """Save current configuration to YAML file"""
        config_data = {
            "field_mappings": self.field_mappings,
            "entity_configs": self.entity_configs
        }
        
        with open(self.config_file, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)
    
    def get_field_config(self, entity_type: str, field_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific field in an entity type"""
        if entity_type in self.entity_configs:
            return self.entity_configs[entity_type].get(field_name)
        return None
    
    def get_target_field(self, entity_type: str, llm_field: str) -> str:
        """Get the target database field for an LLM-generated field"""
        config = self.get_field_config(entity_type, llm_field)
        if config and "target_field" in config:
            return config["target_field"]
        return llm_field  # Default to same name
    
    def get_merge_strategy(self, entity_type: str, field_name: str) -> MergeStrategy:
        """Get merge strategy for a field"""
        config = self.get_field_config(entity_type, field_name)
        if config and "strategy" in config:
            return MergeStrategy(config["strategy"])
        return MergeStrategy.REPLACE_IF_BETTER  # Default strategy
    
    def should_merge_field(self, entity_type: str, field_name: str, is_from_agent: bool = False) -> bool:
        """Determine if a field should be merged based on its configuration"""
        config = self.get_field_config(entity_type, field_name)
        
        if not config:
            return True  # Allow by default
        
        strategy = MergeStrategy(config.get("strategy", MergeStrategy.REPLACE_IF_BETTER.value))
        
        # Agent-only fields can only be set by cleanup agents
        if strategy == MergeStrategy.AGENT_ONLY and not is_from_agent:
            return False
        
        return True
    
    def get_entity_array_fields(self, entity_type: str) -> List[str]:
        """Get array fields for a specific entity type"""
        return self.field_mappings.get('entity_array_fields', {}).get(entity_type, [])
    
    def is_array_field(self, entity_type: str, field_name: str) -> bool:
        """Check if a field is an array field for the given entity type"""
        return field_name in self.get_entity_array_fields(entity_type)

    def transform_value(self, entity_type: str, field_name: str, value: Any, target_field: str) -> Any:
        """Transform a value according to field configuration"""
        config = self.get_field_config(entity_type, field_name)
        
        if not config or "transform" not in config:
            return value
        
        transform = config["transform"]
        
        if transform == "append_to_array":
            # If target is an array field, ensure value is in array format
            if self.is_array_field(entity_type, target_field):
                return [value] if not isinstance(value, list) else value
        
        elif transform == "parse_timestamp":
            # Handle timestamp parsing if needed
            # This could be expanded to handle different timestamp formats
            return value
        
        return value

# Global instance
merge_config = MergeConfig()