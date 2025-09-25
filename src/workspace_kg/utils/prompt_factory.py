from typing import Dict, List, Optional
from enum import Enum

from .prompt import EMAIL_SYSTEM_PROMPT, ENTITY_EXTRACTION_PROMPT, DEFAULT_ENTITY_TYPES


class DataType(Enum):
    EMAIL = "email"
    DOCUMENT = "document"


class PromptFactory:
    """Factory class to generate appropriate prompts based on data type and configuration"""
    
    def __init__(self):
        self._system_prompts = {
            DataType.EMAIL: EMAIL_SYSTEM_PROMPT,
            DataType.DOCUMENT: self._get_document_system_prompt(),
        }
        
        self._extraction_templates = {
            DataType.EMAIL: ENTITY_EXTRACTION_PROMPT,
            DataType.DOCUMENT: self._get_document_extraction_template(),
        }
    
    def get_system_prompt(self, data_type: DataType) -> str:
        """Get system prompt for the specified data type"""
        return self._system_prompts.get(data_type, EMAIL_SYSTEM_PROMPT)
    
    def get_extraction_template(self, data_type: DataType) -> str:
        """Get extraction prompt template for the specified data type"""
        return self._extraction_templates.get(data_type, ENTITY_EXTRACTION_PROMPT)
    
    def create_extraction_prompt(self, 
                               data_type: DataType, 
                               context: str, 
                               entity_types: List[str] = None) -> str:
        """Create formatted extraction prompt for specific data and entity types"""
        if entity_types is None:
            entity_types = DEFAULT_ENTITY_TYPES
            
        template = self.get_extraction_template(data_type)
        return template.format(
            entity_types=", ".join(entity_types),
            context=context
        )
    
    def detect_data_type(self, data: Dict) -> DataType:
        """Auto-detect data type based on data structure and content"""
        # Check for explicit data_type field
        if 'data_type' in data:
            try:
                return DataType(data['data_type'].lower())
            except ValueError:
                pass
        
        # Check for email-specific fields
        if any(field in data for field in ['from', 'to', 'subject', 'sender', 'recipient']):
            return DataType.EMAIL
        
        # Check for code-specific fields
        if any(field in data for field in ['repository', 'commit', 'pull_request', 'code', 'file_path']):
            return DataType.CODE
        
        # Check for meeting-specific fields
        if any(field in data for field in ['meeting_title', 'attendees', 'transcript', 'agenda']):
            return DataType.MEETING
        
        # Check for chat-specific fields
        if any(field in data for field in ['channel', 'thread', 'message_thread', 'chat_id']):
            return DataType.CHAT
        
        # Default to email for backward compatibility
        return DataType.EMAIL
    
    def _get_document_system_prompt(self) -> str:
        """System prompt optimized for document data"""
        return """---Goal---
Your goal is to extract workspace-level entities and relationships from document content in tuple format. Focus on extracting business information, project details, organizational structure, and collaboration patterns from documents.

Use English as output language.

---CRITICAL FORMATTING REQUIREMENTS---
1. **MUST return ONLY tuple format** - no JSON, no markdown, no code blocks, with some reasoning explanatory text before generation
2. **Use specific delimiters** - <|> between fields, ## between records
3. **ALL string values MUST be properly escaped** - escape quotes and special characters
4. **NO line breaks inside string values** - use \\n for line breaks if needed
5. **End with completion delimiter** - <|COMPLETE|>
6. **Use specific names that qualify the entity type** - be descriptive and specific
7. **Names must be unique** - avoid generic names. Only extract the specific names found in the data.

---Document-Specific Instructions---
1. **Business Entities**: Extract organizations, projects, teams, people, processes
2. **Document Relationships**: Focus on AUTHORED, REVIEWED, MENTIONS, DESCRIBES, REFERENCES relationships
3. **Project Information**: Extract project status, timelines, deliverables, stakeholders
4. **Organizational Context**: Include departments, roles, responsibilities when mentioned
5. **Process Information**: Extract workflows, procedures, and business processes

---MANDATORY OUTPUT FORMAT---
For each entity, output ONE line in this exact format:
("entity"<|>"<entity_name>"<|>"<entity_type>"<|>"<attribute_name>": "<attribute_value>"<|>"<attribute_name>": "<attribute_value>")##

For relationships, output ONE line in this exact format:
("relationship"<|>"<source_entity>"<|>"<target_entity>"<|>"<relationship_type>"<|>"<description>"<|><strength>)##
"""
     
    def _get_document_extraction_template(self) -> str:
        """Extraction template for document data"""
        return """
---Real Data---
Entity_types: {entity_types}
Document Content: {context}

IMPORTANT: Return ONLY the tuple format below, end with <|COMPLETE|>:"""
    