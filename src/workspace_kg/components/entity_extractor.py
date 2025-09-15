import os
import json
import openai
import re
import asyncio
from typing import Dict, List, Any, Tuple, Optional

from workspace_kg.utils.prompt import DEFAULT_ENTITY_TYPES
from workspace_kg.utils.prompt_factory import PromptFactory, DataType
from workspace_kg.config import PARALELL_LLM_CALLS, BATCH_SIZE

class EntityExtractor:
    def __init__(self, 
                 model: str = "gemini-2.5-flash",
                 data_type: DataType = DataType.EMAIL,
                 auto_detect_data_type: bool = True):
        self.client = openai.AsyncOpenAI(
            api_key=os.getenv("OPENAI_API_KEY"),
            base_url=os.getenv("OPENAI_API_BASE_URL"),
        )
        self.model = model
        self.data_type = data_type
        self.auto_detect_data_type = auto_detect_data_type
        self.prompt_factory = PromptFactory()

    async def _call_llm_async(self, messages: List[Dict[str, str]]) -> str:
        """
        Asynchronously calls the LLM API.
        """
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.1
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error calling LLM API: {str(e)}")
            return ""

    async def extract_entities_batch(self, 
                                     data_batch: List[Dict[str, Any]], 
                                     entity_types: List[str] = None) -> List[Dict[str, Any]]:
        """
        Extracts entities from a batch of data asynchronously.
        """
        if entity_types is None:
            entity_types = DEFAULT_ENTITY_TYPES
            
        tasks = []
        for item in data_batch:
            item_id = item.get('id', 'unknown_id')
            context = item.get('content', '') # Assuming 'content' holds the text to extract from
            
            if not context.strip():
                print(f"Skipping item {item_id} due to empty content.")
                tasks.append(asyncio.sleep(0, result={
                    "item_id": item_id,
                    "entities": [],
                    "relationships": [],
                    "error": "Empty content",
                    "raw_llm_output": "",
                    "data_type": "unknown"
                }))
                continue

            # Determine data type for this item
            current_data_type = self.data_type
            if self.auto_detect_data_type:
                current_data_type = self.prompt_factory.detect_data_type(item)
            
            # Get appropriate prompts for this data type
            system_prompt = self.prompt_factory.get_system_prompt(current_data_type)
            formatted_prompt = self.prompt_factory.create_extraction_prompt(
                current_data_type, context, entity_types
            )
            
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": formatted_prompt}
            ]
            
            tasks.append(self._extract_single_item_async(item_id, messages, current_data_type.value))
        
        return await asyncio.gather(*tasks)

    async def _extract_single_item_async(self, item_id: str, messages: List[Dict[str, str]], data_type: str = "email") -> Dict[str, Any]:
        """
        Helper to extract entities for a single item asynchronously.
        """
        llm_output = await self._call_llm_async(messages)
        
        if not llm_output:
            return {
                "item_id": item_id,
                "entities": [],
                "relationships": [],
                "error": "LLM call failed or returned empty",
                "raw_llm_output": "",
                "data_type": data_type,
                "processed_at": self._get_timestamp()
            }

        try:
            entities, relationships = self.parse_llm_output(llm_output)
            return {
                "item_id": item_id,
                "entities": entities,
                "relationships": relationships,
                "raw_llm_output": llm_output,
                "data_type": data_type,
                "processed_at": self._get_timestamp(),
                "entity_count": len(entities),
                "relationship_count": len(relationships)
            }
        except Exception as e:
            print(f"Error parsing LLM output for item {item_id}: {str(e)}")
            return {
                "item_id": item_id,
                "entities": [],
                "relationships": [],
                "error": str(e),
                "raw_llm_output": llm_output,
                "data_type": data_type,
                "processed_at": self._get_timestamp()
            }
    
    def _get_timestamp(self) -> str:
        """Get current timestamp for tracking"""
        from datetime import datetime
        return datetime.now().isoformat()

    def parse_llm_output(self, llm_output: str) -> Tuple[List[Dict], List[Dict]]:
        """
        Parse LLM output to extract entities and relationships
        """
        entities = []
        relationships = []
        
        try:
            lines = llm_output.split('\n')
            tuple_section = ""
            
            for line in lines:
                line = line.strip()
                if line.startswith('("entity"') or line.startswith('("relationship"'):
                    tuple_section += line + "\n"
                elif tuple_section and (line.startswith('("entity"') or line.startswith('("relationship"')):
                    tuple_section += line + "\n"
            
            if not tuple_section:
                tuple_section = llm_output
            
            records = tuple_section.split('##')
            
            for record in records:
                record = record.strip()
                if not record or record == '<|COMPLETE|>':
                    continue
                    
                try:
                    if record.startswith('("entity"'):
                        entity = self.parse_entity_record(record)
                        if entity:
                            entities.append(entity)
                    elif record.startswith('("relationship"'):
                        relationship = self.parse_relationship_record(record)
                        if relationship:
                            relationships.append(relationship)
                except Exception as e:
                    print(f"Error parsing record: {record[:100]}... Error: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"Error parsing LLM output: {str(e)}")
            
        return entities, relationships
    
    def parse_entity_record(self, record: str) -> Dict[str, Any]:
        """Parse entity record from tuple format"""
        try:
            record = record.strip()
            if record.startswith('("entity"') and record.endswith(')'):
                record = record[1:-1]
            
            parts = record.split('<|>')
            if len(parts) < 3:
                return None
                
            entity_type_marker = parts[0].strip().strip('"')
            entity_name = parts[1].strip().strip('"')
            entity_type = parts[2].strip().strip('"')
            
            attributes = {"name": entity_name}
            
            for i in range(3, len(parts)):
                attr_part = parts[i].strip()
                if ':' in attr_part:
                    attr_match = re.match(r'"([^"]+)":\s*"([^"]*)"', attr_part)
                    if attr_match:
                        attr_name = attr_match.group(1)
                        attr_value = attr_match.group(2)
                        
                        if attr_value.startswith('[') and attr_value.endswith(']'):
                            try:
                                attr_value = attr_value[1:-1]
                                if attr_value:
                                    attr_value = [item.strip().strip('"') for item in attr_value.split(',')]
                                else:
                                    attr_value = []
                            except:
                                pass
                        
                        attributes[attr_name] = attr_value
            
            return {
                "entity_name": entity_name,
                "entity_type": entity_type,
                "attributes": attributes
            }
            
        except Exception as e:
            print(f"Error parsing entity record: {str(e)}")
            return None
    
    def parse_relationship_record(self, record: str) -> Dict[str, Any]:
        """Parse relationship record from tuple format"""
        try:
            record = record.strip()
            if record.startswith('("relationship"') and record.endswith(')'):
                record = record[1:-1]
            
            parts = record.split('<|>')
            if len(parts) < 6:
                return None
                
            rel_type_marker = parts[0].strip().strip('"')
            source_entity = parts[1].strip().strip('"')
            target_entity = parts[2].strip().strip('"')
            relationship_type = parts[3].strip().strip('"')
            description = parts[4].strip().strip('"')
            
            strength_part = parts[5].strip()
            strength_match = re.search(r'(\d+(?:\.\d+)?)', strength_part)
            strength = float(strength_match.group(1)) if strength_match else 5.0
            
            return {
                "source_entity": source_entity,
                "target_entity": target_entity,
                "relationship_type": relationship_type,
                "description": description,
                "strength": strength
            }
            
        except Exception as e:
            print(f"Error parsing relationship record: {str(e)}")
            return None
