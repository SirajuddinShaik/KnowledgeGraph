#!/usr/bin/env python3
"""
Merge Pipeline for Knowledge Graph
Processes batches of extracted entities and relations, performing deduplication
and merging according to established rules.
"""

import asyncio
import json
import logging
import os
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

from workspace_kg.utils.kuzu_db_handler import KuzuDBHandler
from workspace_kg.utils.entity_config import entity_config, MergeStrategy
from workspace_kg.components.ollama_embedder import InferenceProvider
from workspace_kg.components.systematic_merge_provider import SystematicMergeProvider

logger = logging.getLogger(__name__)

class MergePipeline:
    def __init__(self, kuzu_api_url: str = "http://localhost:7000", schema_file: str = 'schema.yaml', use_systematic_merge: bool = True):
        self.db_handler = KuzuDBHandler(kuzu_api_url, schema_file)
        self.stats = {
            "total_batches": 0,
            "total_entities_processed": 0,
            "total_relations_processed": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None
        }
        try:
            self.inference_provider = InferenceProvider()
        except Exception as e:
            logger.warning(f"Failed to initialize InferenceProvider, continuing without embeddings: {e}")
            self.inference_provider = None
        
        self.use_systematic_merge = use_systematic_merge
        if use_systematic_merge:
            self.systematic_merge_provider = SystematicMergeProvider(self.db_handler)
        else:
            self.systematic_merge_provider = None

    # Methods from MergeHandler
    def _generate_entity_id(self, entity_type: str, attributes: Dict[str, Any]) -> str:
        unique_str = f"{entity_type}"
        if entity_type == "Person":
            if "emails" in attributes and attributes["emails"]:
                unique_str += f"::email::{attributes['emails'][0].lower()}"
            elif "name" in attributes and "worksAt" in attributes:
                unique_str += f"::name_worksAt::{attributes['name'].lower()}::{attributes['worksAt'].lower()}"
            elif "name" in attributes:
                unique_str += f"::name::{attributes['name'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        elif entity_type == "Organization":
            if "domain" in attributes:
                unique_str += f"::domain::{attributes['domain'].lower()}"
            elif "name" in attributes:
                unique_str += f"::name::{attributes['name'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        else:
            if "name" in attributes:
                unique_str += f"::name::{attributes['name'].lower()}"
            else:
                unique_str += f"::fallback::{json.dumps(attributes, sort_keys=True)}"
        return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()

    def _generate_relation_id(self, from_entity_id: str, to_entity_id: str, relation_type: str, relation_tag: str) -> str:
        unique_str = f"{from_entity_id}::{relation_type}::{relation_tag}::{to_entity_id}"
        return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()

    async def _find_existing_entity(self, entity_type: str, entity_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        generated_id = self._generate_entity_id(entity_type, entity_data.get('attributes', {}))
        existing_entity = await self.db_handler.get_entity(entity_type, generated_id)
        if existing_entity:
            return existing_entity
        
        attributes = entity_data.get('attributes', {})
        if entity_type == "Person":
            if "email" in attributes:
                query = f"MATCH (p:Person) WHERE $email IN p.emails RETURN p"
                params = {"email": attributes['email']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
            if "name" in attributes and "worksAt" in attributes:
                query = f"MATCH (p:Person {{name: $name, worksAt: $worksAt}}) RETURN p"
                params = {"name": attributes['name'], "worksAt": attributes['worksAt']}
                result = await self.db_handler.execute_cypher(query, params)
                if result and result.get('data'):
                    return result['data'][0]['p']
        return None

    def _process_attributes(self, entity_type: str, attributes: Dict[str, Any], source_item_id: str, entity_name: str, is_from_agent: bool = False) -> Dict[str, Any]:
        processed = {}
        processed['rawDescriptions'] = []
        entity_array_fields = entity_config.get_entity_array_fields(entity_type)
        if 'sources' in entity_array_fields:
            processed['sources'] = []
        
        for llm_field, value in attributes.items():
            if not entity_config.should_merge_field(entity_type, llm_field, is_from_agent):
                continue
            
            target_field = entity_config.get_target_field(entity_type, llm_field)
            transformed_value = entity_config.transform_value(entity_type, llm_field, value, target_field)
            
            if llm_field == "description":
                if transformed_value and isinstance(transformed_value, list):
                    processed['rawDescriptions'].extend(transformed_value)
                elif transformed_value:
                    processed['rawDescriptions'].append(transformed_value)
            else:
                processed[target_field] = transformed_value
        
        if 'sources' in processed and source_item_id not in processed['sources']:
            processed['sources'].append(source_item_id)

        for field in entity_array_fields:
            if field in processed and not isinstance(processed[field], list):
                processed[field] = [processed[field]]
            elif field not in processed:
                processed[field] = []
        
        return processed

    def _merge_attributes(self, entity_type: str, existing_entity: Dict[str, Any], new_attributes: Dict[str, Any]) -> Dict[str, Any]:
        updates = {}
        for field, new_value in new_attributes.items():
            strategy_str = entity_config.get_merge_strategy(entity_type, field)
            strategy = MergeStrategy(strategy_str)
            existing_value = existing_entity.get(field)
            
            if strategy == MergeStrategy.PRESERVE_EXISTING:
                if not existing_value:
                    updates[field] = new_value
            elif strategy == MergeStrategy.APPEND_UNIQUE:
                if isinstance(new_value, list):
                    existing_list = existing_value if isinstance(existing_value, list) else []
                    merged_list = list(existing_list)
                    for item in new_value:
                        if item not in merged_list:
                            merged_list.append(item)
                    updates[field] = merged_list
                else:
                    existing_list = existing_value if isinstance(existing_value, list) else []
                    if new_value not in existing_list:
                        updates[field] = existing_list + [new_value]
            elif strategy == MergeStrategy.REPLACE_ALWAYS:
                updates[field] = new_value
            elif strategy == MergeStrategy.REPLACE_IF_BETTER:
                if not existing_value or (isinstance(new_value, str) and len(new_value) > len(str(existing_value))):
                    updates[field] = new_value
        return updates

    async def process_batch_systematic(self, batch_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.use_systematic_merge or not self.systematic_merge_provider:
            return await self.process_batch(batch_data)
        
        if 'entities' in batch_data and 'relations' in batch_data:
            entities_list = batch_data['entities']
            relations_list = batch_data['relations']
            source_item_id = batch_data.get('source_item_id', 'unknown')
        elif 'item_id' in batch_data:
            entities_list = batch_data.get('entities', [])
            relations_list = batch_data.get('relationships', [])
            source_item_id = batch_data['item_id']
        else:
            return {"status": "error", "message": "Unknown batch data format"}
        
        try:
            entity_groups = await self.systematic_merge_provider.process_entities_systematic(entities_list)
            entity_mapping, merge_stats = await self.systematic_merge_provider.merge_groups_to_database(entity_groups, source_item_id)
            relations_processed = await self.systematic_merge_provider.process_relations_systematic(relations_list, entity_mapping, source_item_id)
            
            return {
                "status": "success",
                "entities_processed": merge_stats["entities_processed"] + merge_stats["entities_merged"],
                "relations_processed": relations_processed,
                "merge_method": "systematic",
                "merge_stats": merge_stats
            }
        except Exception as e:
            logger.error(f"Systematic merge processing failed: {e}")
            return await self.process_batch(batch_data)

    async def process_batch(self, batch_data: Dict[str, Any]) -> Dict[str, Any]:
        if 'entities' in batch_data and 'relations' in batch_data:
            entities_list = batch_data['entities']
            relations_list = batch_data['relations']
            source_item_id = batch_data.get('source_item_id', 'unknown')
        elif 'item_id' in batch_data:
            entities_list = batch_data.get('entities', [])
            relations_list = batch_data.get('relationships', [])
            source_item_id = batch_data['item_id']
        else:
            return {"status": "error", "message": "Unknown batch data format"}

        processed_entities = {}
        processed_relations = []

        for entity_raw in entities_list:
            entity_type = entity_raw.get('entity_type') or entity_raw.get('type')
            entity_name = entity_raw.get('entity_name') or entity_raw.get('name')
            if not entity_type or not entity_name:
                continue
            attributes = entity_raw.get('attributes', {}).copy()
            processed_attributes = self._process_attributes(entity_type, attributes, source_item_id, entity_name)
            existing_entity = await self._find_existing_entity(entity_type, entity_raw)

            if existing_entity:
                entity_id = existing_entity['entity_id']
                updates = self._merge_attributes(entity_type, existing_entity, processed_attributes)
                if updates:
                    await self.db_handler.update_entity(entity_type, entity_id, updates)
                processed_entities[entity_name] = {'entity_id': entity_id, 'entity_type': entity_type}
            else:
                entity_id = self._generate_entity_id(entity_type, processed_attributes)
                processed_attributes['entity_id'] = entity_id
                new_entity = await self.db_handler.create_entity(entity_type, processed_attributes)
                if new_entity:
                    processed_entities[entity_name] = {'entity_id': entity_id, 'entity_type': entity_type}
        
        for rel_raw in relations_list:
            source_entity_name = rel_raw.get('source_entity') or rel_raw.get('source')
            target_entity_name = rel_raw.get('target_entity') or rel_raw.get('target')
            relationship_type = rel_raw.get('relationship_type') or rel_raw.get('type')
            if not source_entity_name or not target_entity_name or not relationship_type:
                continue
            if source_entity_name not in processed_entities or target_entity_name not in processed_entities:
                continue

            from_entity_id = processed_entities[source_entity_name]['entity_id']
            from_entity_type = processed_entities[source_entity_name]['entity_type']
            to_entity_id = processed_entities[target_entity_name]['entity_id']
            to_entity_type = processed_entities[target_entity_name]['entity_type']
            relation_tag = relationship_type
            relation_id = self._generate_relation_id(from_entity_id, to_entity_id, relationship_type, relation_tag)
            relation_properties = {"relation_id": relation_id, "sources": [source_item_id]}
            
            await self.db_handler.create_relation(from_entity_type, from_entity_id, to_entity_type, to_entity_id, relation_properties)
            processed_relations.append(relation_id)

        return {"status": "success", "entities_processed": len(processed_entities), "relations_processed": len(processed_relations)}

    # Pipeline processing methods
    async def initialize(self):
        try:
            await self.db_handler.execute_cypher("RETURN 'connection_test' as status")
            logger.info("✅ Database connection established")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to initialize database connection: {e}")
            return False

    async def process_extracted_file(self, file_path: str) -> Dict[str, Any]:
        if not os.path.exists(file_path):
            return {"status": "error", "message": f"File not found: {file_path}"}
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            if 'results' in data:
                batches = data['results']
            elif 'entities' in data and 'relations' in data:
                batches = [data]
            else:
                return {"status": "error", "message": "Unknown file format"}
            return await self.process_batches(batches)
        except Exception as e:
            return {"status": "error", "message": f"Processing error: {e}"}

    async def process_batches(self, batches: List[Dict[str, Any]]) -> Dict[str, Any]:
        self.stats["start_time"] = datetime.now()
        self.stats["total_batches"] = len(batches)
        batch_results = []
        for i, batch in enumerate(batches):
            try:
                result = await self.process_batch_systematic(batch)
                batch_results.append(result)
                if result.get("status") == "success":
                    self.stats["total_entities_processed"] += result.get("entities_processed", 0)
                    self.stats["total_relations_processed"] += result.get("relations_processed", 0)
                else:
                    self.stats["errors"] += 1
            except Exception as e:
                self.stats["errors"] += 1
                batch_results.append({"status": "error", "message": str(e)})
        self.stats["end_time"] = datetime.now()
        processing_time = (self.stats["end_time"] - self.stats["start_time"]).total_seconds()
        return {
            "status": "completed",
            "statistics": self.stats,
            "batch_results": batch_results,
            "processing_time_seconds": processing_time
        }

    async def process_directory(self, directory_path: str, pattern: str = "*.json") -> Dict[str, Any]:
        directory = Path(directory_path)
        if not directory.exists():
            return {"status": "error", "message": f"Directory not found: {directory_path}"}
        json_files = list(directory.glob(pattern))
        if not json_files:
            return {"status": "warning", "message": f"No files matching {pattern} found"}
        
        combined_stats = {"files_processed": 0, "total_entities": 0, "total_relations": 0, "total_errors": 0, "file_results": []}
        for file_path in json_files:
            result = await self.process_extracted_file(str(file_path))
            combined_stats["files_processed"] += 1
            combined_stats["file_results"].append({"file": file_path.name, "result": result})
            if result.get("status") == "completed":
                stats = result.get("statistics", {})
                combined_stats["total_entities"] += stats.get("total_entities_processed", 0)
                combined_stats["total_relations"] += stats.get("total_relations_processed", 0)
            else:
                combined_stats["total_errors"] += 1
        return {"status": "completed", "combined_statistics": combined_stats}

    async def get_database_statistics(self) -> Dict[str, Any]:
        try:
            stats = {}
            for entity_type in self.db_handler.entity_schemas.keys():
                query = f"MATCH (n:{entity_type}) RETURN count(n) as count"
                result = await self.db_handler.execute_cypher(query)
                count = result.get('data', [{}])[0].get('count', 0) if result.get('data') else 0
                stats[f"{entity_type}_count"] = count
            query = "MATCH ()-[r:Relation]->() RETURN count(r) as count"
            result = await self.db_handler.execute_cypher(query)
            stats["total_relations"] = result.get('data', [{}])[0].get('count', 0) if result.get('data') else 0
            stats["total_entities"] = sum(v for k, v in stats.items() if k.endswith('_count'))
            return stats
        except Exception as e:
            return {"error": str(e)}

    async def cleanup(self):
        await self.db_handler.close()

async def process_file(file_path: str, kuzu_url: str = "http://localhost:7000") -> Dict[str, Any]:
    pipeline = MergePipeline(kuzu_url)
    try:
        if not await pipeline.initialize():
            return {"status": "error", "message": "Failed to initialize pipeline"}
        result = await pipeline.process_extracted_file(file_path)
        db_stats = await pipeline.get_database_statistics()
        result["database_statistics"] = db_stats
        return result
    finally:
        await pipeline.cleanup()

async def process_directory(directory_path: str, pattern: str = "*.json", kuzu_url: str = "http://localhost:7000") -> Dict[str, Any]:
    pipeline = MergePipeline(kuzu_url)
    try:
        if not await pipeline.initialize():
            return {"status": "error", "message": "Failed to initialize pipeline"}
        result = await pipeline.process_directory(directory_path, pattern)
        db_stats = await pipeline.get_database_statistics()
        result["database_statistics"] = db_stats
        return result
    finally:
        await pipeline.cleanup()

async def main():
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    if len(sys.argv) < 2:
        print("Usage: python merge_pipeline.py <file_path_or_directory>")
        return
    path = sys.argv[1]
    if os.path.isfile(path):
        result = await process_file(path)
    elif os.path.isdir(path):
        result = await process_directory(path)
    else:
        print(f"Path not found: {path}")
        return
    print(json.dumps(result, indent=2, default=str))

if __name__ == "__main__":
    asyncio.run(main())
