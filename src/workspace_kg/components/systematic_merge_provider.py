#!/usr/bin/env python3
"""
Systematic Multi-Attribute Entity Merge Provider

Implements the systematic grouping approach:
1. Batch processing - assign IDs to each entity
2. N×N similarity comparison with case-insensitive string matching
3. Group merging with transitive closure
4. Database matching and array field merging
5. Relation mapping and grouping
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from dataclasses import dataclass
from collections import defaultdict
import hashlib
import difflib

from workspace_kg.utils.entity_config import entity_config
from workspace_kg.components.ollama_embedder import InferenceProvider

logger = logging.getLogger(__name__)

@dataclass
class EntityItem:
    """Represents an entity with batch ID"""
    batch_id: int
    entity_type: str
    entity_name: str
    attributes: Dict[str, Any]
    original_data: Dict[str, Any]

@dataclass
class EntityGroup:
    """Represents a group of entities that should be merged"""
    group_id: str
    entity_type: str
    items: List[EntityItem]
    primary_entity_id: Optional[str] = None  # Existing DB entity to merge into
    primary_entity_data: Optional[Dict[str, Any]] = None
    
    def add_item(self, item: EntityItem):
        self.items.append(item)

class SystematicMergeProvider:
    """Systematic entity merge provider with N×N comparison and proper grouping"""
    
    def __init__(self, kuzu_db_handler):
        self.db_handler = kuzu_db_handler
        try:
            self.inference_provider = InferenceProvider()
        except Exception as e:
            logger.warning(f"Failed to initialize InferenceProvider, continuing without embeddings: {e}")
            self.inference_provider = None
        
    def _normalize_string(self, s: str) -> str:
        """Normalize string for comparison - always lowercase"""
        return s.lower().strip() if s else ""
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two normalized strings"""
        if not str1 or not str2:
            return 0.0
        norm1 = self._normalize_string(str1)
        norm2 = self._normalize_string(str2)
        return difflib.SequenceMatcher(None, norm1, norm2).ratio()
    
    def _entities_match(self, item1: EntityItem, item2: EntityItem) -> Tuple[bool, float, str]:
        """
        Check if two entities match using configuration-based systematic rules.
        Returns (matches, confidence, reason)
        """
        if item1.entity_type != item2.entity_type:
            return False, 0.0, "different_types"
        
        # Get matching rules from configuration
        matching_rules = entity_config.get_systematic_merge_rules(item1.entity_type)
        if not matching_rules:
            # Fallback to basic name matching if no rules configured
            return self._basic_name_match(item1, item2)
        
        attrs1 = item1.attributes
        attrs2 = item2.attributes
        
        # Apply rules in priority order
        for rule in sorted(matching_rules, key=lambda x: x.get("priority", 999)):
            rule_type = rule.get("rule", "")
            match_field = rule.get("match", "")
            db_field = rule.get("db", match_field)
            field_type = rule.get("type", "string")
            confidence = rule.get("confidence", 0.5)
            
            if rule_type == "exact":
                # Exact match between two fields
                value1 = self._normalize_string(str(attrs1.get(match_field, '')))
                value2 = self._normalize_string(str(attrs2.get(match_field, '')))
                if value1 and value2 and value1 == value2:
                    return True, confidence, f"exact_{match_field}"
            
            elif rule_type == "search":
                # Search for value in array field
                search_value1 = self._normalize_string(str(attrs1.get(match_field, '')))
                search_value2 = self._normalize_string(str(attrs2.get(match_field, '')))
                
                if field_type == "list":
                    # Check if value1 exists in item2's db_field array or vice versa
                    list1 = attrs1.get(db_field, []) if isinstance(attrs1.get(db_field), list) else []
                    list2 = attrs2.get(db_field, []) if isinstance(attrs2.get(db_field), list) else []
                    
                    # Special handling for email field mapped to emails array
                    if match_field == "email" and db_field == "emails":
                        # Also check the single email field value against the arrays
                        if search_value1 and search_value1 in [self._normalize_string(str(v)) for v in list2]:
                            return True, confidence, f"search_{match_field}_in_{db_field}"
                        if search_value2 and search_value2 in [self._normalize_string(str(v)) for v in list1]:
                            return True, confidence, f"search_{match_field}_in_{db_field}"
                    
                    # Normalize list values
                    normalized_list1 = [self._normalize_string(str(v)) for v in list1]
                    normalized_list2 = [self._normalize_string(str(v)) for v in list2]
                    
                    # Check if search_value1 is in list2 or search_value2 is in list1
                    if (search_value1 and search_value1 in normalized_list2) or \
                       (search_value2 and search_value2 in normalized_list1):
                        return True, confidence, f"search_{match_field}_in_{db_field}"
                    
                    # Also check for overlap between the lists
                    if normalized_list1 and normalized_list2:
                        overlap = set(normalized_list1) & set(normalized_list2)
                        if overlap:
                            return True, confidence, f"overlap_{db_field}"
        
        return False, 0.0, "no_match"
    
    def _basic_name_match(self, item1: EntityItem, item2: EntityItem) -> Tuple[bool, float, str]:
        """Fallback basic name matching when no rules are configured"""
        attrs1 = item1.attributes
        attrs2 = item2.attributes
        
        name1 = self._normalize_string(attrs1.get('name', ''))
        name2 = self._normalize_string(attrs2.get('name', ''))
        
        if name1 and name2 and name1 == name2:
            return True, 0.70, "basic_name_match"
        
        return False, 0.0, "no_match"
    
    async def process_entities_systematic(self, entities_batch: List[Dict[str, Any]]) -> Dict[str, List[EntityGroup]]:
        """
        Step 1: Assign batch IDs to entities
        Step 2: Perform N×N comparison to find similarities
        Step 3: Group entities with transitive closure
        Step 4: Match groups against database
        """
        
        # Step 1: Assign batch IDs and create EntityItems
        entity_items = []
        for i, entity_data in enumerate(entities_batch):
            entity_type = entity_data.get('entity_type') or entity_data.get('type')
            entity_name = entity_data.get('entity_name') or entity_data.get('name')
            attributes = entity_data.get('attributes', {})
            
            if entity_type and entity_name:
                item = EntityItem(
                    batch_id=i,
                    entity_type=entity_type,
                    entity_name=entity_name,
                    attributes=attributes,
                    original_data=entity_data
                )
                entity_items.append(item)
        
        logger.info(f"🔢 Step 1: Assigned IDs to {len(entity_items)} entities")
        
        # Step 2: N×N comparison to find matching pairs
        entity_groups_by_type = defaultdict(list)
        processed_items = set()
        
        for i, item1 in enumerate(entity_items):
            if i in processed_items:
                continue
            
            # Start new group with this item
            group = EntityGroup(
                group_id=f"group_{item1.entity_type}_{i}",
                entity_type=item1.entity_type,
                items=[item1]
            )
            processed_items.add(i)
            
            # Compare with all other items of same type
            for j, item2 in enumerate(entity_items):
                if j <= i or j in processed_items or item1.entity_type != item2.entity_type:
                    continue
                
                matches, confidence, reason = self._entities_match(item1, item2)
                if matches:
                    group.add_item(item2)
                    processed_items.add(j)
                    logger.debug(f"✅ Matched {item1.entity_name} with {item2.entity_name} ({reason}, {confidence:.2f})")
            
            entity_groups_by_type[item1.entity_type].append(group)
        
        # Step 3: Apply transitive closure to merge overlapping groups
        for entity_type in entity_groups_by_type:
            entity_groups_by_type[entity_type] = self._apply_transitive_closure_systematic(
                entity_groups_by_type[entity_type]
            )
        
        # Log initial grouping results
        total_groups = sum(len(groups) for groups in entity_groups_by_type.values())
        total_items_in_groups = sum(len(item.items) for groups in entity_groups_by_type.values() for item in groups)
        
        logger.info(f"🔄 Step 2-3: Created {total_groups} groups from {len(entity_items)} entities")
        for entity_type, groups in entity_groups_by_type.items():
            multi_item_groups = [g for g in groups if len(g.items) > 1]
            if multi_item_groups:
                logger.info(f"   {entity_type}: {len(multi_item_groups)} groups with duplicates")
                for group in multi_item_groups:
                    names = [item.entity_name for item in group.items]
                    logger.info(f"      Group: {names}")
        
        # Step 4: Match groups against database
        await self._match_groups_with_database(entity_groups_by_type)
        
        return dict(entity_groups_by_type)
    
    def _apply_transitive_closure_systematic(self, groups: List[EntityGroup]) -> List[EntityGroup]:
        """Apply transitive closure - if A matches B and B matches C, then A, B, C should be in same group"""
        if len(groups) <= 1:
            return groups
        
        # Check for overlaps and merge
        merged_groups = []
        processed = set()
        
        for i, group1 in enumerate(groups):
            if i in processed:
                continue
            
            # Find all groups that should merge with this one
            to_merge = [group1]
            to_merge_indices = {i}
            
            # Keep checking for new overlaps until no more found
            changed = True
            while changed:
                changed = False
                for j, group2 in enumerate(groups):
                    if j in to_merge_indices or j in processed:
                        continue
                    
                    # Check if any items in group2 match any items in our merged groups
                    should_merge = False
                    for merge_group in to_merge:
                        for item1 in merge_group.items:
                            for item2 in group2.items:
                                matches, _, _ = self._entities_match(item1, item2)
                                if matches:
                                    should_merge = True
                                    break
                            if should_merge:
                                break
                        if should_merge:
                            break
                    
                    if should_merge:
                        to_merge.append(group2)
                        to_merge_indices.add(j)
                        changed = True
            
            # Merge all groups into one
            if len(to_merge) > 1:
                merged_group = EntityGroup(
                    group_id=f"merged_{groups[0].entity_type}_{i}",
                    entity_type=group1.entity_type,
                    items=[]
                )
                for group in to_merge:
                    merged_group.items.extend(group.items)
                merged_groups.append(merged_group)
            else:
                merged_groups.append(group1)
            
            processed.update(to_merge_indices)
        
        return merged_groups
    
    async def _match_groups_with_database(self, entity_groups_by_type: Dict[str, List[EntityGroup]]):
        """Step 4: Match each group against existing database entities"""
        
        for entity_type, groups in entity_groups_by_type.items():
            for group in groups:
                # Use the first item in group to search for database matches
                representative_item = group.items[0]
                
                # Try to find existing entity using systematic rules
                existing_entity = await self._find_existing_entity_systematic(
                    entity_type, representative_item.attributes
                )
                
                if existing_entity:
                    # Get the correct primary key field value (now always 'name')
                    group.primary_entity_id = existing_entity.get('name')
                    group.primary_entity_data = existing_entity
                    logger.debug(f"🔗 Group {group.group_id} matched with existing entity {group.primary_entity_id}")
                else:
                    logger.debug(f"🆕 Group {group.group_id} will create new entity")
    
    async def _find_existing_entity_systematic(self, entity_type: str, attributes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find existing entity using the same systematic rules"""
        
        # Get matching rules from configuration
        matching_rules = entity_config.get_systematic_merge_rules(entity_type)
        if not matching_rules:
            return None
        
        # Get the actual schema fields for this entity type to validate queries
        entity_schema = self.db_handler.entity_schemas.get(entity_type, {})
        
        # Apply rules in priority order
        for rule in sorted(matching_rules, key=lambda x: x.get("priority", 999)):
            rule_type = rule.get("rule", "")
            match_field = rule.get("match", "")
            db_field = rule.get("db", match_field)
            field_type = rule.get("type", "string")
            
            # Skip if the database field doesn't exist in the schema
            if db_field not in entity_schema:
                logger.debug(f"Skipping rule for {entity_type}.{db_field} - field not in schema")
                continue
            
            if rule_type == "exact":
                # Exact match query
                value = attributes.get(match_field, '').strip()
                if value and len(value) > 0:  # Ensure value is not empty
                    if entity_type == "Person" and match_field == "email" and db_field == "emails":
                        # Special case: Person email stored in emails array
                        # Use string concatenation to avoid parameter issues with array queries
                        query = f"MATCH (e:{entity_type}) WHERE ANY(x IN e.emails WHERE toLower(x) = toLower('{value}')) RETURN e"
                        params = None
                    else:
                        query = f"MATCH (e:{entity_type}) WHERE toLower(e.{db_field}) = toLower($value) RETURN e"
                        params = {"value": value}
                    
                    try:
                        result = await self.db_handler.execute_cypher(query, params)
                        if result and (result.get('data') or result.get('rows')):
                            data = result.get('data') or result.get('rows')
                            return data[0]['e']
                    except Exception as e:
                        logger.warning(f"Query failed for {entity_type}.{db_field}: {e}")
                        continue
            
            elif rule_type == "search" and field_type == "list":
                # Search in array field - only if the field actually exists and is an array
                field_schema = entity_schema.get(db_field, {})
                field_type_def = field_schema.get('type', '') if isinstance(field_schema, dict) else str(field_schema)
                
                if not field_type_def.endswith('[]'):
                    logger.debug(f"Skipping array search for {entity_type}.{db_field} - not an array field")
                    continue
                
                search_value = attributes.get(match_field, '').strip()
                if search_value and len(search_value) > 0:  # Ensure value is not empty
                    # Use string concatenation to avoid parameter issues with array queries
                    # Escape single quotes in the search value to prevent SQL injection
                    escaped_value = search_value.replace("'", "''")
                    query = f"MATCH (e:{entity_type}) WHERE ANY(x IN e.{db_field} WHERE toLower(x) = toLower('{escaped_value}')) RETURN e"
                    try:
                        result = await self.db_handler.execute_cypher(query, None)
                        if result and (result.get('data') or result.get('rows')):
                            data = result.get('data') or result.get('rows')
                            return data[0]['e']
                    except Exception as e:
                        logger.warning(f"Array search query failed for {entity_type}.{db_field}: {e}")
                        continue
        
        return None
    
    async def merge_groups_to_database(self, entity_groups_by_type: Dict[str, List[EntityGroup]],
                                     source_item_id: str) -> Dict[str, Any]:
        """
        Step 5: Merge groups to database with proper array field handling
        Returns mapping of entity_name -> entity_info for relation processing
        MODIFIED: Process entities one at a time to avoid 413 payload errors from large embeddings
        """

        processed_entities = {}  # entity_name -> {entity_id, entity_type}
        stats = {
            "entities_processed": 0,
            "entities_created": 0,
            "entities_merged": 0,
            "groups_processed": 0
        }

        # Process each entity type separately
        for entity_type, groups in entity_groups_by_type.items():
            logger.info(f"🔄 Processing {len(groups)} groups for entity type {entity_type}")

            # Process each group individually to avoid payload size issues
            for group_idx, group in enumerate(groups):
                stats["groups_processed"] += 1
                logger.debug(f"📦 Processing group {group_idx + 1}/{len(groups)} for {entity_type}")

                try:
                    if group.primary_entity_id:
                        # Merge all items into existing entity - process one group at a time
                        merged_entity_id = await self._merge_group_into_existing_single(
                            group, source_item_id
                        )
                        if merged_entity_id:
                            stats["entities_merged"] += len(group.items)

                            # CRITICAL FIX: Map all entity names to the EXISTING database entity ID
                            # Use the primary_entity_id (the actual database entity ID) not the merged_entity_id
                            actual_db_entity_id = group.primary_entity_id

                            # For merged entities, we need to be careful about the mapping
                            # The primary entity should keep its original name as the key
                            # but point to the database entity ID
                            primary_entity_name = None
                            if group.primary_entity_data:
                                primary_entity_name = group.primary_entity_data.get('name')

                            # For merged entities, the primary_entity_id is the database entity ID
                            # But all relations should point to the primary_entity_name
                            final_name = primary_entity_name if primary_entity_name else actual_db_entity_id

                            # Map the primary entity name to itself
                            if primary_entity_name:
                                processed_entities[primary_entity_name] = {
                                    'entity_id': primary_entity_name,
                                    'entity_type': entity_type
                                }

                            # Map all other entity names to the primary entity name
                            for item in group.items:
                                processed_entities[item.entity_name] = {
                                    'entity_id': final_name,
                                    'entity_type': entity_type,
                                    'is_merged': True,
                                    'is_alias': item.entity_name != final_name,
                                    'primary_name': final_name
                                }

                            logger.info(f"✅ Merged {len(group.items)} entities into existing {actual_db_entity_id}")
                        else:
                            logger.error(f"❌ Failed to merge group {group.group_id}: merge returned None")
                            # Add fallback mapping using the existing entity ID if available
                            if group.primary_entity_id:
                                # Get primary entity name for proper mapping
                                primary_entity_name = None
                                if group.primary_entity_data:
                                    primary_entity_name = group.primary_entity_data.get('name')

                                # Map primary entity name first
                                if primary_entity_name:
                                    processed_entities[primary_entity_name] = {
                                        'entity_id': group.primary_entity_id,
                                        'entity_type': entity_type
                                    }

                                # Map all entity names to the primary entity
                                for item in group.items:
                                    processed_entities[item.entity_name] = {
                                        'entity_id': group.primary_entity_id,
                                        'entity_type': entity_type,
                                        'is_merged': True,
                                        'primary_entity_name': primary_entity_name
                                    }
                            else:
                                self._add_fallback_entity_mapping(group, processed_entities)

                    else:
                        # Create new entity from group - process one group at a time
                        new_entity_id = await self._create_entity_from_group_single(
                            group, source_item_id
                        )
                        if new_entity_id:
                            stats["entities_created"] += 1
                            stats["entities_processed"] += len(group.items)

                            # Map all entity names to the primary entity name (not the individual item names)
                            # This ensures relations use consistent entity references
                            primary_name = new_entity_id  # This is the primary entity name

                            # Map the primary name to itself
                            processed_entities[primary_name] = {
                                'entity_id': primary_name,
                                'entity_type': entity_type
                            }

                            # Map all original entity names to the primary entity name
                            for item in group.items:
                                processed_entities[item.entity_name] = {
                                    'entity_id': primary_name,  # All point to the primary entity name
                                    'entity_type': entity_type,
                                    'is_alias': item.entity_name != primary_name,
                                    'primary_name': primary_name
                                }
                                logger.debug(f"📝 Mapped entity: {item.entity_name} -> {entity_type}:{primary_name}")

                            logger.info(f"✅ Created new entity {new_entity_id} from {len(group.items)} items")
                        else:
                            logger.error(f"❌ Failed to create entity from group {group.group_id}: create returned None")
                            # Don't add fallback mapping for failed entities as this causes relation failures

                except Exception as e:
                    logger.error(f"❌ Failed to process group {group.group_id}: {e}")
                    # Add fallback mapping for debugging
                    if group.primary_entity_id:
                        primary_entity_name = None
                        if group.primary_entity_data:
                            primary_entity_name = group.primary_entity_data.get('name')

                        if primary_entity_name:
                            processed_entities[primary_entity_name] = {
                                'entity_id': group.primary_entity_id,
                                'entity_type': entity_type
                            }

                        for item in group.items:
                            processed_entities[item.entity_name] = {
                                'entity_id': group.primary_entity_id,
                                'entity_type': entity_type,
                                'is_merged': True,
                                'primary_entity_name': primary_entity_name
                            }
                    else:
                        self._add_fallback_entity_mapping(group, processed_entities)

        return processed_entities, stats
    
    def _add_fallback_entity_mapping(self, group: EntityGroup, processed_entities: Dict[str, Dict[str, Any]]):
        """Add fallback entity mapping when creation/merge fails to allow relationship processing"""
        entity_type = group.entity_type
        
        for item in group.items:
            # Use the entity name as the fallback entity_id for relationship processing
            fallback_entity_id = item.entity_name
            processed_entities[item.entity_name] = {
                'entity_id': fallback_entity_id,
                'entity_type': entity_type
            }
            logger.debug(f"Added fallback mapping: {item.entity_name} -> {fallback_entity_id}")
    
    async def _merge_group_into_existing_single(self, group: EntityGroup, source_item_id: str) -> str:
        """
        Merge all items in group into existing primary entity - SINGLE ENTITY VERSION
        This version processes entities one at a time to avoid 413 payload errors
        """
        return await self._merge_group_into_existing(group, source_item_id)

    async def _merge_group_into_existing(self, group: EntityGroup, source_item_id: str) -> str:
        """Merge all items in group into existing primary entity"""
        
        primary_entity = group.primary_entity_data
        primary_entity_id = group.primary_entity_id
        
        # Collect all attributes from all items in group
        merged_attributes = {}
        
        # Initialize array fields from configuration, but only those that exist in the database schema
        try:
            array_fields = entity_config.get_entity_array_fields(group.entity_type)
            if array_fields is None:
                logger.warning(f"No array fields found for entity type {group.entity_type}, using empty list")
                array_fields = []
        except Exception as e:
            logger.error(f"Error getting array fields for {group.entity_type}: {e}")
            array_fields = []
        
        # Filter array fields to only those that exist in the database schema
        entity_schema = self.db_handler.entity_schemas.get(group.entity_type, {})
        valid_array_fields = []
        for field in array_fields:
            if field in entity_schema:
                field_schema = entity_schema.get(field, {})
                field_type_def = field_schema.get('type', '') if isinstance(field_schema, dict) else str(field_schema)
                if field_type_def.endswith('[]'):
                    valid_array_fields.append(field)
                else:
                    logger.debug(f"Skipping {group.entity_type}.{field} - not an array field in schema")
            else:
                logger.debug(f"Skipping {group.entity_type}.{field} - field not in database schema")
        
        for field in valid_array_fields:
            try:
                if primary_entity and isinstance(primary_entity, dict):
                    existing_value = primary_entity.get(field, [])
                    if existing_value is None:
                        existing_value = []
                    merged_attributes[field] = list(existing_value)
                else:
                    merged_attributes[field] = []
            except Exception as e:
                logger.error(f"Error initializing array field {field} for {group.entity_type}: {e}")
                merged_attributes[field] = []
        
        # Add source tracking
        if 'sources' in array_fields and source_item_id not in merged_attributes.get('sources', []):
            if 'sources' not in merged_attributes:
                merged_attributes['sources'] = []
            merged_attributes['sources'].append(source_item_id)
        
        # Get merge fields from configuration
        merge_fields = entity_config.get_systematic_merge_fields()
        string_fields = merge_fields.get('string_fields', ['name', 'email', 'worksAt', 'industry', 'domain', 'url'])
        config_array_fields = merge_fields.get('array_fields', array_fields)
        
        # Filter string fields to only those that exist in the entity schema
        entity_schema = self.db_handler.entity_schemas.get(group.entity_type, {})
        valid_string_fields = [field for field in string_fields if field in entity_schema]
        
        # Filter array fields to only those that exist in the entity schema
        valid_array_fields = [field for field in config_array_fields if field in entity_schema]
        
        # Merge attributes from all items
        for item in group.items:
            # Transform LLM attributes to database fields
            attrs = self._transform_attributes_for_database(group.entity_type, item.attributes)
            
            # Handle string fields - keep first non-empty value, add others to aliases
            for field in valid_string_fields:
                if field in attrs and attrs[field]:
                    if not merged_attributes.get(field):
                        # First value becomes the primary value
                        merged_attributes[field] = attrs[field]
                    else:
                        # Additional values go to aliases if they're different
                        if attrs[field] != merged_attributes[field]:
                            # Add to aliases if the entity schema supports aliases field
                            entity_schema = self.db_handler.entity_schemas.get(group.entity_type, {})
                            if 'aliases' in entity_schema:
                                if 'aliases' not in merged_attributes:
                                    merged_attributes['aliases'] = []
                                if attrs[field] not in merged_attributes['aliases']:
                                    merged_attributes['aliases'].append(attrs[field])
                
                # Handle array fields - append unique values
                for field in valid_array_fields:
                    if field in attrs and attrs[field]:
                        if field not in merged_attributes:
                            merged_attributes[field] = []
                        if isinstance(attrs[field], list):
                            for value in attrs[field]:
                                if value and value not in merged_attributes[field]:
                                    merged_attributes[field].append(value)
                        elif attrs[field] not in merged_attributes[field]:
                            merged_attributes[field].append(attrs[field])
                
                # Add descriptions using field mapping
                if 'description' in attrs and attrs['description']:
                    target_field = entity_config.get_target_field(group.entity_type, 'description')
                    if target_field not in merged_attributes:
                        merged_attributes[target_field] = []
                        
                    desc = attrs['description']
                    if isinstance(desc, list):
                        for d in desc:
                            if d and d not in merged_attributes[target_field]:
                                merged_attributes[target_field].append(d)
                    elif desc not in merged_attributes[target_field]:
                        merged_attributes[target_field].append(desc)
        
        # Remove primary key fields from updates as they cannot be changed
        update_attributes = merged_attributes.copy()
        # Primary key is name for all entities
        update_attributes.pop('name', None)
        
        # Generate embedding for updated entity if significant content has changed
        if update_attributes and any(field in update_attributes for field in ['name', 'rawDescriptions', 'title', 'description']):
            try:
                if self.inference_provider:
                    # Create combined entity data for embedding
                    combined_data = {**primary_entity, **update_attributes} if primary_entity else update_attributes
                    embedding = self.inference_provider.embed_entity(group.entity_type, combined_data)
                    if embedding:
                        update_attributes['embedding'] = embedding
            except Exception as e:
                logger.debug(f"Failed to generate embedding for entity {group.entity_type}:{primary_entity_id}: {e}")
        
        # Update the entity using the primary key field value
        result = await self.db_handler.update_entity(group.entity_type, primary_entity_id, update_attributes)
        if result:
            return primary_entity_id
        else:
            logger.error(f"Failed to update entity {primary_entity_id} in database")
            return None
    
    async def _create_entity_from_group_single(self, group: EntityGroup, source_item_id: str) -> str:
        """
        Create new entity by merging all items in group - SINGLE ENTITY VERSION
        This version processes entities one at a time to avoid 413 payload errors
        """
        return await self._create_entity_from_group(group, source_item_id)

    async def _create_entity_from_group(self, group: EntityGroup, source_item_id: str) -> str:
        """Create new entity by merging all items in group"""
        
        # Use first item as base
        base_item = group.items[0]
        
        # Transform LLM fields to database fields using entity configuration
        merged_attributes = self._transform_attributes_for_database(group.entity_type, base_item.attributes)
        
        # Generate entity ID and set the primary key field
        # For grouped entities, use the first item's name as primary and add others to aliases
        primary_entity_name = base_item.entity_name
        merged_attributes['name'] = primary_entity_name
        entity_id = primary_entity_name
        
        # Initialize aliases array if not exists
        if 'aliases' not in merged_attributes:
            merged_attributes['aliases'] = []
        elif not isinstance(merged_attributes['aliases'], list):
            merged_attributes['aliases'] = []
        
        # Add all other entity names to aliases
        for item in group.items[1:]:  # Skip the first item (primary)
            if item.entity_name != primary_entity_name and item.entity_name not in merged_attributes['aliases']:
                merged_attributes['aliases'].append(item.entity_name)
        
        logger.debug(f"🔑 Setting {group.entity_type} name: {entity_id}, aliases: {merged_attributes.get('aliases', [])}")
        
        # Initialize array fields from configuration
        try:
            array_fields = entity_config.get_entity_array_fields(group.entity_type)
            if array_fields is None:
                logger.warning(f"No array fields found for entity type {group.entity_type}, using empty list")
                array_fields = []
        except Exception as e:
            logger.error(f"Error getting array fields for {group.entity_type}: {e}")
            array_fields = []
        
        for field in array_fields:
            try:
                if field not in merged_attributes:
                    merged_attributes[field] = []
                elif not isinstance(merged_attributes[field], list):
                    merged_attributes[field] = [merged_attributes[field]] if merged_attributes[field] else []
            except Exception as e:
                logger.error(f"Error initializing array field {field} for {group.entity_type}: {e}")
                merged_attributes[field] = []
        
        # Add source tracking
        if 'sources' in array_fields and source_item_id not in merged_attributes.get('sources', []):
            if 'sources' not in merged_attributes:
                merged_attributes['sources'] = []
            merged_attributes['sources'].append(source_item_id)
        
        # Get merge fields from configuration
        merge_fields = entity_config.get_systematic_merge_fields()
        string_fields = merge_fields.get('string_fields', ['name', 'email', 'worksAt', 'industry', 'domain', 'url'])
        config_array_fields = merge_fields.get('array_fields', array_fields)
        
        # Filter string fields to only those that exist in the entity schema
        entity_schema = self.db_handler.entity_schemas.get(group.entity_type, {})
        valid_string_fields = [field for field in string_fields if field in entity_schema]
        
        # Filter array fields to only those that exist in the entity schema
        valid_array_fields = [field for field in config_array_fields if field in entity_schema]
        
        # Merge attributes from all items
        for item in group.items[1:]:  # Skip first item as it's the base
            # Transform LLM attributes to database fields
            attrs = self._transform_attributes_for_database(group.entity_type, item.attributes)
            
            # Handle string fields - keep first non-empty value, add others to aliases
            for field in valid_string_fields:
                if field in attrs and attrs[field]:
                    if not merged_attributes.get(field):
                        # First value becomes the primary value
                        merged_attributes[field] = attrs[field]
                    else:
                        # Additional values go to aliases if they're different
                        if attrs[field] != merged_attributes[field]:
                            # Add to aliases if the entity schema supports aliases field
                            entity_schema = self.db_handler.entity_schemas.get(group.entity_type, {})
                            if 'aliases' in entity_schema:
                                if 'aliases' not in merged_attributes:
                                    merged_attributes['aliases'] = []
                                if attrs[field] not in merged_attributes['aliases']:
                                    merged_attributes['aliases'].append(attrs[field])
            
            # Handle array fields - append unique values
            for field in config_array_fields:
                if field in attrs and attrs[field]:
                    if field not in merged_attributes:
                        merged_attributes[field] = []
                    if isinstance(attrs[field], list):
                        for value in attrs[field]:
                            if value and value not in merged_attributes[field]:
                                merged_attributes[field].append(value)
                    elif attrs[field] not in merged_attributes[field]:
                        merged_attributes[field].append(attrs[field])
            
            # Add descriptions using field mapping
            if 'description' in attrs and attrs['description']:
                target_field = entity_config.get_target_field(group.entity_type, 'description')
                if target_field not in merged_attributes:
                    merged_attributes[target_field] = []
                    
                desc = attrs['description']
                if isinstance(desc, list):
                    for d in desc:
                        if d and d not in merged_attributes[target_field]:
                            merged_attributes[target_field].append(d)
                elif desc not in merged_attributes[target_field]:
                    merged_attributes[target_field].append(desc)
        
        # Generate embedding for new entity
        try:
            if self.inference_provider:
                embedding = self.inference_provider.embed_entity(group.entity_type, merged_attributes)
                if embedding:
                    merged_attributes['embedding'] = embedding
        except Exception as e:
            logger.debug(f"Failed to generate embedding for entity {group.entity_type}:{entity_id}: {e}")
        
        # Create the entity
        logger.debug(f"🏗️ Creating entity {group.entity_type} with ID: {entity_id}")
        logger.debug(f"   Merged attributes: {merged_attributes}")
        
        try:
            result = await self.db_handler.create_entity(group.entity_type, merged_attributes)
            if result:
                logger.debug(f"✅ Successfully created entity {group.entity_type}:{entity_id}")
                return entity_id
            else:
                logger.error(f"❌ Failed to create entity {entity_id} in database - create_entity returned None")
                return None
        except Exception as e:
            logger.error(f"❌ Exception while creating entity {entity_id}: {e}")
            return None
    
    def _generate_entity_id(self, entity_type: str, attributes: Dict[str, Any]) -> str:
        """Generate consistent entity ID based on primary key field"""
        
        # All entities now use 'name' as primary key
        if 'name' in attributes and attributes['name']:
            return attributes['name']
        else:
            # Generate a fallback name
            if 'title' in attributes and attributes['title']:
                return attributes['title']
            elif 'email' in attributes and attributes['email']:
                return f"User_{attributes['email'].split('@')[0]}"
            else:
                return f"{entity_type}_{hash(str(sorted(attributes.items())))}"
    
    def _transform_attributes_for_database(self, entity_type: str, llm_attributes: Dict[str, Any]) -> Dict[str, Any]:
        """Transform LLM extracted attributes to database schema fields"""
        from workspace_kg.utils.entity_config import entity_config
        
        transformed = {}
        
        try:
            # Get all possible database fields for this entity type
            db_fields = entity_config.get_db_fields(entity_type)
            if not db_fields:
                # If no mapping exists, return attributes as-is
                logger.debug(f"No database field mappings found for entity type {entity_type}, using original attributes")
                return llm_attributes.copy()
            
            # Transform each LLM field to its corresponding database field(s)
            for llm_field, value in llm_attributes.items():
                if value is None:
                    continue
                
                try:
                    # Get target database field for this LLM field
                    target_field = entity_config.get_target_field(entity_type, llm_field)
                    
                    # Transform the value according to the field configuration
                    transformed_value = entity_config.transform_value(entity_type, llm_field, value, target_field)
                    
                    # Apply the transformed value to the target field
                    if target_field in db_fields:
                        field_config = db_fields[target_field]
                        if field_config:
                            field_type = field_config.get('type', 'STRING')
                            
                            if field_type.endswith('[]'):
                                # Array field - ensure value is a list
                                if not isinstance(transformed_value, list):
                                    transformed_value = [transformed_value] if transformed_value else []
                                
                                # Merge with existing values
                                if target_field in transformed:
                                    transformed[target_field].extend(transformed_value)
                                else:
                                    transformed[target_field] = transformed_value
                            else:
                                # Scalar field - take the value
                                transformed[target_field] = transformed_value
                    else:
                        # Field not in schema, keep original
                        transformed[llm_field] = value
                        
                except Exception as e:
                    logger.warning(f"Error transforming field {llm_field} for {entity_type}: {e}")
                    # Keep original field on error
                    transformed[llm_field] = value
            
            return transformed
            
        except Exception as e:
            logger.error(f"Error in field transformation for {entity_type}: {e}")
            # Fallback to original attributes
            return llm_attributes.copy()
    
    async def process_relations_systematic(self, relations_list: List[Dict[str, Any]], 
                                         entity_mapping: Dict[str, Dict[str, Any]], 
                                         source_item_id: str) -> int:
        """
        Process relations using the updated schema with array fields
        Groups relations with same source-target-type and merges their properties
        """
        
        # Log summary of entity mapping
        logger.info(f"🗂️ Processing relations with {len(entity_mapping)} entities in mapping")
        
        relations_processed = 0
        relation_groups = defaultdict(list)  # (source_id, target_id, type) -> [relations]
        
        # Step 1: Group relations by source-target-type using CANONICAL entity names
        for rel_data in relations_list:
            source_name = rel_data.get('source_entity') or rel_data.get('source')
            target_name = rel_data.get('target_entity') or rel_data.get('target')
            rel_type = rel_data.get('relationship_type') or rel_data.get('type')
            
            if not source_name or not target_name or not rel_type:
                continue
            
            if source_name not in entity_mapping or target_name not in entity_mapping:
                missing_entities = []
                if source_name not in entity_mapping:
                    missing_entities.append(f"source '{source_name}'")
                if target_name not in entity_mapping:
                    missing_entities.append(f"target '{target_name}'")
                
                logger.warning(f"Skipping relation {source_name} -> {target_name} ({rel_type}): {', '.join(missing_entities)} not in entity mapping")
                
                # Show some entity names to help debug
                available_entities = list(entity_mapping.keys())[:5]  # Show first 5
                logger.debug(f"Sample available entities: {available_entities}...")
                continue
            
            # CRITICAL FIX: Use canonical entity names for grouping to prevent duplicates
            source_info = entity_mapping[source_name]
            target_info = entity_mapping[target_name]
            
            # Get the canonical entity names (primary names) for relation grouping
            canonical_source_name = source_info.get('primary_name', source_info['entity_id'])
            canonical_target_name = target_info.get('primary_name', target_info['entity_id'])
            
            # Log the mapping for debugging
            if canonical_source_name != source_name:
                logger.debug(f"🔄 Mapping source: {source_name} -> {canonical_source_name}")
            if canonical_target_name != target_name:
                logger.debug(f"🔄 Mapping target: {target_name} -> {canonical_target_name}")
            
            # Use canonical names for grouping to ensure relations with same canonical entities are grouped together
            group_key = (canonical_source_name, canonical_target_name, rel_type)
            
            # Store the original relation data with canonical names for processing
            rel_data_with_canonical = rel_data.copy()
            rel_data_with_canonical['canonical_source'] = canonical_source_name
            rel_data_with_canonical['canonical_target'] = canonical_target_name
            rel_data_with_canonical['original_source'] = source_name
            rel_data_with_canonical['original_target'] = target_name
            
            relation_groups[group_key].append(rel_data_with_canonical)
        
        # Step 2: Process each relation group using canonical names
        for (canonical_source_name, canonical_target_name, rel_type), relations in relation_groups.items():
            # Generate relation ID using canonical names to ensure uniqueness
            relation_id = self._generate_relation_id(canonical_source_name, canonical_target_name, rel_type)
            
            # Merge all relation data
            merged_descriptions = []
            merged_relation_tags = []
            merged_permissions = []
            merged_sources = [source_item_id]
            max_strength = 0.0
            
            for rel in relations:
                # Collect descriptions
                desc = rel.get('description', '')
                if desc and desc not in merged_descriptions:
                    merged_descriptions.append(desc)
                
                # Collect relation tags
                tag = rel.get('relationship_type') or rel.get('type', '')
                if tag and tag not in merged_relation_tags:
                    merged_relation_tags.append(tag)
                
                # Collect permissions
                perms = rel.get('permissions', [])
                if isinstance(perms, list):
                    for perm in perms:
                        if perm and perm not in merged_permissions:
                            merged_permissions.append(perm)
                elif perms and perms not in merged_permissions:
                    merged_permissions.append(perms)
                
                # Max strength
                strength = rel.get('strength', 1.0)
                max_strength = max(max_strength, float(strength))
            
            # Check if relation exists
            existing_relation = await self.db_handler.get_relation(relation_id)
            
            # Generate embedding for the relation
            relation_embedding = None
            if self.inference_provider:
                try:
                    # Create relation data dictionary for embedding
                    relation_data_for_embedding = {
                        "type": rel_type,
                        "relationTag": merged_relation_tags,
                        "description": merged_descriptions,
                        "strength": max_strength
                    }
                    
                    # Generate embedding using the inference provider's embed_relation method
                    relation_embedding = self.inference_provider.embed_relation(relation_data_for_embedding)
                    if relation_embedding:
                        logger.debug(f"Generated embedding for relation {canonical_source_name} -> {canonical_target_name} ({rel_type})")
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for relation {canonical_source_name} -> {canonical_target_name}: {e}")
            
            relation_properties = {
                "relation_id": relation_id,
                "description": merged_descriptions,
                "relationTag": merged_relation_tags,
                "type": rel_type,
                "strength": max_strength,
                "permissions": merged_permissions,
                "sources": merged_sources,
                "createdAt": existing_relation.get('createdAt') if existing_relation else "",
                "lastUpdated": "",
                "embedding": relation_embedding if relation_embedding else []
            }
            
            if existing_relation:
                # Merge with existing relation
                existing_descriptions = existing_relation.get('description', [])
                existing_tags = existing_relation.get('relationTag', [])
                existing_permissions = existing_relation.get('permissions', [])
                existing_sources = existing_relation.get('sources', [])
                
                # Merge arrays
                for desc in merged_descriptions:
                    if desc not in existing_descriptions:
                        existing_descriptions.append(desc)
                
                for tag in merged_relation_tags:
                    if tag not in existing_tags:
                        existing_tags.append(tag)
                
                for perm in merged_permissions:
                    if perm not in existing_permissions:
                        existing_permissions.append(perm)
                
                if source_item_id not in existing_sources:
                    existing_sources.append(source_item_id)
                
                updates = {
                    "description": existing_descriptions,
                    "relationTag": existing_tags,
                    "permissions": existing_permissions,
                    "sources": existing_sources,
                    "strength": max(existing_relation.get('strength', 0), max_strength)
                }
                
                # Generate embedding for updated relation if significant content has changed
                if any(field in updates for field in ['description', 'relationTag', 'strength']):
                    try:
                        if self.inference_provider:
                            # Create combined relation data for embedding
                            updated_relation_data = {
                                "type": rel_type,
                                "relationTag": existing_tags,
                                "description": existing_descriptions,
                                "strength": updates['strength']
                            }
                            
                            # Generate new embedding
                            relation_embedding = self.inference_provider.embed_relation(updated_relation_data)
                            if relation_embedding:
                                updates['embedding'] = relation_embedding
                                logger.debug(f"Updated embedding for relation {canonical_source_name} -> {canonical_target_name} ({rel_type})")
                    except Exception as e:
                        logger.warning(f"Failed to generate embedding for updated relation {canonical_source_name} -> {canonical_target_name}: {e}")
                
                await self.db_handler.update_relation(relation_id, updates)
                relations_processed += 1
            else:
                # Create new relation using canonical names
                # Get entity info from the first relation in the group (they all have same canonical entities)
                first_relation = relations[0]
                original_source_name = first_relation['original_source']
                original_target_name = first_relation['original_target']
                
                source_info = entity_mapping[original_source_name]
                target_info = entity_mapping[original_target_name]
                source_type = source_info['entity_type']
                target_type = target_info['entity_type']
                
                # Use the canonical names directly (already computed in grouping step)
                source_id = canonical_source_name
                target_id = canonical_target_name
                
                logger.debug(f"🔗 Creating relation with canonical names: {source_id} -> {target_id} ({rel_type})")
                logger.debug(f"   Grouped {len(relations)} relations from original entities")
                for rel in relations:
                    logger.debug(f"     {rel['original_source']} -> {rel['original_target']}")
                
                # Validate that both entities exist in the database before creating relation
                try:
                    # Use canonical names for entity lookup
                    source_lookup_id = canonical_source_name
                    target_lookup_id = canonical_target_name
                    
                    # Add debug logging for troubleshooting
                    logger.debug(f"🔍 Validating entities for relation {canonical_source_name} -> {canonical_target_name}")
                    logger.debug(f"   Source: {source_type}:{source_lookup_id}")
                    logger.debug(f"   Target: {target_type}:{target_lookup_id}")
                    
                    # Try entity lookup with error handling
                    source_exists = None
                    target_exists = None
                    
                    try:
                        source_exists = await self.db_handler.get_entity(source_type, source_lookup_id)
                    except Exception as e:
                        logger.warning(f"❌ Error looking up source entity {source_type}:{source_lookup_id}: {e}")
                    
                    try:
                        target_exists = await self.db_handler.get_entity(target_type, target_lookup_id)
                    except Exception as e:
                        logger.warning(f"❌ Error looking up target entity {target_type}:{target_lookup_id}: {e}")
                    
                    # Skip validation if entity lookup fails - proceed with relation creation
                    # This handles cases where entities were just created and might not be immediately available
                    if source_exists is False:  # Explicitly False, not None (which indicates lookup error)
                        logger.warning(f"❌ Skipping relation: source entity {canonical_source_name} ({source_lookup_id}) does not exist in database")
                        continue
                    
                    if target_exists is False:  # Explicitly False, not None (which indicates lookup error)
                        logger.warning(f"❌ Skipping relation: target entity {canonical_target_name} ({target_lookup_id}) does not exist in database")
                        continue
                    
                    # Create the relation
                    logger.debug(f"🔗 Creating relation: {source_type}:{source_id} -> {target_type}:{target_id} ({rel_type})")
                    
                    result = await self.db_handler.create_relation(
                        source_type, source_id, target_type, target_id, relation_properties
                    )
                    if result:
                        relations_processed += 1
                        logger.debug(f"✅ Created relation: {canonical_source_name} -> {canonical_target_name} ({rel_type})")
                        logger.debug(f"   Consolidated {len(relations)} duplicate relations")
                    else:
                        logger.warning(f"❌ Failed to create relation: {canonical_source_name} -> {canonical_target_name} ({rel_type})")
                        logger.warning(f"   Original entities: {[f"{r['original_source']} -> {r['original_target']}" for r in relations]}")
                except Exception as e:
                    logger.error(f"❌ Error creating relation {canonical_source_name} -> {canonical_target_name}: {e}")
                    logger.error(f"   Original entities: {[f"{r['original_source']} -> {r['original_target']}" for r in relations]}")
                    # Continue processing other relations even if this one fails
                    continue
        
        logger.info(f"✅ Processed {relations_processed} unique relations from {len(relations_list)} raw relations")
        return relations_processed
    
    def _generate_relation_id(self, source_id: str, target_id: str, rel_type: str) -> str:
        """Generate consistent relation ID"""
        unique_str = f"{source_id}::{rel_type}::{target_id}"
        return hashlib.sha256(unique_str.encode('utf-8')).hexdigest()
