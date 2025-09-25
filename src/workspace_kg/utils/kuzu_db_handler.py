import asyncio
import json
import httpx
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
import logging
import yaml
import os

logger = logging.getLogger(__name__)

class KuzuDBHandler:
    def __init__(self, api_url: str = "http://localhost:7000", schema_file: str = 'schema.yaml'):
        self.api_url = api_url
        # Disable httpx logging
        httpx_logger = logging.getLogger("httpx")
        httpx_logger.setLevel(logging.WARNING)
        self.client = httpx.AsyncClient(base_url=api_url, timeout=30.0)
        self.schema_file = schema_file
        self.entity_schemas: Dict[str, Any] = {}
        self.relationship_schemas: Dict[str, Any] = {}
        self._load_schema()

    def _load_schema(self):
        """Load schema from the YAML file."""
        if not os.path.exists(self.schema_file):
            logger.error(f"Schema file not found at {self.schema_file}")
            return
        try:
            with open(self.schema_file, 'r') as f:
                schema_data = yaml.safe_load(f)
            self.entity_schemas, self.relationship_schemas = self._separate_schemas(schema_data)
            logger.info(f"Schema loaded successfully from {self.schema_file}")
        except yaml.YAMLError as e:
            logger.error(f"Error loading YAML schema from {self.schema_file}: {e}")

    def _separate_schemas(self, schema_data: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """Separate entity and relationship schemas from the loaded YAML data."""
        entity_schemas = {}
        relationship_schemas = {}
        
        # Known relationship types - add more as needed
        relationship_types = {"Relation"}
        
        for schema_name, schema_def in schema_data.items():
            if schema_name in relationship_types:
                relationship_schemas[schema_name] = schema_def
            else:
                entity_schemas[schema_name] = schema_def
        
        # If no relationship schemas found, use default
        if not relationship_schemas:
            relationship_schemas = {
                "Relation": {
                    "relation_id": "STRING PRIMARY KEY",
                    "description": "STRING",
                    "relationTag": "STRING",
                    "type": "STRING",
                    "strength": "FLOAT",
                    "sources": "STRING[]",
                    "createdAt": "TIMESTAMP",
                    "lastUpdated": "TIMESTAMP",
                    "embedding": "DOUBLE[]"
                }
            }
        
        return entity_schemas, relationship_schemas

    async def execute_cypher(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute a Cypher query against Kuzu API"""
        payload = {"query": query}
        if params:
            payload["params"] = params
        
        try:
            response = await self.client.post("/cypher", json=payload)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"Kuzu API error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

    def _validate_and_filter_properties(self, entity_type: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and filter properties against the schema."""
        if entity_type not in self.entity_schemas:
            logger.error(f"Unknown entity type: {entity_type}")
            return {}
        
        schema = self.entity_schemas[entity_type]
        validated_props = {}
        
        for key, value in properties.items():
            if key in schema:
                validated_props[key] = value
            else:
                logger.warning(f"Property '{key}' not found in schema for {entity_type}, skipping")
        
        return validated_props

    async def create_entity(self, entity_type: str, properties: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a new entity (node) in the database.
        Properties must include 'entity_id'.
        """
        if entity_type not in self.entity_schemas:
            logger.error(f"Unknown entity type: {entity_type}")
            return None
        
        # All entity types now use 'name' as primary key
        primary_key_field = 'name'
        
        if primary_key_field not in properties:
            logger.error(f"Missing '{primary_key_field}' for entity type {entity_type}")
            return None

        # Validate properties against schema
        validated_properties = self._validate_and_filter_properties(entity_type, properties)
        
        if not validated_properties:
            logger.error(f"No valid properties for entity type {entity_type}")
            return None

        # Ensure rawDescriptions is an array
        if 'rawDescriptions' in validated_properties and not isinstance(validated_properties['rawDescriptions'], list):
            validated_properties['rawDescriptions'] = [validated_properties['rawDescriptions']]
        elif 'rawDescriptions' not in validated_properties:
            validated_properties['rawDescriptions'] = []

        # Ensure sources is an array only if entity type supports it
        if entity_type in self.entity_schemas and 'sources' in self.entity_schemas[entity_type]:
            if 'sources' in validated_properties and not isinstance(validated_properties['sources'], list):
                validated_properties['sources'] = [validated_properties['sources']]
            elif 'sources' not in validated_properties:
                validated_properties['sources'] = []

        # Ensure other array fields are arrays
        array_fields = ['role', 'aliases', 'location', 'tags', 'reviewers', 'assignees', 'labels', 'keywords', 'relatedThreads']
        for field in array_fields:
            if field in validated_properties and not isinstance(validated_properties[field], list):
                validated_properties[field] = [validated_properties[field]]

        # Remove timestamp fields - will be handled automatically
        if 'lastUpdated' in validated_properties:
            del validated_properties['lastUpdated']
        if 'createdAt' in validated_properties:
            del validated_properties['createdAt']

        # Generate current timestamp
        current_time = datetime.now(timezone.utc).isoformat()

        # For CREATE, don't include primary key field in SET clause since it's used in MERGE
        create_set_clauses = []
        match_set_clauses = []
        params = {}
        
        for key, value in validated_properties.items():
            params[key] = value
            if key != primary_key_field:
                create_set_clauses.append(f"n.{key} = ${key}")
                if key not in ['rawDescriptions', 'sources']:
                    match_set_clauses.append(f"n.{key} = ${key}")
        
        # Add timestamp to both CREATE and MATCH
        create_set_clauses.append(f"n.lastUpdated = $current_time")
        match_set_clauses.append(f"n.lastUpdated = $current_time")
        params['current_time'] = current_time
        
        create_set_str = ", ".join(create_set_clauses)
        match_set_str = ", ".join(match_set_clauses)
        
        if match_set_str:
            match_set_str += ", "
        
        # Build array update clauses based on entity schema
        array_updates = ["n.rawDescriptions = n.rawDescriptions + $rawDescriptions"]
        if entity_type in self.entity_schemas and 'sources' in self.entity_schemas[entity_type]:
            array_updates.append("n.sources = n.sources + $sources")
        
        query = f"""
        MERGE (n:{entity_type} {{{primary_key_field}: ${primary_key_field}}})
        ON CREATE SET {create_set_str}
        ON MATCH SET {match_set_str}{", ".join(array_updates)}
        RETURN n
        """
        
        try:
            logger.debug(f"Executing query: {query}")
            logger.debug(f"With params: {params}")
            result = await self.execute_cypher(query, params)
            logger.debug(f"Query result: {result}")
            if result and (result.get('data') or result.get('rows')):
                # Handle both response formats
                data = result.get('data') or result.get('rows')
                if data:
                    # logger.info(f"Entity {entity_type}:{validated_properties[primary_key_field]} created/updated.")
                    return data[0]['n']
            logger.warning(f"No data returned from query for entity {entity_type}:{validated_properties[primary_key_field]}")
            return None
        except Exception as e:
            logger.error(f"Failed to create/update entity {entity_type}:{validated_properties[primary_key_field]}: {e}")
            return None

    async def get_entity(self, entity_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve an entity by its type and ID."""
        # All entity types now use 'name' as primary key
        primary_key_field = 'name'
        
        query = f"MATCH (n:{entity_type} {{{primary_key_field}: $entity_id}}) RETURN n"
        params = {"entity_id": entity_id}
        try:
            result = await self.execute_cypher(query, params)
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                if data:
                    return data[0]['n']
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve entity {entity_type}:{entity_id}: {e}")
            return None

    async def update_entity(self, entity_type: str, entity_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update properties of an existing entity."""
        if not updates:
            return await self.get_entity(entity_type, entity_id)

        # All entity types now use 'name' as primary key
        primary_key_field = 'name'

        # Ensure rawDescriptions is handled as an array append
        if 'rawDescriptions' in updates:
            if not isinstance(updates['rawDescriptions'], list):
                updates['rawDescriptions'] = [updates['rawDescriptions']]
            
            # Remove from updates to handle separately in query
            raw_descriptions_to_add = updates.pop('rawDescriptions')
        else:
            raw_descriptions_to_add = []

        # Remove lastUpdated - will handle automatically
        if 'lastUpdated' in updates:
            del updates['lastUpdated']

        # Generate current timestamp
        current_time = datetime.now(timezone.utc).isoformat()

        set_clauses = []
        params = {"entity_id": entity_id, "current_time": current_time}
        for key, value in updates.items():
            set_clauses.append(f"n.{key} = ${key}")
            params[key] = value
        
        # Always add lastUpdated timestamp
        set_clauses.append("n.lastUpdated = $current_time")
        set_clause_str = ", ".join(set_clauses)

        raw_desc_update_clause = ""
        if raw_descriptions_to_add:
            params['rawDescriptionsToAdd'] = raw_descriptions_to_add
            raw_desc_update_clause = ", n.rawDescriptions = n.rawDescriptions + $rawDescriptionsToAdd"

        query = f"""
        MATCH (n:{entity_type} {{{primary_key_field}: $entity_id}})
        SET {set_clause_str}{raw_desc_update_clause}
        RETURN n
        """
        
        try:
            result = await self.execute_cypher(query, params)
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                if data:
                    # logger.info(f"Entity {entity_type}:{entity_id} updated.")
                    return data[0]['n']
            return None
        except Exception as e:
            logger.error(f"Failed to update entity {entity_type}:{entity_id}: {e}")
            return None

    async def delete_entity(self, entity_type: str, entity_id: str) -> bool:
        """Delete an entity and its associated relationships."""
        # All entity types now use 'name' as primary key
        primary_key_field = 'name'
            
        query = f"MATCH (n:{entity_type} {{{primary_key_field}: $entity_id}}) DETACH DELETE n"
        params = {"entity_id": entity_id}
        try:
            await self.execute_cypher(query, params)
            # logger.info(f"Entity {entity_type}:{entity_id} deleted.")
            return True
        except Exception as e:
            logger.error(f"Failed to delete entity {entity_type}:{entity_id}: {e}")
            return False

    async def create_relation(self, 
                              from_entity_type: str, 
                              from_entity_id: str, 
                              to_entity_type: str, 
                              to_entity_id: str, 
                              relation_properties: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create a generic Relation node and connect it to two entities.
        relation_properties must include 'relation_id' and 'relationTag'.
        """
        if 'relation_id' not in relation_properties or 'relationTag' not in relation_properties:
            logger.error("Missing 'relation_id' or 'relationTag' for relation creation.")
            return None

        # Ensure sources is an array
        if 'sources' in relation_properties and not isinstance(relation_properties['sources'], list):
            relation_properties['sources'] = [relation_properties['sources']]
        elif 'sources' not in relation_properties:
            relation_properties['sources'] = []

        # Remove timestamp properties - will handle automatically
        if 'createdAt' in relation_properties:
            del relation_properties['createdAt']
        if 'lastUpdated' in relation_properties:
            del relation_properties['lastUpdated']

        # Generate current timestamp
        current_time = datetime.now(timezone.utc).isoformat()

        # Construct SET clause for relation properties
        create_set_clauses = []
        match_set_clauses = []
        params = {
            "from_entity_id": from_entity_id,
            "to_entity_id": to_entity_id,
            "relation_id": relation_properties['relation_id'],
            "current_time": current_time
        }
        for key, value in relation_properties.items():
            params[key] = value
            if key != 'relation_id':  # Don't set the primary key
                create_set_clauses.append(f"r.{key} = ${key}")
                if key not in ['lastUpdated', 'sources']:
                    match_set_clauses.append(f"r.{key} = ${key}")
        
        # Add timestamp to both CREATE and MATCH
        create_set_clauses.append("r.lastUpdated = $current_time")
        match_set_clauses.append("r.lastUpdated = $current_time")
        
        create_set_str = ", ".join(create_set_clauses)
        match_set_str = ", ".join(match_set_clauses)
        
        if match_set_str:
            match_set_str += ", "

        # Determine primary key fields for source and target entities
        # All entities now use 'name' as primary key
        from_primary_key = 'name'
        to_primary_key = 'name'
        
        query = f"""
        MATCH (a:{from_entity_type} {{{from_primary_key}: $from_entity_id}}),
              (b:{to_entity_type} {{{to_primary_key}: $to_entity_id}})
        MERGE (a)-[r:Relation {{relation_id: $relation_id}}]->(b)
        ON CREATE SET {create_set_str}
        ON MATCH SET {match_set_str}r.sources = r.sources + $sources
        RETURN r
        """
        
        try:
            result = await self.execute_cypher(query, params)
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                if data:
                    # logger.info(f"Relation {relation_properties['relation_id']} created/updated between {from_entity_id} and {to_entity_id}.")
                    return data[0]['r']
            return None
        except Exception as e:
            logger.error(f"Failed to create/update relation {relation_properties['relation_id']}: {e}")
            return None

    async def get_relation(self, relation_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a relation by its ID."""
        query = f"MATCH ()-[r:Relation]->() WHERE r.relation_id = $relation_id RETURN r"
        params = {"relation_id": relation_id}
        try:
            result = await self.execute_cypher(query, params)
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                if data:
                    return data[0]['r']
            return None
        except Exception as e:
            logger.error(f"Failed to retrieve relation {relation_id}: {e}")
            return None

    async def get_relations_between_entities(self, 
                                             from_entity_type: str, 
                                             from_entity_id: str, 
                                             to_entity_type: str, 
                                             to_entity_id: str,
                                             relation_tag: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Retrieve generic Relation nodes between two specific entities, optionally filtered by relationTag.
        This query uses the intermediary HAS_RELATION and RELATES_TO edges as per the schema.
        """
        # Determine primary key fields for source and target entities
        # All entities now use 'name' as primary key
        from_primary_key = 'name'
        to_primary_key = 'name'
        
        query_parts = [
            f"MATCH (a:{from_entity_type} {{{from_primary_key}: $from_entity_id}})",
            f"MATCH (b:{to_entity_type} {{{to_primary_key}: $to_entity_id}})",
            f"MATCH (a)-[r:Relation]->(b)"
        ]
        params = {
            "from_entity_id": from_entity_id,
            "to_entity_id": to_entity_id
        }

        if relation_tag:
            query_parts.append("WHERE r.relationTag = $relation_tag")
            params["relation_tag"] = relation_tag
        
        query_parts.append("RETURN r")
        query = "\n".join(query_parts)

        try:
            result = await self.execute_cypher(query, params)
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                if data:
                    return [item['r'] for item in data]
            return []
        except Exception as e:
            logger.error(f"Failed to retrieve relations between {from_entity_id} and {to_entity_id}: {e}")
            return []

    async def update_relation(self, relation_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update properties of an existing relation."""
        if not updates:
            return await self.get_relation(relation_id)

        # Ensure sources is handled as an array append
        if 'sources' in updates:
            if not isinstance(updates['sources'], list):
                updates['sources'] = [updates['sources']]
            
            # Remove from updates to handle separately in query
            sources_to_add = updates.pop('sources')
        else:
            sources_to_add = []

        # Remove timestamp fields - will be handled automatically
        if 'lastUpdated' in updates:
            del updates['lastUpdated']
        if 'createdAt' in updates:
            del updates['createdAt']

        # Generate current timestamp
        current_time = datetime.now(timezone.utc).isoformat()

        set_clauses = []
        params = {"relation_id": relation_id, "current_time": current_time}
        for key, value in updates.items():
            set_clauses.append(f"r.{key} = ${key}")
            params[key] = value
        
        # Always add lastUpdated timestamp
        set_clauses.append("r.lastUpdated = $current_time")
        set_clause_str = ", ".join(set_clauses)

        sources_update_clause = ""
        if sources_to_add:
            params['sourcesToAdd'] = sources_to_add
            sources_update_clause = ", r.sources = r.sources + $sourcesToAdd"

        set_part = f"{set_clause_str}{sources_update_clause}"
        query = f"""
        MATCH ()-[r:Relation]->()
        WHERE r.relation_id = $relation_id
        SET {set_part}
        RETURN r
        """
        
        try:
            result = await self.execute_cypher(query, params)
            if result and (result.get('data') or result.get('rows')):
                data = result.get('data') or result.get('rows')
                if data:
                    # logger.info(f"Relation {relation_id} updated.")
                    return data[0]['r']
            return None
        except Exception as e:
            logger.error(f"Failed to update relation {relation_id}: {e}")
            return None

    async def delete_relation(self, relation_id: str) -> bool:
        """Delete a relation by its ID."""
        query = f"MATCH ()-[r:Relation]->() WHERE r.relation_id = $relation_id DELETE r"
        params = {"relation_id": relation_id}
        try:
            await self.execute_cypher(query, params)
            # logger.info(f"Relation {relation_id} deleted.")
            return True
        except Exception as e:
            logger.error(f"Failed to delete relation {relation_id}: {e}")
            return False
