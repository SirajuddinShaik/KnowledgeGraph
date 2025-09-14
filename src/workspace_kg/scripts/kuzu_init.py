#!/usr/bin/env python3
"""
Kuzu DB Schema Initialization Script
Uses centralized schema from kuzudb_schema.py for database management
"""

import asyncio
import json
import httpx
from typing import Dict, Any, List
from datetime import datetime
import logging
import yaml # Added yaml import
import os # Added os import

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kuzu DB Configuration
KUZU_API_URL = "http://localhost:7000"
SCHEMA_FILE = 'schema.yaml' # Path to schema.yaml

class KuzuSchemaManager:
    def __init__(self, api_url: str = KUZU_API_URL):
        self.api_url = api_url
        self.client = httpx.AsyncClient(base_url=api_url, timeout=30.0)
        schema_data = self._load_schema_from_yaml()
        self.entity_schemas, self.relationship_schemas = self._separate_schemas(schema_data)
        self.relationship_types = list(self.relationship_schemas.keys())
        self.schema_metadata = { # Placeholder for schema metadata
            "version": "1.0",
            "last_updated": datetime.now().isoformat()
        }
    
    def _load_schema_from_yaml(self) -> Dict[str, Any]:
        """Load schema from the YAML file."""
        if not os.path.exists(SCHEMA_FILE):
            logger.error(f"Schema file not found at {SCHEMA_FILE}")
            return {}
        try:
            with open(SCHEMA_FILE, 'r') as f:
                schema = yaml.safe_load(f)
            logger.info(f"Schema loaded successfully from {SCHEMA_FILE}")
            return schema
        except yaml.YAMLError as e:
            logger.error(f"Error loading YAML schema from {SCHEMA_FILE}: {e}")
            return {}

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
                    "type": "STRING",
                    "timestamp": "TIMESTAMP"
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
    
    def _generate_node_table_query(self, entity_type: str, attributes: Dict[str, str]) -> str:
        """Generate CREATE NODE TABLE query from schema"""
        attr_definitions = []
        
        for attr_name, attr_type in attributes.items():
            attr_definitions.append(f"{attr_name} {attr_type}")
        
        attributes_str = ",\n            ".join(attr_definitions)
        
        return f"""
        CREATE NODE TABLE IF NOT EXISTS {entity_type}(
            {attributes_str}
        )
        """
    
    def _generate_relationship_table_query(self) -> str:
        """Generate comprehensive relationship table for all entity types"""
        entity_types = list(self.entity_schemas.keys())
        
        # Generate all possible FROM-TO combinations
        from_to_combinations = []
        for from_type in entity_types:
            for to_type in entity_types:
                from_to_combinations.append(f"FROM {from_type} TO {to_type}")
        
        from_to_str = ",\n            ".join(from_to_combinations)
        
        # Get relationship attributes
        rel_attributes = self.relationship_schemas["Relation"]
        attr_definitions = []
        for attr_name, attr_type in rel_attributes.items():
            attr_definitions.append(f"{attr_name} {attr_type}")
        
        attributes_str = ",\n            " + ",\n            ".join(attr_definitions)
        
        return f"""
        CREATE REL TABLE IF NOT EXISTS Relation(
            {from_to_str}{attributes_str}
        )
        """
    
    # ==================== DATABASE MANAGEMENT ====================
    
    async def validate_connection(self) -> bool:
        """Test connection to KuzuDB"""
        try:
            response = await self.client.get("/")
            logger.info(f"✅ Connected to KuzuDB: {response.json()}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to KuzuDB: {e}")
            return False
    
    async def get_database_info(self) -> Dict[str, Any]:
        """Get database information and statistics"""
        try:
            info = {
                "schema_version": self.schema_metadata["version"],
                "schema_last_updated": self.schema_metadata["last_updated"],
                "total_entity_types": len(self.entity_schemas),
                "total_relationship_types": len(self.relationship_types),
                "entity_types": list(self.entity_schemas.keys()),
                "relationship_types": self.relationship_types
            }
            
            # Get table counts
            table_counts = {}
            for entity_type in self.entity_schemas.keys():
                try:
                    query = f"MATCH (n:{entity_type}) RETURN count(n) as count"
                    result = await self.execute_cypher(query)
                    count = result.get('data', [{}])[0].get('count', 0) if result.get('data') else 0
                    table_counts[f"{entity_type}_count"] = count
                except:
                    table_counts[f"{entity_type}_count"] = 0
            
            # Get relationship count
            try:
                query = "MATCH ()-[r:Relation]->() RETURN count(r) as count"
                result = await self.execute_cypher(query)
                table_counts["relationship_count"] = result.get('data', [{}])[0].get('count', 0) if result.get('data') else 0
            except:
                table_counts["relationship_count"] = 0
            
            info.update(table_counts)
            return info
            
        except Exception as e:
            logger.error(f"Error getting database info: {e}")
            return {}
    
    async def clean_database(self) -> bool:
        """Clean all data but keep schema"""
        try:
            logger.info("🧹 Cleaning database data...")
            
            # Delete all relationships first
            await self.execute_cypher("MATCH ()-[r]->() DELETE r")
            logger.info("  - Deleted all relationships")
            
            # Delete all nodes
            for entity_type in self.entity_schemas.keys():
                try:
                    await self.execute_cypher(f"MATCH (n:{entity_type}) DELETE n")
                    logger.info(f"  - Deleted all {entity_type} nodes")
                except Exception as e:
                    logger.warning(f"  - Could not delete {entity_type} nodes: {e}")
            
            logger.info("✅ Database cleaned successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to clean database: {e}")
            return False
    
    async def drop_all_tables(self) -> bool:
        """Drop all tables (complete schema reset)"""
        try:
            logger.info("🗑️ Dropping all tables...")
            
            # First drop relationship tables (including any that might exist)
            for rel_table in self.relationship_types:
                try:
                    await self.execute_cypher(f"DROP TABLE {rel_table}")
                    logger.info(f"  - Dropped {rel_table} table")
                except Exception as e:
                    logger.debug(f"  - {rel_table} table not found: {e}")
            
            # Then drop all node tables
            for entity_type in self.entity_schemas.keys():
                try:
                    await self.execute_cypher(f"DROP TABLE {entity_type}")
                    logger.info(f"  - Dropped {entity_type} table")
                except Exception as e:
                    logger.debug(f"  - {entity_type} table not found: {e}")
            
            logger.info("✅ All tables dropped successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to drop tables: {e}")
            return False
    
    async def create_schema(self) -> bool:
        """Create database schema from centralized definitions"""
        try:
            logger.info("🚀 Creating database schema...")
            
            # Create node tables
            logger.info("📋 Creating node tables...")
            for entity_type, attributes in self.entity_schemas.items():
                query = self._generate_node_table_query(entity_type, attributes)
                await self.execute_cypher(query)
                logger.info(f"  ✅ Created {entity_type} table")
            
            # Create relationship table
            logger.info("🔗 Creating relationship table...")
            rel_query = self._generate_relationship_table_query()
            await self.execute_cypher(rel_query)
            logger.info("  ✅ Created Relation table")
            
            logger.info("✅ Schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema creation failed: {e}")
            return False
    
    async def migrate_schema(self, clean_first: bool = True) -> bool:
        """Complete schema migration (drop + create)"""
        try:
            logger.info("🔄 Starting schema migration...")
            
            if clean_first:
                await self.drop_all_tables()
            
            success = await self.create_schema()
            
            if success:
                logger.info("✅ Schema migration completed successfully")
            else:
                logger.error("❌ Schema migration failed")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Schema migration failed: {e}")
            return False
    
    async def backup_schema(self, filename: str = None) -> bool:
        """Backup current schema to JSON file"""
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"kuzu_schema_backup_{timestamp}.json"
            
            backup_data = {
                "timestamp": datetime.now().isoformat(),
                "schema_metadata": self.schema_metadata,
                "entity_schemas": self.entity_schemas,
                "relationship_schemas": self.relationship_schemas,
                "relationship_types": self.relationship_types
            }
            
            with open(filename, 'w') as f:
                json.dump(backup_data, f, indent=2)
            
            logger.info(f"✅ Schema backed up to {filename}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Schema backup failed: {e}")
            return False
    
    async def list_tables(self) -> List[str]:
        """List all existing tables in the database"""
        try:
            # This is a simplified approach - KuzuDB might have different syntax
            tables = []
            
            # Try to query each expected table
            for entity_type in self.entity_schemas.keys():
                try:
                    await self.execute_cypher(f"MATCH (n:{entity_type}) RETURN count(n) LIMIT 1")
                    tables.append(entity_type)
                except:
                    pass
            
            # Check for relationship table
            try:
                await self.execute_cypher("MATCH ()-[r:Relation]->() RETURN count(r) LIMIT 1")
                tables.append("Relation")
            except:
                pass
            
            return tables
            
        except Exception as e:
            logger.error(f"Error listing tables: {e}")
            return []
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()

# ==================== UTILITY FUNCTIONS ====================

async def initialize_database(clean_first: bool = True) -> bool:
    """Initialize the database with current schema"""
    manager = KuzuSchemaManager()
    
    try:
        # Test connection
        if not await manager.validate_connection():
            return False
        
        # Migrate schema
        success = await manager.migrate_schema(clean_first=clean_first)
        
        if success:
            # Show database info
            info = await manager.get_database_info()
            logger.info(f"📊 Database initialized with {info.get('total_entity_types', 0)} entity types")
        
        return success
        
    finally:
        await manager.close()

async def clean_database_data() -> bool:
    """Clean all data but keep schema"""
    manager = KuzuSchemaManager()
    
    try:
        if not await manager.validate_connection():
            return False
        
        return await manager.clean_database()
        
    finally:
        await manager.close()

async def get_database_status() -> Dict[str, Any]:
    """Get current database status and statistics"""
    manager = KuzuSchemaManager()
    
    try:
        if not await manager.validate_connection():
            return {"status": "disconnected"}
        
        info = await manager.get_database_info()
        tables = await manager.list_tables()
        
        info.update({
            "status": "connected",
            "existing_tables": tables,
            "tables_count": len(tables)
        })
        
        return info
        
    finally:
        await manager.close()

async def backup_current_schema(filename: str = None) -> bool:
    """Backup current schema to file"""
    manager = KuzuSchemaManager()
    
    try:
        return await manager.backup_schema(filename)
        
    finally:
        await manager.close()

# ==================== MAIN FUNCTION ====================

async def main():
    """Main function for command-line usage"""
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "init":
            print("🚀 Initializing database...")
            success = await initialize_database(clean_first=True)
            print("✅ Database initialized" if success else "❌ Initialization failed")
            
        elif command == "clean":
            print("🧹 Cleaning database data...")
            success = await clean_database_data()
            print("✅ Database cleaned" if success else "❌ Cleaning failed")
            
        elif command == "status":
            print("📊 Getting database status...")
            status = await get_database_status()
            print(json.dumps(status, indent=2))
            
        elif command == "backup":
            filename = sys.argv[2] if len(sys.argv) > 2 else None
            print("💾 Backing up schema...")
            success = await backup_current_schema(filename)
            print("✅ Schema backed up" if success else "❌ Backup failed")
            
        elif command == "schema":
            print("📋 Current schema information:")
            manager = KuzuSchemaManager()
            print(json.dumps(manager.entity_schemas, indent=2))
            await manager.close()
            
        elif command == "clear":
            print("🗑️ Clearing all tables...")
            manager = KuzuSchemaManager()
            success = await manager.drop_all_tables()
            print("✅ All tables cleared" if success else "❌ Clearing failed")
            await manager.close()

        elif command == "migrate":
            print("🔄 Migrating schema...")
            manager = KuzuSchemaManager()
            success = await manager.migrate_schema(clean_first=True)
            print("✅ Schema migrated" if success else "❌ Migration failed")
            await manager.close()
            
        else:
            print("❌ Unknown command. Available commands: init, clean, status, backup, schema, clear, migrate")
    
    else:
        print("🔧 KuzuDB Schema Manager")
        print("=" * 40)
        print("Available commands:")
        print("  python kuzu_schema_init.py init     - Initialize database")
        print("  python kuzu_schema_init.py clean    - Clean all data")
        print("  python kuzu_schema_init.py status   - Show database status")
        print("  python kuzu_schema_init.py backup   - Backup schema")
        print("  python kuzu_schema_init.py schema   - Show schema info")
        print("  python kuzu_schema_init.py clear    - Drop all tables")
        print("  python kuzu_schema_init.py migrate  - Migrate schema (drop + create)")
        
        # Show current schema summary
        print("\n📋 Current Schema:")
        manager = KuzuSchemaManager()
        print(json.dumps(manager.entity_schemas, indent=2))
        await manager.close()

if __name__ == "__main__":
    asyncio.run(main())
