#!/usr/bin/env python3

import asyncio
import json
from src.workspace_kg.utils.kuzu_db_handler import KuzuDBHandler

async def test_simple_entity_creation():
    """Test basic entity creation to isolate the parameter issue"""
    
    db_handler = KuzuDBHandler()
    
    # Test 1: Simple Person creation with minimal data
    print("🧪 Test 1: Creating simple Person entity")
    person_data = {
        "name": "Test Person",
        "emails": ["test@example.com"],
        "role": ["Developer"],
        "rawDescriptions": ["Test description"]
    }
    
    try:
        result = await db_handler.create_entity("Person", person_data)
        print(f"✅ Person creation result: {result}")
    except Exception as e:
        print(f"❌ Person creation failed: {e}")
    
    # Test 2: Simple query test
    print("\n🧪 Test 2: Simple query test")
    try:
        result = await db_handler.execute_cypher("MATCH (n:Person) RETURN count(n) as count")
        print(f"✅ Query result: {result}")
    except Exception as e:
        print(f"❌ Query failed: {e}")
    
    # Test 3: Test with Branch entity (the one that was duplicated)
    print("\n🧪 Test 3: Creating Branch entity")
    branch_data = {
        "name": "test-branch",
        "repo": "test-repo"
    }
    
    try:
        result = await db_handler.create_entity("Branch", branch_data)
        print(f"✅ Branch creation result: {result}")
    except Exception as e:
        print(f"❌ Branch creation failed: {e}")
    
    await db_handler.close()

if __name__ == "__main__":
    asyncio.run(test_simple_entity_creation())