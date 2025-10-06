# Unified Table Architecture - Kuzu Database Design

## Overview

The Knowledge Graph Pipeline uses a unified table architecture in Kuzu Database to optimize performance and simplify entity management. This approach differs from traditional graph databases that create separate tables for each entity type.

## Unified Table Design

### Core Tables

1. **Nodes Table**: Contains all entities (Person, Organization, Project, etc.)
2. **Relation Table**: Contains all relationships between entities

### Nodes Table Structure

All entity types are stored in a single unified `Nodes` table with the following key characteristics:

- **Primary Key**: `name` (STRING PRIMARY KEY)
- **Type Field**: `type` (STRING) - distinguishes entity types (Person, Organization, etc.)
- **Unified Schema**: Contains all possible fields from all entity types
- **Dynamic Fields**: Not all fields are used by all entity types

#### Example Node Record
```json
{
  "name": "John Doe",
  "type": "Person", 
  "emails": ["john@company.com"],
  "role": ["Software Engineer"],
  "worksAt": "TechCorp",
  "rawDescriptions": ["Senior developer..."],
  "sources": ["email_123"],
  "lastUpdated": "2024-10-04T10:30:00Z"
}
```

### Relation Table Structure

Relationships are stored in a directed `Relation` table connecting any two nodes:

- **FROM**: Source node (any entity type)
- **TO**: Target node (any entity type)  
- **Primary Key**: `relation_id` (STRING PRIMARY KEY)
- **Type Field**: `relationTag` (STRING[]) - relationship types
- **Metadata**: description, strength, sources, permissions

#### Example Relation Record
```json
{
  "relation_id": "hash_based_id",
  "relationTag": ["works_at"],
  "description": ["John works at TechCorp"],
  "strength": 0.9,
  "sources": ["email_123"],
  "lastUpdated": "2024-10-04T10:30:00Z"
}
```

## Query Patterns

### Entity Queries

All entity queries use the unified table with type filtering:

```cypher
// Find all Person entities
MATCH (n:Nodes) WHERE n.type = 'Person' RETURN n

// Find person by email
MATCH (n:Nodes) 
WHERE n.type = 'Person' AND 'john@company.com' IN n.emails 
RETURN n

// Find all organizations
MATCH (n:Nodes) WHERE n.type = 'Organization' RETURN n
```

### Relationship Queries

```cypher
// Find all relationships from a person
MATCH (p:Nodes)-[r:Relation]->(target:Nodes)
WHERE p.type = 'Person' AND p.name = 'John Doe'
RETURN p, r, target

// Find work relationships
MATCH (p:Nodes)-[r:Relation]->(o:Nodes)
WHERE p.type = 'Person' AND o.type = 'Organization' 
AND 'works_at' IN r.relationTag
RETURN p, r, o
```

## Advantages of Unified Table Approach

### 1. **Simplified Schema Management**
- Single table schema reduces complexity
- Schema changes affect only one table structure
- Easier migration and versioning

### 2. **Flexible Entity Types**
- Easy to add new entity types without schema changes
- Dynamic field usage based on entity type
- Simplified polymorphic queries

### 3. **Performance Optimization**
- Reduced JOIN complexity for cross-entity queries
- Single table scans for multi-entity operations
- Better cache locality for related entities

### 4. **Consistent Relationships**
- Unified relationship table handles any entity type connections
- Consistent relationship metadata across all entity types
- Simplified relationship traversal

### 5. **Development Efficiency**
- Single set of CRUD operations for all entity types
- Simplified ORM/database abstraction layer
- Reduced code duplication

## Implementation Details

### Entity Creation

```python
# All entities use the same create method
await db_handler.create_entity("Person", {
    "name": "John Doe",
    "type": "Person",  # Added automatically
    "emails": ["john@company.com"],
    "role": ["Engineer"]
})

await db_handler.create_entity("Organization", {
    "name": "TechCorp", 
    "type": "Organization",  # Added automatically
    "domain": "techcorp.com",
    "industry": "Technology"
})
```

### Entity Retrieval

```python
# Retrieve by type and primary key
person = await db_handler.get_entity("Person", "John Doe")
org = await db_handler.get_entity("Organization", "TechCorp")
```

### Relationship Management

```python
# Create relationship between any entity types
await db_handler.create_relation(
    from_entity_type="Person",
    from_entity_id="John Doe", 
    to_entity_type="Organization",
    to_entity_id="TechCorp",
    relation_properties={
        "relation_id": "generated_id",
        "relationTag": ["works_at"],
        "description": ["John works at TechCorp"],
        "strength": 0.9
    }
)
```

## Schema Configuration

The unified table is generated from the YAML schema by combining all entity type definitions:

```yaml
# schema.yaml (excerpt)
Person:
  name: STRING PRIMARY KEY
  emails: STRING[]
  role: STRING[]
  worksAt: STRING

Organization:
  name: STRING PRIMARY KEY  
  domain: STRING
  industry: STRING
  location: STRING[]
```

The system automatically:
1. Merges all fields into unified schema
2. Adds `type` field for entity type discrimination
3. Handles array fields consistently across types
4. Maintains field type definitions

## Migration from Separate Tables

If migrating from a separate-tables approach:

1. **Data Migration**: Extract data from separate entity tables
2. **Schema Unification**: Merge schemas into unified definition
3. **Query Updates**: Update all queries to use type filtering
4. **Index Optimization**: Add indexes on type + commonly queried fields

## Best Practices

### 1. **Indexing Strategy**
- Index on `type` field for efficient entity type filtering
- Composite indexes on `(type, frequently_queried_field)`
- Array field indexes for email/tag searches

### 2. **Query Optimization**
- Always filter by `type` when querying specific entity types
- Use specific field queries rather than full table scans
- Leverage array operations for multi-value fields

### 3. **Schema Evolution**
- Add new fields to unified schema as needed
- Use nullable fields for type-specific attributes
- Document field usage per entity type

### 4. **Data Integrity**
- Validate entity type matches expected schema
- Enforce required fields per entity type in application logic
- Use transactions for multi-entity operations

## Monitoring and Debugging

### Query Performance
```cypher
// Check distribution of entity types
MATCH (n:Nodes) 
RETURN n.type, count(*) as entity_count
ORDER BY entity_count DESC

// Monitor relationship patterns  
MATCH ()-[r:Relation]->()
RETURN r.relationTag, count(*) as relation_count
ORDER BY relation_count DESC
```

### Data Quality
```cypher
// Find entities missing required fields
MATCH (n:Nodes)
WHERE n.type = 'Person' AND (n.name IS NULL OR n.emails IS NULL)
RETURN n

// Check relationship data integrity
MATCH (source:Nodes)-[r:Relation]->(target:Nodes)
WHERE r.relationTag IS NULL OR size(r.relationTag) = 0
RETURN source, r, target
```

This unified table architecture provides a scalable, flexible foundation for the Knowledge Graph Pipeline while maintaining performance and simplifying development.