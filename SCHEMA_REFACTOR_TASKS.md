# Schema Refactor Tasks - Two-Table Design

## Overview
Refactor the current multi-table schema to a simplified two-table design:
1. **Nodes** table - stores all entity types with a `type` field to distinguish them
2. **Relations** table - stores all relationships between nodes

## Current State Analysis

### Current Schema (schema.yaml)
- **11 separate entity tables**: Person, Team, Organization, Project, Repository, Branch, CodeChangeRequest, Issue, Event, Topic
- **1 relationship table**: Relation (with FROM/TO connecting all entity types)
- Each entity table has its own specific fields plus common fields (rawDescriptions, cleanDescription, permissions, sources, lastUpdated, embedding)

### Current Implementation Files

#### 1. schema.yaml
- Defines 11 entity types as separate tables
- Each entity has PRIMARY KEY on `name` field
- Relation table uses FROM/TO with all possible entity type combinations
- Common fields across entities: rawDescriptions, cleanDescription, permissions, sources, lastUpdated, embedding

#### 2. src/workspace_kg/scripts/kuzu_init.py
- `KuzuSchemaManager` class loads schema from YAML
- `_separate_schemas()` method separates entities from relationships
- `_generate_node_table_query()` creates individual node tables for each entity type
- `_generate_relationship_table_query()` creates Relation table with all FROM-TO combinations
- Schema migration functions: `create_schema()`, `migrate_schema()`, `drop_all_tables()`

#### 3. src/workspace_kg/utils/kuzu_db_handler.py
- `KuzuDBHandler` class manages database operations
- Loads schema from YAML and separates entities/relationships
- CRUD operations assume entity-specific tables:
  - `create_entity()` - uses entity_type as table name
  - `get_entity()` - queries specific entity table
  - `update_entity()` - updates specific entity table
  - `delete_entity()` - deletes from specific entity table
- Relation operations work with generic Relation table
- All entities use `name` as PRIMARY KEY

#### 4. src/workspace_kg/utils/merge_pipeline.py
- `MergePipeline` class processes entity/relation batches
- `_generate_entity_id()` - creates unique IDs based on entity type and attributes
- `_find_existing_entity()` - searches for entities in type-specific tables
- `process_batch()` - creates/updates entities in type-specific tables
- Uses `entity_config` for merge strategies per entity type

## Refactoring Tasks

### Phase 1: Schema Design
- [ ] **Task 1.1**: Design new Nodes table schema
  - Add `type` field (STRING) to store entity type (Person, Team, Organization, etc.)
  - Keep `name` as PRIMARY KEY
  - Add all common fields: rawDescriptions, cleanDescription, permissions, sources, lastUpdated, embedding
  - Add `properties` field (JSON/MAP) to store entity-specific attributes
  - Alternative: Keep all entity-specific fields as optional columns in Nodes table

- [ ] **Task 1.2**: Design new Relations table schema
  - Keep existing structure but simplify FROM/TO to just reference Nodes table
  - Fields: relation_id (PRIMARY KEY), description, relationTag, type, strength, permissions, sources, createdAt, lastUpdated, embedding
  - FROM Nodes TO Nodes (single declaration instead of all combinations)

- [ ] **Task 1.3**: Update schema.yaml
  - Replace 11 entity definitions with single Nodes table definition
  - Update Relations table to reference Nodes instead of specific entity types
  - Document the `type` field values (enum-like documentation)

### Phase 2: Database Initialization Script Updates
- [ ] **Task 2.1**: Update kuzu_init.py - Schema Loading
  - Modify `_load_schema_from_yaml()` to handle new two-table structure
  - Update `_separate_schemas()` to recognize Nodes vs Relations
  - Remove entity-specific schema separation logic

- [ ] **Task 2.2**: Update kuzu_init.py - Table Creation
  - Modify `_generate_node_table_query()` to create single Nodes table
  - Update `_generate_relationship_table_query()` to use simplified FROM Nodes TO Nodes
  - Remove logic for creating multiple entity tables

- [ ] **Task 2.3**: Update kuzu_init.py - Migration Functions
  - Update `create_schema()` to create only 2 tables
  - Update `drop_all_tables()` to drop only Nodes and Relations tables
  - Update `list_tables()` to expect only 2 tables
  - Update `get_database_info()` to query by node types instead of tables

### Phase 3: Database Handler Updates
- [ ] **Task 3.1**: Update kuzu_db_handler.py - Schema Loading
  - Modify `_load_schema()` to handle new schema structure
  - Update `_separate_schemas()` for Nodes/Relations distinction
  - Store entity type definitions for validation

- [ ] **Task 3.2**: Update kuzu_db_handler.py - Entity CRUD Operations
  - **create_entity()**: 
    - Change to insert into Nodes table with `type` field
    - Add entity_type as `type` field value
    - Handle entity-specific properties appropriately
  - **get_entity()**:
    - Query Nodes table with WHERE type = entity_type AND name = entity_id
  - **update_entity()**:
    - Update Nodes table with type filter
  - **delete_entity()**:
    - Delete from Nodes table with type filter

- [ ] **Task 3.3**: Update kuzu_db_handler.py - Relation Operations
  - **create_relation()**:
    - Update to reference Nodes table instead of specific entity tables
    - Simplify MATCH clauses to use Nodes with type filters
  - **get_relations_between_entities()**:
    - Update MATCH clauses to query Nodes with type filters
  - Other relation methods should work with minimal changes

- [ ] **Task 3.4**: Update kuzu_db_handler.py - Validation
  - Modify `_validate_and_filter_properties()` to work with unified schema
  - Add validation for `type` field values
  - Handle entity-specific property validation based on type

### Phase 4: Merge Pipeline Updates
- [ ] **Task 4.1**: Update merge_pipeline.py - Entity ID Generation
  - Update `_generate_entity_id()` to work with Nodes table
  - Ensure entity_type is included in ID generation logic

- [ ] **Task 4.2**: Update merge_pipeline.py - Entity Finding
  - Modify `_find_existing_entity()` to query Nodes table with type filter
  - Update Cypher queries to use: MATCH (n:Nodes {type: $entity_type, ...})

- [ ] **Task 4.3**: Update merge_pipeline.py - Batch Processing
  - Update `process_batch()` to create entities in Nodes table
  - Ensure `type` field is set for all entities
  - Update relation creation to reference Nodes table

- [ ] **Task 4.4**: Update merge_pipeline.py - Statistics
  - Modify `get_database_statistics()` to count by node type
  - Update queries: MATCH (n:Nodes {type: $entity_type}) RETURN count(n)

### Phase 5: Testing & Validation
- [ ] **Task 5.1**: Create test migration script
  - Script to backup existing data
  - Script to migrate data from old schema to new schema
  - Validation that all entities and relations are preserved

- [ ] **Task 5.2**: Test database initialization
  - Run kuzu_init.py to create new schema
  - Verify only 2 tables are created
  - Verify table structures match design

- [ ] **Task 5.3**: Test CRUD operations
  - Test creating entities of different types
  - Test retrieving entities by type and name
  - Test updating entities
  - Test deleting entities
  - Test creating relations between different node types

- [ ] **Task 5.4**: Test merge pipeline
  - Test processing sample batch data
  - Verify entities are created with correct type field
  - Verify relations are created correctly
  - Check statistics reporting

- [ ] **Task 5.5**: Integration testing
  - Test full pipeline from extraction to database
  - Verify data integrity
  - Check performance compared to old schema

### Phase 6: Documentation & Cleanup
- [ ] **Task 6.1**: Update documentation
  - Update README with new schema design
  - Document migration process
  - Update API documentation

- [ ] **Task 6.2**: Code cleanup
  - Remove unused entity-specific code
  - Clean up comments and docstrings
  - Update type hints and annotations

- [ ] **Task 6.3**: Performance optimization
  - Add indexes on Nodes.type field
  - Optimize common query patterns
  - Benchmark performance

## Implementation Notes

### Design Decision: Properties Storage
Two approaches for entity-specific properties:

**Option A: JSON/MAP field**
- Store entity-specific properties in a `properties` JSON/MAP field
- Pros: Flexible, easy to add new entity types
- Cons: Harder to query specific properties, no schema validation

**Option B: All fields as optional columns**
- Include all entity-specific fields as optional columns in Nodes table
- Pros: Better query performance, schema validation
- Cons: Many NULL values, less flexible

**Recommendation**: Start with Option B (all fields as columns) since:
- Current schema has manageable number of unique fields
- Better query performance for known fields
- Easier migration from current structure
- Can add properties field later if needed

### Migration Strategy
1. Create new schema alongside old schema (temporary dual-schema)
2. Migrate data from old tables to new Nodes table
3. Verify data integrity
4. Switch application to use new schema
5. Drop old tables after verification period

### Backward Compatibility
- Consider keeping old table names as views for transition period
- Provide migration utilities for external tools
- Version the schema changes

## Success Criteria
- [ ] All entity types stored in single Nodes table with type field
- [ ] All relationships stored in single Relations table
- [ ] All CRUD operations work correctly
- [ ] Merge pipeline processes batches successfully
- [ ] Statistics and reporting functions work
- [ ] No data loss during migration
- [ ] Performance is acceptable or improved
- [ ] All tests pass

## Estimated Effort
- Phase 1: 2-3 hours (schema design and documentation)
- Phase 2: 3-4 hours (kuzu_init.py updates)
- Phase 3: 4-6 hours (kuzu_db_handler.py updates)
- Phase 4: 3-4 hours (merge_pipeline.py updates)
- Phase 5: 4-6 hours (testing and validation)
- Phase 6: 2-3 hours (documentation and cleanup)

**Total: 18-26 hours**

## Risk Assessment
- **High Risk**: Data migration - need careful backup and validation
- **Medium Risk**: Query performance changes - need benchmarking
- **Low Risk**: Code refactoring - well-defined interfaces

## Next Steps
1. Review and approve this task list
2. Create backup of current database
3. Start with Phase 1 (schema design)
4. Implement changes incrementally with testing at each phase
5. Conduct thorough testing before production deployment
