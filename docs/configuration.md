# Configuration

The behavior of the data processing pipeline is controlled by a set of configuration files. These files define the data schema, the entity extraction logic, and the rules for merging data into the knowledge graph.

## `schema.yaml`

This file defines the schema of the knowledge graph. It specifies the node types (e.g., `Person`, `Organization`, `Project`) and their properties. It also defines the types of relationships that can exist between nodes.

### Example Node Definition

```yaml
Person:
  name: STRING PRIMARY KEY
  emails: STRING[]
  role: STRING[]
  aliases: STRING[]
  sourceSystemId: STRING
  worksAt: STRING
  rawDescriptions: STRING[]
  cleanDescription: STRING
  permissions: STRING[]
  sources: STRING[]
  lastUpdated: STRING
  embedding: DOUBLE[]
```

This example defines a `Person` node with several properties, including a primary key (`name`), arrays of strings (`emails`, `role`), and an embedding vector.

## `entity_config.yaml`

This file is central to the entity extraction and merging process. It contains three main sections:

### `entity_schemas`

This section defines which fields from the source data should be extracted for each entity. It also maps these fields to the corresponding properties in the `schema.yaml` and specifies the merge strategy for each property.

-   **`llm_fields`**: A list of fields that the Large Language Model (LLM) should attempt to extract from the source text.
-   **`mappings`**: This section maps the extracted fields to the database schema. Each mapping includes:
    -   `type`: The data type of the field in the database.
    -   `merge_strategy`: How to handle conflicts when an entity is updated. Common strategies include:
        -   `preserve_existing`: Keep the existing value and discard the new one.
        -   `append_unique`: Add the new value to a list if it's not already present.
        -   `replace_always`: Always replace the existing value with the new one.
    -   `mapping`: The name of the corresponding field in the `schema.yaml`.

### `systematic_merge`

This section defines the rules for identifying and merging duplicate entities.

-   **`matching_rules`**: A set of rules for each entity type that determine whether two entities should be considered the same. Rules can be based on exact matches or other criteria, and each rule has a confidence score.
-   **`merge_fields`**: A list of fields that should be merged when two entities are identified as duplicates.

For more details on the merge process, refer to the `MergePipeline` in `src/workspace_kg/utils/merge_pipeline.py`.
