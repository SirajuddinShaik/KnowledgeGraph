# Scripts

This repository includes scripts to help manage and interact with the knowledge graph.

## `src/workspace_kg/scripts/kuzu_init.py`

This script is a command-line tool for managing the Kuzu database schema. It uses the `schema.yaml` file to ensure the database schema is aligned with the data model.

### Usage

You can run the script from the command line with various commands:

```bash
python src/workspace_kg/scripts/kuzu_init.py [command]
```

### Commands

-   **`init`**: Initializes the database. This command creates the node and relationship tables based on the `schema.yaml` file. By default, it drops all existing tables before creating the new ones.
-   **`clean`**: Cleans all data from the database but preserves the schema. This is useful for when you want to reload the data without re-creating the schema.
-   **`status`**: Displays the current status of the database, including the number of nodes and relationships of each type.
-   **`backup`**: Creates a JSON backup of the current schema.
-   **`schema`**: Displays the current schema information.
-   **`clear`**: Drops all tables from the database, completely resetting the schema.
-   **`migrate`**: Performs a full schema migration by dropping all existing tables and then re-creating them based on the `schema.yaml` file.

This script is essential for setting up the database for the first time and for managing schema changes as the project evolves.
