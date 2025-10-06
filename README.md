# Knowledge Graph Pipeline

This repository contains a data processing pipeline that extracts information from various sources, transforms it into a structured format, and loads it into a knowledge graph. The primary implementation focuses on processing emails from Vespa, but the architecture is designed to be extensible to other data sources.

## Features

-   **Stateful Processing:** The pipeline tracks its progress, allowing it to be resumed without reprocessing data.
-   **LLM-Powered Entity Extraction:** It uses a Large Language Model (LLM) to extract entities and relationships from unstructured text.
-   **Configurable Schema:** The knowledge graph schema is defined in a simple YAML file (`schema.yaml`), making it easy to customize.
-   **Systematic Merging:** The pipeline includes a systematic merge process to identify and merge duplicate entities.
-   **Vespa Integration:** It includes a connector for fetching data from a Vespa instance.
-   **Kuzu Database:** The extracted knowledge is stored in a Kuzu graph database using a unified table approach where all entities are stored in a single "Nodes" table with type distinctions.

## Getting Started

### Prerequisites

-   Python 3.8+
-   Docker
-   Access to a Vespa instance with email data
-   An environment with the required LLM API keys

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/SirajuddinShaik/KnowledgeGraph.git
    cd KnowledgeGraph
    ```

2.  **Start the services:**
    This project uses Docker to run the required services. To start the Kuzu database and explorer, run the following command:
    ```bash
    docker-compose up -d
    ```

3.  **Install the required Python packages:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Set up the environment variables:**
    Create a `.env` file by copying the `.env.example` file and filling in the required values.

### Running the Pipeline

1.  **Initialize the Database:**
    Before running the pipeline for the first time, you need to initialize the Kuzu database with the correct schema:
    ```bash
    python src/workspace_kg/scripts/kuzu_init.py init
    ```

2.  **Run the Pipeline:**
    To start the email processing pipeline, run the following command:
    ```bash
    python src/workspace_kg/pipeline/vespa_email_pipeline.py
    ```

## Documentation

For more detailed information, please refer to the documentation in the `docs` directory:

-   **[Directory Structure](docs/directory_structure.md):** An overview of the project's directory structure.
-   **[Data Processing Pipeline](docs/pipeline.md):** A detailed explanation of the data processing pipeline.
-   **[Configuration](docs/configuration.md):** Information on how to configure the pipeline and the data schema.
-   **[Scripts](docs/scripts.md):** Documentation for the command-line scripts included in the project.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue.
