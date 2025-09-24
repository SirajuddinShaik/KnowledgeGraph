# Data Processing Pipeline

The core of this repository is the data processing pipeline, which is responsible for extracting information from various sources, transforming it into a structured format, and loading it into a knowledge graph. The main pipeline is implemented in `src/workspace_kg/pipeline/vespa_email_pipeline.py`.

## Pipeline Overview

The pipeline is designed to be a stateful, end-to-end solution that processes emails from Vespa, extracts entities and relationships, and stores them in a Kuzu graph database. It includes the following key features:

- **Progress Tracking:** The pipeline maintains a JSON file (`data/email_processing_progress.json`) to keep track of which emails have been processed. This allows the pipeline to be stopped and resumed without reprocessing the same data.
- **Data Fetching:** It connects to a Vespa instance to fetch email data. It only fetches emails that have not been processed yet, based on the progress tracking file.
- **Entity Extraction:** The pipeline uses a Large Language Model (LLM) to extract entities and relationships from the email content. The extraction process is configured through `entity_config.yaml`.
- **Data Merging:** The extracted data is merged into a Kuzu database. The merge logic, defined in `entity_config.yaml`, handles the creation and updating of entities and relationships, including a systematic merge to resolve duplicates.
- **Batch Processing:** The pipeline processes emails in batches to manage memory usage and to handle large volumes of data efficiently.

## Pipeline Steps

The pipeline executes the following steps:

1.  **Initialization:** The pipeline initializes all its components, including the Vespa connector, the entity extractor, and the merge pipeline. It also loads the progress tracking data.
2.  **Fetch Unprocessed Emails:** It fetches a batch of emails from Vespa, filtering out any that have already been processed.
3.  **Extract Entities and Relationships:** For each email in the batch, it uses the entity extractor to identify and extract entities (like `Person`, `Organization`, `Project`) and the relationships between them.
4.  **Merge to Database:** The extracted entities and relationships are then passed to the merge pipeline, which upserts them into the Kuzu database.
5.  **Update Progress:** After each batch is successfully processed, the progress tracking file is updated to mark the emails as processed.

## Configuration

The pipeline is configured using environment variables, which are defined in the `.env` file. The `VespaEmailPipelineConfig` class in `src/workspace_kg/pipeline/vespa_email_pipeline.py` encapsulates all the configuration options, including:

- Vespa connection details (`VESPA_ENDPOINT`, `VESPA_SCHEMA`)
- LLM model for entity extraction (`LLM_MODEL_NAME`)
- Batch size and other processing parameters (`BATCH_SIZE`, `MAX_EMAILS`)
- Kuzu database URL (`KUZU_URL`)

For more details on configuration, see the [Configuration Documentation](configuration.md).
