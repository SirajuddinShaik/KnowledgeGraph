# Directory Structure

This project is organized into the following directories:

-   **`config/`**: Contains global configuration files for the project.
-   **`data/`**: Used for storing data, such as the email processing progress file (`email_processing_progress.json`).
-   **`docs/`**: Contains documentation files, including this one.
-   **`kuzu_data/`**: The default directory for the Kuzu database files.
-   **`research/`**: Contains Jupyter notebooks and other research-related files.
-   **`src/`**: The main source code directory.
    -   **`workspace_kg/`**: The main Python package for this project.
        -   **`components/`**: Contains the core components of the data processing pipeline, such as the `EntityExtractor`.
        -   **`config/`**: Contains configuration-related modules.
        -   **`constants/`**: For storing project-wide constants.
        -   **`pipeline/`**: Contains the main data processing pipelines, like the `VespaEmailPipeline`.
        -   **`scripts/`**: Contains command-line scripts for managing the project, such as `kuzu_init.py`.
        -   **`utils/`**: Contains utility modules for various tasks, such as connecting to Vespa, handling the merge pipeline, and more.

This structure separates the different parts of the project, making it easier to navigate and maintain.
