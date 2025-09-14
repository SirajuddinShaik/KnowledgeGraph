import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='[%(asctime)s]: %(message)s:')

# Project name for your KG system
project_name = "workspace-kg"

# Define file structure
list_of_files = [
    # Core package
    f"src/{project_name}/__init__.py",

    # Utils
    f"src/{project_name}/utils/__init__.py",
    f"src/{project_name}/utils/common.py",
    f"src/{project_name}/utils/merge.py",        # for merging entities

    # Config
    f"src/{project_name}/config/__init__.py",
    f"src/{project_name}/config/configuration.py",

    # Entities & Schemas
    f"src/{project_name}/entity/__init__.py",
    f"src/{project_name}/entity/config_entity.py",   # dataclasses for entity configs

    # Components (building blocks)
    f"src/{project_name}/components/__init__.py",
    f"src/{project_name}/components/entity_extractor.py",   # LLM to extract entities
    f"src/{project_name}/components/graph_builder.py",      # inserts into DB
    f"src/{project_name}/components/embedder.py",           # embeddings generator

    # Pipeline orchestration
    f"src/{project_name}/pipeline/__init__.py",
    f"src/{project_name}/pipeline/extract_pipeline.py",   # calls LLM + schema

    # Constants
    f"src/{project_name}/constants/__init__.py",
    f"src/{project_name}/constants/entity_types.py",   # all default entity types
    f"src/{project_name}/constants/relation_types.py", # all default relation types

    # Config files
    "config/config.yaml",
    "params.yaml",
    "schema.yaml",   # holds entity + relation schema

    # Entrypoints
    "main.py",       # pipeline entry
    "app.py",        # FastAPI/Streamlit for serving
    "Dockerfile",
    "requirements.txt",
    "project.toml",
    "test.py",

    # Research / Experiments
    "research/trials.ipynb",

    # Templates (for app)
    "templates/index.html"
]

# File creation logic
for filepath in list_of_files:
    filepath = Path(filepath)
    filedir, filename = os.path.split(filepath)

    if filedir != "":
        os.makedirs(filedir, exist_ok=True)
        logging.info(f"Creating directory: {filedir} for the file: {filename}")

    if (not os.path.exists(filepath)) or (os.path.getsize(filepath) == 0):
        with open(filepath, "w") as f:
            pass
        logging.info(f"Creating empty file: {filepath}")
    else:
        logging.info(f"{filename} already exists")
