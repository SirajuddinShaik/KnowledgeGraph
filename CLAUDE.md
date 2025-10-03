# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Knowledge Graph Pipeline that extracts information from various sources (primarily emails from Vespa), transforms it into structured entities and relationships using LLMs, and stores it in a Kuzu graph database. The system is designed for stateful processing with progress tracking and systematic entity merging.

## Core Architecture

### Main Components
- **Pipeline Layer** (`src/workspace_kg/pipeline/`): Main orchestration (VespaEmailPipeline)
- **Components Layer** (`src/workspace_kg/components/`): Core processing units (EntityExtractor, Embedder, SystematicMergeProvider)
- **Utils Layer** (`src/workspace_kg/utils/`): Integrations and utilities (VespaIntegration, MergePipeline, KuzuDBHandler)
- **Config Layer** (`src/workspace_kg/config/`): Configuration management

### Data Flow
1. **Vespa Data Source** → Email fetching with metadata and permissions
2. **Entity Extraction** → LLM-powered extraction with source tracking
3. **Systematic Merging** → Intelligent deduplication based on configurable rules
4. **Knowledge Graph Storage** → Kuzu database with full relationship modeling

## Development Commands

### Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration

# Start required services (Kuzu database and explorer)
docker-compose up -d
```

### Database Operations
```bash
# Initialize Kuzu database with schema
python src/workspace_kg/scripts/kuzu_init.py init

# Reset database (destructive)
python src/workspace_kg/scripts/kuzu_init.py reset
```

### Pipeline Execution
```bash
# Run the main email processing pipeline
python src/workspace_kg/pipeline/vespa_email_pipeline.py

# Run with custom configuration via environment variables
VESPA_MAX_EMAILS=50 BATCH_SIZE=5 python src/workspace_kg/pipeline/vespa_email_pipeline.py
```

### Development Scripts
```bash
# Print all persons in the knowledge graph
python src/workspace_kg/scripts/print_persons.py

# Check progress without running pipeline
python -c "import asyncio; from src.workspace_kg.pipeline.vespa_email_pipeline import get_progress_summary; print(asyncio.run(get_progress_summary()))"
```

## Key Configuration Files

### Schema Definition (`schema.yaml`)
- Defines all entity types (Person, Organization, Project, Repository, etc.)
- Specifies properties, data types, and relationships
- Central to the knowledge graph structure

### Entity Configuration (`entity_config.yaml`)
- Controls LLM extraction fields and database mappings
- Defines merge strategies for conflict resolution
- Contains systematic merge rules for duplicate detection

### Environment Configuration (`.env`)
- Vespa endpoint and authentication
- LLM model configuration (OpenAI/Gemini)
- Processing parameters (batch sizes, parallelism)
- Database connections and paths

## Stateful Processing

The pipeline maintains comprehensive state tracking:
- **Progress Tracking**: JSON file tracks processed/failed emails with detailed metadata
- **Resume Capability**: Can restart from last successful state
- **Error Handling**: Failed emails are logged with error details for retry
- **Session Management**: Each run creates a session with statistics

Progress files are stored in `data/email_processing_progress.json` by default.

## LLM Integration

### Supported Models
- OpenAI models (GPT-4, etc.)
- Google Gemini models (gemini-2.5-flash, etc.)
- Configurable via `LLM_MODEL_NAME` environment variable

### Entity Extraction Process
- Automatic data type detection for optimal prompts
- Parallel processing with configurable concurrency
- Source tracking for full traceability
- Permission inheritance from source data

## Database Schema

### Entity Types
- **People & Organization**: Person, Team, Organization
- **Projects & Work**: Project, Event, Topic
- **Code & Development**: Repository, Branch, CodeChangeRequest, Issue
- **Relationships**: Generic Relation type connecting any entities

### Key Features
- Embedding support for semantic search
- Permission tracking for access control
- Source attribution for audit trails
- Flexible relationship modeling

## Development Guidelines

### Testing Pipeline Changes
1. Use small batch sizes (`VESPA_MAX_EMAILS=10 BATCH_SIZE=2`)
2. Enable data saving (`SAVE_EXTRACTED_DATA=true`) for inspection
3. Monitor progress files for debugging
4. Check Kuzu Explorer at http://localhost:8000 for results

### Adding New Entity Types
1. Define in `schema.yaml` with all required properties
2. Update `entity_config.yaml` with extraction and merge rules
3. Regenerate database schema with `kuzu_init.py init`
4. Test with small dataset first

### Debugging Common Issues
- **Vespa Connection**: Check endpoint, schema, and namespace configuration
- **LLM Failures**: Verify API keys and model availability
- **Database Issues**: Ensure Kuzu services are running via docker-compose
- **Progress Problems**: Check write permissions for `data/` directory

## Docker Services

The project uses docker-compose for required services:
- **kuzu-db**: Graph database API (port 7000)
- **kuzu-explorer**: Web interface for database exploration (port 8000)

Always run `docker-compose up -d` before pipeline execution.