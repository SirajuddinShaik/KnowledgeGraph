# Vespa Email Pipeline - Knowledge Graph Extraction

This README provides a comprehensive guide for setting up and running the Vespa email pipeline to extract entities and relationships from email data and build a knowledge graph.

## Overview

The Vespa Email Pipeline is designed to:
- Connect to Vespa search engine containing email data
- Extract entities (People, Organizations, Projects, etc.) from email content
- Identify relationships between entities
- Store the knowledge graph in KuzuDB with proper entity merging and deduplication

## Prerequisites

### 1. Environment Setup
```bash
# Install required dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration
```

### 2. Required Services
- **Docker & Docker Compose**: For running KuzuDB and other services
- **KuzuDB**: Graph database for storing the knowledge graph
- **Vespa**: Search engine containing email data
- **LLM Provider**: For entity extraction (OpenAI, Anthropic, or local models)

### 3. Configuration Files
- `.env` - Environment variables and API keys
- `schema.yaml` - Database schema definition
- `entity_config.yaml` - Entity extraction and merging rules
- `docker-compose.yml` - Service orchestration configuration

## Docker Compose Setup

The project includes a `docker-compose.yml` file to easily start all required services:

```yaml
version: '3.8'
services:
  kuzu:
    image: kuzudb/kuzu-api:latest
    ports:
      - "7000:7000"
    volumes:
      - kuzu_data:/var/lib/kuzu
    environment:
      - KUZU_PORT=7000
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:7000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Optional: Include Vespa if you want to run it locally
  # vespa:
  #   image: vespaengine/vespa:latest
  #   ports:
  #     - "8080:8080"
  #     - "19071:19071"
  #   volumes:
  #     - vespa_data:/opt/vespa/var

volumes:
  kuzu_data:
  # vespa_data:
```

## Quick Start

### 1. Configure Environment Variables

Edit your `.env` file:

```bash
# LLM Configuration
OPENAI_API_KEY=your_openai_api_key_here
ANTHROPIC_API_KEY=your_anthropic_api_key_here

# Vespa Configuration
VESPA_URL=http://localhost:8080
VESPA_APPLICATION=your_vespa_app

# KuzuDB Configuration
KUZU_API_URL=http://localhost:7000

# Pipeline Configuration
BATCH_SIZE=10
MAX_EMAILS_TO_PROCESS=100
ENABLE_SYSTEMATIC_MERGE=true
```

### 2. Start Required Services

```bash
# Start all services using Docker Compose
docker-compose up -d

# Or start individual services:
# Start KuzuDB only
docker-compose up -d kuzu

# Start Vespa only (if included in docker-compose.yml)
docker-compose up -d vespa

# Verify services are running
docker-compose ps

# Check KuzuDB health
curl http://localhost:7000/health

# Ensure Vespa is running and accessible
curl http://localhost:8080/ApplicationStatus
```

### 3. Initialize Database Schema

```bash
# Initialize KuzuDB with the schema
python src/workspace_kg/scripts/kuzu_init.py
```

### 4. Run the Email Pipeline

```bash
# Basic pipeline run
python src/workspace_kg/pipeline/vespa_email_pipeline.py

# With custom parameters
python src/workspace_kg/pipeline/vespa_email_pipeline.py --max-emails 50 --batch-size 5
```

## Pipeline Components

### 1. Email Data Extraction
- **Source**: Vespa search engine
- **Query**: Configurable email search queries
- **Fields**: Subject, body, sender, recipients, timestamps

### 2. Entity Extraction
- **Entities**: Person, Organization, Project, Repository, Issue, CodeChangeRequest
- **Method**: LLM-based extraction using configured prompts
- **Output**: Structured entity data with attributes

### 3. Relationship Extraction
- **Types**: works_at, collaborates_with, manages, contributes_to, etc.
- **Method**: LLM analysis of entity interactions in email content
- **Output**: Typed relationships with confidence scores

### 4. Entity Merging & Deduplication
- **Strategy**: Systematic merge with configurable rules
- **Matching**: Email addresses, names, aliases, system IDs
- **Deduplication**: Prevents duplicate entities and relationships

### 5. Knowledge Graph Storage
- **Database**: KuzuDB graph database
- **Schema**: Defined in `schema.yaml`
- **Indexing**: Optimized for graph queries and traversal

## Configuration

### Entity Configuration (`entity_config.yaml`)

```yaml
entity_schemas:
  Person:
    llm_fields:
      - name
      - email
      - role
      - aliases
    mappings:
      name:
        type: "STRING PRIMARY KEY"
        merge_strategy: "preserve_existing"
      emails:
        type: "STRING[]"
        merge_strategy: "append_unique"

systematic_merge:
  matching_rules:
    Person:
      - rule: "search"
        match: "email"
        db: "emails"
        type: "list"
        priority: 1
        confidence: 0.90
```

### Pipeline Configuration

Key parameters in the pipeline:

```python
# Email processing
MAX_EMAILS = 100
BATCH_SIZE = 10
VESPA_QUERY = "SELECT * FROM email WHERE timestamp > '2024-01-01'"

# Entity extraction
ENABLE_ENTITY_EXTRACTION = True
ENABLE_RELATIONSHIP_EXTRACTION = True

# Merging strategy
USE_SYSTEMATIC_MERGE = True
MERGE_CONFIDENCE_THRESHOLD = 0.8
```

## Usage Examples

### 1. Process Recent Emails
```bash
python src/workspace_kg/pipeline/vespa_email_pipeline.py \
  --query "timestamp:[2024-01-01 TO *]" \
  --max-emails 200 \
  --batch-size 20
```

### 2. Process Specific Email Domain
```bash
python src/workspace_kg/pipeline/vespa_email_pipeline.py \
  --query "sender:*@company.com" \
  --max-emails 500
```

### 3. Extract Only Specific Entity Types
```bash
python src/workspace_kg/pipeline/vespa_email_pipeline.py \
  --entity-types "Person,Organization" \
  --max-emails 100
```

## Monitoring and Debugging

### 1. Pipeline Logs
```bash
# View real-time logs
tail -f logs/vespa_email_pipeline.log

# Check for errors
grep "ERROR" logs/vespa_email_pipeline.log
```

### 2. Database Queries
```bash
# Check extracted entities
python -c "
from workspace_kg.utils.kuzu_db_handler import KuzuDBHandler
import asyncio

async def check_entities():
    db = KuzuDBHandler()
    result = await db.execute_cypher('MATCH (p:Person) RETURN count(p)')
    print(f'Total persons: {result}')
    await db.close()

asyncio.run(check_entities())
"
```

### 3. Test Entity Merging
```bash
# Run merge test
python test_merge_handler.py

# Test relationship deduplication
python test_relationship_deduplication_fix.py
```

## Performance Optimization

### 1. Batch Processing
- Adjust `BATCH_SIZE` based on available memory
- Larger batches = better throughput, more memory usage
- Recommended: 10-50 emails per batch

### 2. Parallel Processing
```python
# Enable parallel entity extraction
PARALLEL_EXTRACTION = True
MAX_WORKERS = 4
```

### 3. Caching
- Enable LLM response caching for repeated content
- Cache entity extraction results
- Use database connection pooling

## Troubleshooting

### Common Issues

1. **Vespa Connection Failed**
   ```bash
   # Check Vespa status
   curl http://localhost:8080/ApplicationStatus
   
   # Verify Vespa configuration in .env
   ```

2. **KuzuDB Connection Error**
   ```bash
   # Check KuzuDB service
   curl http://localhost:7000/health
   
   # Check Docker Compose services
   docker-compose ps
   
   # Restart KuzuDB if needed
   docker-compose restart kuzu
   
   # View KuzuDB logs
   docker-compose logs kuzu
   
   # Restart all services
   docker-compose down && docker-compose up -d
   ```

3. **LLM API Errors**
   ```bash
   # Check API keys in .env
   # Monitor rate limits
   # Verify model availability
   ```

4. **Entity Merging Issues**
   ```bash
   # Check entity_config.yaml syntax
   # Verify matching rules
   # Test with smaller batches
   ```

### Debug Mode
```bash
# Run with debug logging
export LOG_LEVEL=DEBUG
python src/workspace_kg/pipeline/vespa_email_pipeline.py --debug
```

## Data Flow

```
Vespa Email Data
       ↓
   Email Extraction
       ↓
   Entity Extraction (LLM)
       ↓
   Relationship Extraction (LLM)
       ↓
   Entity Merging & Deduplication
       ↓
   KuzuDB Storage
       ↓
   Knowledge Graph
```

## Output

The pipeline produces:
- **Entities**: Structured data about people, organizations, projects
- **Relationships**: Typed connections between entities
- **Metadata**: Source tracking, confidence scores, timestamps
- **Logs**: Detailed processing information and statistics

## Next Steps

After running the pipeline:
1. **Query the Knowledge Graph**: Use KuzuDB queries to explore relationships
2. **Visualize Results**: Connect to graph visualization tools
3. **Analyze Patterns**: Identify collaboration networks and communication patterns
4. **Extend Entities**: Add custom entity types as needed
5. **Optimize Performance**: Tune configuration based on your data volume

## Support

For issues and questions:
- Check the troubleshooting section above
- Review pipeline logs for error details
- Test individual components in isolation
- Verify configuration files and environment variables