import os
from typing import Optional

# Pipeline Configuration with environment variable fallbacks
PARALLEL_LLM_CALLS = int(os.getenv('PARALLEL_LLM_CALLS', '10'))  # Number of parallel LLM calls for entity extraction
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '5'))                   # Number of documents to process in each batch

# Database Configuration with environment variable fallbacks
DB_ENTITY_BATCH_SIZE = int(os.getenv('DB_ENTITY_BATCH_SIZE', '1'))   # Process entities one at a time to avoid 413 payload errors
DB_RELATION_BATCH_SIZE = int(os.getenv('DB_RELATION_BATCH_SIZE', '1')) # Process relations one at a time to avoid 413 payload errors

# Timeout Configuration
DEFAULT_REQUEST_TIMEOUT = int(os.getenv('DEFAULT_REQUEST_TIMEOUT', '120'))  # Default timeout in seconds
CONNECTION_TIMEOUT = int(os.getenv('CONNECTION_TIMEOUT', '10'))              # Connection timeout in seconds
READ_TIMEOUT = int(os.getenv('READ_TIMEOUT', '30'))                         # Read timeout in seconds

# Database URL Configuration
KUZU_DB_URL = os.getenv('KUZU_DB_URL', 'http://localhost:7000')

def validate_configuration() -> Optional[str]:
    """Validate configuration values and return error message if invalid."""
    if PARALLEL_LLM_CALLS <= 0:
        return "PARALLEL_LLM_CALLS must be greater than 0"
    if BATCH_SIZE <= 0:
        return "BATCH_SIZE must be greater than 0"
    if DB_ENTITY_BATCH_SIZE <= 0:
        return "DB_ENTITY_BATCH_SIZE must be greater than 0"
    if DB_RELATION_BATCH_SIZE <= 0:
        return "DB_RELATION_BATCH_SIZE must be greater than 0"
    if DEFAULT_REQUEST_TIMEOUT <= 0:
        return "DEFAULT_REQUEST_TIMEOUT must be greater than 0"
    if CONNECTION_TIMEOUT <= 0:
        return "CONNECTION_TIMEOUT must be greater than 0"
    if READ_TIMEOUT <= 0:
        return "READ_TIMEOUT must be greater than 0"
    
    return None  # No errors

# Validate configuration on import
_validation_error = validate_configuration()
if _validation_error:
    raise ValueError(f"Configuration validation failed: {_validation_error}")