# Pipeline Configuration
PARALELL_LLM_CALLS = 10  # Number of parallel LLM calls for entity extraction
BATCH_SIZE = 10          # Number of documents to process in each batch

# Data Type Configuration
DEFAULT_DATA_TYPE = "email"  # Default data type for extraction
AUTO_DETECT_DATA_TYPE = True  # Whether to auto-detect data type from content

# Model Configuration
DEFAULT_MODEL = "gemini-2.5-flash"  # Default LLM model to use

# Output Configuration
INCLUDE_METADATA = True  # Include pipeline metadata in output files
INCLUDE_TIMESTAMPS = True  # Include processing timestamps in results
INCLUDE_STATS = True  # Include extraction statistics

# Performance Configuration
MAX_RETRY_ATTEMPTS = 3  # Maximum retry attempts for failed extractions
RETRY_DELAY = 1.0  # Delay between retry attempts (seconds)
TIMEOUT_SECONDS = 120  # Timeout for individual LLM calls
