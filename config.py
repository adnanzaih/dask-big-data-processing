"""
Configuration module for the optimized data pipeline.
Provides environment-specific settings and tuning parameters.
"""

import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class PipelineConfig:
    """Production-ready configuration for the data pipeline."""
    
    # Input/Output Configuration
    json_root: str = os.environ.get("JSON_ROOT", '')
    db_path: str = os.environ.get("DB_PATH", "")
    table_name: str = os.environ.get("TABLE_NAME", "leads")
    
    # Performance Tuning
    chunk_size_mb: int = int(os.environ.get("CHUNK_SIZE_MB", 128))
    max_workers: int = int(os.environ.get("MAX_WORKERS", 0)) or None  # 0 = auto-detect
    threads_per_worker: int = int(os.environ.get("THREADS_PER_WORKER", 2))
    memory_limit: str = os.environ.get("MEMORY_LIMIT", "6GB")
    
    # Partitioning Strategy
    partition_size_mb: int = int(os.environ.get("PARTITION_SIZE_MB", 256))
    max_partitions_per_file: int = int(os.environ.get("MAX_PARTITIONS_PER_FILE", 50))
    batch_size: int = int(os.environ.get("BATCH_SIZE", 25))  # Files per batch
    
    # Schema Configuration
    leads_fields: List[str] = field(default_factory=lambda: [
        "id", "company_id", "name", "email", "email_score", "phone", "work_phone",
        "lead_location", "lead_divison", "lead_titles", "linkedin_url", "skills",
        "company_name", "company_size", "company_website", "company_phone_numbers",
        "company_location_text", "company_type", "company_function", "company_sector",
        "company_industry", "company_funding_range", "revenue_range",
        "company_facebook_page", "company_twitter_page", "company_linkedin_page",
        "company_products_services", "company_description"
    ])
    
    # Field Type Mappings
    json_fields: Dict[str, bool] = field(default_factory=lambda: {
        "lead_location": True,
        "skills": True,
        "company_phone_numbers": True,
    })
    
    integer_fields: Dict[str, bool] = field(default_factory=lambda: {
        "email_score": True
    })
    
    # Optimization Settings
    compression: str = os.environ.get("COMPRESSION", "snappy")
    enable_caching: bool = os.environ.get("ENABLE_CACHING", "true").lower() == "true"
    cache_dir: str = os.environ.get("CACHE_DIR", "./cache")
    
    # Monitoring and Resilience
    checkpoint_interval: int = int(os.environ.get("CHECKPOINT_INTERVAL", 100))
    max_retries: int = int(os.environ.get("MAX_RETRIES", 3))
    enable_progress_tracking: bool = True
    
    # DuckDB Optimization
    duckdb_memory_limit: str = os.environ.get("DUCKDB_MEMORY_LIMIT", "8GB")
    duckdb_threads: int = int(os.environ.get("DUCKDB_THREADS", 8))
    
    # Temporary directories
    temp_dir: str = os.environ.get("TEMP_DIR", "./tmp")
    
    def __post_init__(self):
        """Validate configuration after initialization."""
        # Ensure paths exist
        Path(self.cache_dir).mkdir(exist_ok=True)
        
        # Validate paths
        if not Path(self.json_root).exists():
            raise ValueError(f"JSON root path does not exist: {self.json_root}")
        
        # Auto-detect workers if not specified
        if self.max_workers is None:
            self.max_workers = min(os.cpu_count(), 16)  # Cap at 16 workers
            
        # Validate memory limits
        if not self.memory_limit.endswith(('GB', 'MB')):
            raise ValueError("Memory limit must end with 'GB' or 'MB'")


# Environment-specific configurations
DEVELOPMENT_CONFIG = PipelineConfig(
    max_workers=2,
    memory_limit="2GB",
    chunk_size_mb=32,
    batch_size=5,
    checkpoint_interval=10
)

PRODUCTION_CONFIG = PipelineConfig(
    max_workers=None,  # Auto-detect
    memory_limit="8GB",
    chunk_size_mb=256,
    batch_size=50,
    checkpoint_interval=500,
    duckdb_memory_limit="16GB",
    duckdb_threads=16
)

def get_config(env: str = None) -> PipelineConfig:
    """Get configuration for the specified environment."""
    env = env or os.environ.get("ENVIRONMENT", "production").lower()
    
    if env == "development":
        return DEVELOPMENT_CONFIG
    elif env == "production":
        return PRODUCTION_CONFIG
    else:
        return PipelineConfig()  # Default configuration