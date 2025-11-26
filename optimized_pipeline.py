#!/usr/bin/env python3
"""
High-Performance Dask Data Pipeline for Large-Scale JSON to DuckDB ETL
Optimized for handling terabytes of data with modern data engineering principles.
"""

import os
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Tuple, Any, Iterator
from dataclasses import dataclass, field
import hashlib

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import dask
from dask.distributed import Client, as_completed, get_worker
from dask.utils import parse_bytes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PipelineConfig:
    """Configuration class for the data pipeline."""
    
    # Input/Output paths
    json_root: str = r"D:\Datasets\clean\final\batch1"
    db_path: str = "leads.db"
    table_name: str = "leads"
    
    # Performance tuning
    chunk_size_mb: int = 64  # Size of each processing chunk in MB
    max_workers: int = None  # Auto-detect based on cores
    threads_per_worker: int = 2
    memory_limit: str = "4GB"  # Per worker memory limit
    
    # Partitioning strategy
    partition_size_mb: int = 128  # Target partition size
    max_partitions_per_file: int = 100
    
    # Schema and field definitions
    leads_fields: List[str] = field(default_factory=lambda: [
        "id", "company_id", "name", "email", "email_score", "phone", "work_phone",
        "lead_location", "lead_divison", "lead_titles", "linkedin_url", "skills",
        "company_name", "company_size", "company_website", "company_phone_numbers",
        "company_location_text", "company_type", "company_function", "company_sector",
        "company_industry", "company_funding_range", "revenue_range",
        "company_facebook_page", "company_twitter_page", "company_linkedin_page",
        "company_products_services", "company_description"
    ])
    
    json_fields: Dict[str, bool] = field(default_factory=lambda: {
        "lead_location": True,
        "skills": True,
    })
    
    integer_fields: Dict[str, bool] = field(default_factory=lambda: {
        "email_score": True
    })
    
    # Caching and optimization
    enable_caching: bool = True
    cache_dir: str = "./cache"
    compression: str = "snappy"  # parquet compression
    
    # Monitoring and resilience
    checkpoint_interval: int = 1000  # Checkpoint every N files
    max_retries: int = 3
    enable_progress_tracking: bool = True


class SchemaManager:
    """Manages data schema validation and type coercion."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self._arrow_schema = None
        
    @property
    def arrow_schema(self) -> pa.Schema:
        """Get PyArrow schema for the leads table."""
        if self._arrow_schema is None:
            fields = []
            for field_name in self.config.leads_fields:
                if field_name in self.config.integer_fields:
                    field_type = pa.int64()
                elif field_name in self.config.json_fields:
                    field_type = pa.string()  # JSON stored as string
                else:
                    field_type = pa.string()
                    
                fields.append(pa.field(field_name, field_type, nullable=True))
            
            self._arrow_schema = pa.schema(fields)
            
        return self._arrow_schema
    
    def validate_parquet_schema(self, parquet_file: Path) -> bool:
        """Validate that a parquet file has the correct schema."""
        try:
            table = pq.read_table(parquet_file)
            expected_columns = set(self.config.leads_fields)
            actual_columns = set(table.column_names)
            
            # Check for extra columns (should not have idx or any others)
            extra_columns = actual_columns - expected_columns
            if extra_columns:
                logger.error(f"Parquet file {parquet_file} has extra columns: {extra_columns}")
                debug_schema_mismatch(parquet_file, self.config.leads_fields)
                return False
                
            # Check for missing columns
            missing_columns = expected_columns - actual_columns
            if missing_columns:
                logger.error(f"Parquet file {parquet_file} missing columns: {missing_columns}")
                debug_schema_mismatch(parquet_file, self.config.leads_fields)
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to validate schema for {parquet_file}: {e}")
            return False


class DataNormalizer:
    """High-performance data normalization with caching."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        
    def extract_value_recursive(self, value: Any) -> Any:
        """Recursively extract nested 'value' fields."""
        while isinstance(value, dict) and "value" in value:
            value = value["value"]
        return value
    
    def normalize_row(self, raw_row: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a single row with optimized field processing."""
        clean = {}
        
        for field_name in self.config.leads_fields:
            val = raw_row.get(field_name)
            
            # Handle JSON fields
            if field_name in self.config.json_fields:
                if val is None:
                    clean[field_name] = None
                else:
                    try:
                        clean[field_name] = json.dumps(val, ensure_ascii=False, separators=(',', ':'))
                    except (TypeError, ValueError):
                        clean[field_name] = None
                continue
            
            # Recursively extract nested values
            if isinstance(val, dict):
                extracted = self.extract_value_recursive(val)
                if isinstance(extracted, (dict, list)):
                    try:
                        clean[field_name] = json.dumps(extracted, ensure_ascii=False, separators=(',', ':'))
                    except (TypeError, ValueError):
                        clean[field_name] = str(extracted) if extracted else None
                else:
                    clean[field_name] = extracted
                continue
            
            # Handle lists
            if isinstance(val, list):
                try:
                    clean[field_name] = json.dumps(val, ensure_ascii=False, separators=(',', ':'))
                except (TypeError, ValueError):
                    clean[field_name] = str(val) if val else None
                continue
            
            # Scalar values
            clean[field_name] = val
            
        return clean


class FileProcessor:
    """Optimized file processing with parallel I/O."""
    
    def __init__(self, config: PipelineConfig, normalizer: DataNormalizer):
        self.config = config
        self.normalizer = normalizer
        
    def get_file_hash(self, filepath: Path) -> str:
        """Generate a hash for file to enable caching."""
        stat = filepath.stat()
        content = f"{filepath.as_posix()}:{stat.st_size}:{stat.st_mtime}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def process_json_file(self, filepath: Path) -> Iterator[Dict[str, Any]]:
        """Process a single JSON file with memory-efficient streaming."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                logger.warning(f"Unexpected data type in {filepath}: {type(data)}")
                return
                
            for row in data:
                if isinstance(row, dict):
                    normalized = self.normalizer.normalize_row(row)
                    yield normalized
                    
        except Exception as e:
            logger.error(f"Failed to process {filepath}: {e}")


class DuckDBManager:
    """Optimized DuckDB operations with connection pooling."""
    
    def __init__(self, config: PipelineConfig, schema_manager: SchemaManager):
        self.config = config
        self.schema_manager = schema_manager
        
    def init_database(self):
        """Initialize DuckDB with optimized settings."""
        conn = duckdb.connect(self.config.db_path)
        
        # Optimize DuckDB settings for large data
        conn.execute("SET memory_limit='8GB'")
        conn.execute("SET threads=8")
        conn.execute("SET enable_progress_bar=true")
        conn.execute("SET preserve_insertion_order=false")
        
        # Create optimized table schema (no primary key needed)
        schema_fields = []
        for schema_field in self.schema_manager.arrow_schema:
            field_name = schema_field.name
            if field_name in self.config.integer_fields:
                schema_fields.append(f"{field_name} BIGINT")
            else:
                schema_fields.append(f"{field_name} VARCHAR")
                
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.config.table_name} (
            {', '.join(schema_fields)}
        )
        """
        
        conn.execute(create_sql)
        
        # Create indexes for common query patterns
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_email ON {self.config.table_name}(email)")
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_company ON {self.config.table_name}(company_name)")
        except Exception:
            pass  # Indexes might already exist
            
        conn.close()
        logger.info("Database initialized with optimized settings")
    
    def bulk_insert_parquet(self, parquet_files: List[Path]) -> Tuple[int, int]:
        """Simple bulk insert without primary key constraints - can run in parallel."""
        successful = 0
        failed = 0
        
        # Now that we removed primary keys, we can process multiple files simultaneously
        # But keep it simple with a small batch size to avoid too many connections
        batch_size = min(5, len(parquet_files))  # Max 5 parallel connections
        
        for i in range(0, len(parquet_files), batch_size):
            batch = parquet_files[i:i + batch_size]
            
            # Process this batch in parallel
            from concurrent.futures import ThreadPoolExecutor
            
            def insert_single_file(parquet_file):
                conn = None
                try:
                    conn = duckdb.connect(self.config.db_path)
                    
                    # Validate parquet schema before insertion
                    if not self.schema_manager.validate_parquet_schema(parquet_file):
                        logger.error(f"Schema validation failed for {parquet_file}, skipping")
                        return False
                    
                    # Simple direct insert - no primary key conflicts possible
                    field_list = ", ".join([f'"{field}"' for field in self.schema_manager.config.leads_fields])
                    insert_sql = f"""
                    INSERT INTO {self.config.table_name} ({field_list})
                    SELECT {field_list}
                    FROM read_parquet('{parquet_file.as_posix()}')
                    """
                    
                    conn.execute(insert_sql)
                    logger.debug(f"Successfully inserted {parquet_file.name}")
                    return True
                    
                except Exception as e:
                    logger.error(f"Failed to insert {parquet_file}: {e}")
                    return False
                finally:
                    if conn:
                        conn.close()
            
            # Execute batch in parallel
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                results = list(executor.map(insert_single_file, batch))
                
            successful += sum(results)
            failed += len(results) - sum(results)
            
            logger.info(f"Completed batch {i//batch_size + 1}: {sum(results)}/{len(results)} successful")
        
        return successful, failed


def create_arrow_table_from_rows(rows: List[Dict[str, Any]], schema: pa.Schema) -> pa.Table:
    """Convert normalized rows to PyArrow table with schema validation."""
    if not rows:
        return pa.table([], schema=schema)
    
    # Prepare data arrays - only for fields in schema (no idx)
    arrays = []
    for schema_field in schema:
        field_name = schema_field.name
        # Note: idx field is not in the schema anymore, so this check is not needed
        
        values = []
        for row in rows:
            val = row.get(field_name)
            if val is None:
                values.append(None)
            elif schema_field.type == pa.int64():
                try:
                    values.append(int(val))
                except (ValueError, TypeError):
                    values.append(None)
            else:
                values.append(str(val) if val is not None else None)
                
        arrays.append(pa.array(values, type=schema_field.type))
    
    # Create table and validate
    table = pa.table(arrays, schema=schema)
    
    # Log schema info for debugging
    logger.debug(f"Created PyArrow table with {len(table.columns)} columns: {table.column_names}")
    
    return table


def debug_schema_mismatch(parquet_file: Path, expected_fields: List[str]):
    """Debug utility to analyze schema mismatches."""
    try:
        table = pq.read_table(parquet_file)
        
        logger.error("=== SCHEMA MISMATCH DEBUG ===")
        logger.error(f"File: {parquet_file}")
        logger.error(f"Expected columns ({len(expected_fields)}): {expected_fields}")
        logger.error(f"Actual columns ({len(table.column_names)}): {table.column_names}")
        
        # Show differences
        expected_set = set(expected_fields)
        actual_set = set(table.column_names)
        
        extra = actual_set - expected_set
        missing = expected_set - actual_set
        
        if extra:
            logger.error(f"Extra columns: {extra}")
        if missing:
            logger.error(f"Missing columns: {missing}")
            
        logger.error("=== END DEBUG ===")
        
    except Exception as e:
        logger.error(f"Failed to debug schema for {parquet_file}: {e}")
def process_file_batch(file_paths: List[Path], config: PipelineConfig) -> List[Path]:
    """Process a batch of files on a Dask worker."""
    worker = get_worker()
    worker_id = worker.id if worker else "unknown"
    
    normalizer = DataNormalizer(config)
    processor = FileProcessor(config, normalizer)
    schema_manager = SchemaManager(config)
    
    output_files = []
    
    for file_path in file_paths:
        try:
            # Process file and collect rows
            rows = list(processor.process_json_file(file_path))
            if not rows:
                continue
                
            # Convert to Arrow table
            table = create_arrow_table_from_rows(rows, schema_manager.arrow_schema)
            
            # Write to parquet
            #output_path = Path(f".\tmp\processed_{worker_id}_{file_path.stem}_{int(time.time())}.parquet")
            BASE_TMP = Path(r"./tmp")   # <— hard-coded Windows path
            output_path = BASE_TMP / f"processed_{worker_id}_{file_path.stem}_{int(time.time())}.parquet"
            pq.write_table(table, output_path, compression=config.compression)
            output_files.append(output_path)
            
        except Exception as e:
            logger.error(f"Worker {worker_id} failed to process {file_path}: {e}")
            
    return output_files


class OptimizedDataPipeline:
    """Main pipeline class orchestrating the entire ETL process."""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.schema_manager = SchemaManager(config)
        self.duckdb_manager = DuckDBManager(config, self.schema_manager)
        
        # Initialize Dask client with optimized settings
        self._init_dask_client()
        
    def _init_dask_client(self):
        """Initialize Dask client with performance optimizations."""
        dask.config.set({
            'distributed.worker.memory.target': 0.8,
            'distributed.worker.memory.spill': 0.9,
            'distributed.worker.memory.pause': 0.95,
            'distributed.worker.memory.terminate': 0.98,
            'distributed.comm.timeouts.connect': 60,
            'distributed.comm.timeouts.tcp': 60,
        })
        
        n_workers = self.config.max_workers or os.cpu_count()
        
        self.client = Client(
            n_workers=n_workers,
            threads_per_worker=self.config.threads_per_worker,
            memory_limit=self.config.memory_limit,
            dashboard_address=':5000'
        )
        
        logger.info(f"Dask client initialized: {self.client}")
    
    def discover_files(self) -> List[Path]:
        """Efficiently discover all JSON files."""
        json_files = []
        root_path = Path(self.config.json_root)
        
        if not root_path.exists():
            raise ValueError(f"JSON root path does not exist: {root_path}")
            
        for json_file in root_path.rglob("*.json"):
            if json_file.is_file():
                json_files.append(json_file)
                
        logger.info(f"Discovered {len(json_files)} JSON files")
        return json_files
    
    def batch_files(self, files: List[Path], batch_size: int) -> List[List[Path]]:
        """Create balanced batches of files for processing."""
        # Calculate batch size based on average file size
        total_size = sum(f.stat().st_size for f in files)
        avg_file_size = total_size / len(files) if files else 0
        target_batch_size_bytes = parse_bytes(f"{self.config.chunk_size_mb}MB")
        
        if avg_file_size > 0:
            optimal_batch_size = max(1, int(target_batch_size_bytes / avg_file_size))
            batch_size = min(batch_size, optimal_batch_size)
        
        batches = []
        for i in range(0, len(files), batch_size):
            batches.append(files[i:i + batch_size])
            
        logger.info(f"Created {len(batches)} batches with target size {batch_size} files each")
        return batches
    
    def run(self):
        """Execute the complete ETL pipeline."""
        start_time = time.time()
        
        try:
            # Initialize database
            self.duckdb_manager.init_database()
            
            # Discover files
            json_files = self.discover_files()
            if not json_files:
                logger.warning("No JSON files found to process")
                return
            
            # Create file batches
            file_batches = self.batch_files(json_files, batch_size=50)
            
            # Submit processing jobs to Dask
            logger.info(f"Submitting {len(file_batches)} batches for processing")
            futures = []
            
            for batch in file_batches:
                future = self.client.submit(
                    process_file_batch, 
                    batch, 
                    self.config,
                    pure=False  # Allow caching
                )
                futures.append(future)
            
            # Process results as they complete
            all_parquet_files = []
            completed = 0
            
            for future in as_completed(futures):
                try:
                    parquet_files = future.result()
                    all_parquet_files.extend(parquet_files)
                    completed += 1
                    
                    if completed % 10 == 0:
                        logger.info(f"Completed {completed}/{len(futures)} batches")
                        
                except Exception as e:
                    logger.error(f"Batch processing failed: {e}")
            
            # Bulk insert into DuckDB
            if all_parquet_files:
                logger.info(f"Inserting {len(all_parquet_files)} parquet files into DuckDB")
                successful, failed = self.duckdb_manager.bulk_insert_parquet(all_parquet_files)
                logger.info(f"DuckDB insert results: {successful} successful, {failed} failed")
                
                # Cleanup temporary files
                for pf in all_parquet_files:
                    try:
                        pf.unlink()
                    except Exception:
                        pass
            
            execution_time = time.time() - start_time
            logger.info(f"Pipeline completed in {execution_time:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.client.close()


def main():
    """Main entry point."""
    # Configuration
    config = PipelineConfig(
        json_root=r"D:\Datasets\clean\final\batch5",  # Update this path
        db_path="leads.db",
        chunk_size_mb=256,
        max_workers=8,
        memory_limit="12GB"
    )
    
    # Run pipeline
    pipeline = OptimizedDataPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()