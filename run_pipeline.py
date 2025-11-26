#!/usr/bin/env python3
"""
Main runner script for the optimized data pipeline.
Provides command-line interface and example usage.
"""

import argparse
import logging
import sys
import time

# Import our optimized modules
from config import get_config, PipelineConfig
from optimized_pipeline import OptimizedDataPipeline
from init_optimized_duckdb import init_optimized_duckdb, get_table_stats

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log')
    ]
)

logger = logging.getLogger(__name__)


def run_pipeline(config: PipelineConfig, init_db: bool = True):
    """
    Run the complete ETL pipeline.
    
    Args:
        config: Pipeline configuration
        init_db: Whether to initialize the database first
    """
    start_time = time.time()
    
    try:
        # Initialize database if requested
        if init_db:
            logger.info("Initializing optimized DuckDB...")
            init_optimized_duckdb(
                db_path=config.db_path,
                table_name=config.table_name,
                memory_limit=config.duckdb_memory_limit,
                threads=config.duckdb_threads
            )
        
        # Create and run pipeline
        logger.info("Starting optimized data pipeline...")
        pipeline = OptimizedDataPipeline(config)
        pipeline.run()
        
        # Get final statistics
        stats = get_table_stats(config.db_path, config.table_name)
        
        execution_time = time.time() - start_time
        
        # Print summary
        logger.info("=" * 60)
        logger.info("PIPELINE EXECUTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total execution time: {execution_time:.2f} seconds")
        logger.info(f"Database: {config.db_path}")
        logger.info(f"Table: {config.table_name}")
        
        if stats.get("table_exists"):
            logger.info(f"Total rows processed: {stats['row_count']:,}")
            logger.info(f"Columns: {stats['column_count']}")
            
            # Data quality summary
            if stats.get("quality_metrics"):
                logger.info("\nData Quality Metrics:")
                for metric, value in stats["quality_metrics"].items():
                    logger.info(f"  {metric}: {value:.1f}%")
        
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


def main():
    """Main entry point with command-line interface."""
    parser = argparse.ArgumentParser(
        description="High-Performance Data Pipeline for JSON to DuckDB ETL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with development settings
  python run_pipeline.py --environment development
  
  # Run with custom JSON root
  python run_pipeline.py --json-root /path/to/data
  
  # Run with specific configuration
  python run_pipeline.py --max-workers 16 --memory-limit 16GB
  
  # Just show statistics
  python run_pipeline.py --stats-only
        """
    )
    
    # Environment and basic config
    parser.add_argument(
        "--environment", "-e", 
        choices=["development", "production"], 
        default="production",
        help="Environment configuration to use"
    )
    
    parser.add_argument(
        "--json-root", 
        type=str,
        help="Root directory containing JSON files"
    )
    
    parser.add_argument(
        "--db-path",
        type=str,
        help="Path to DuckDB database file"
    )
    
    # Performance tuning
    parser.add_argument(
        "--max-workers",
        type=int,
        help="Maximum number of Dask workers"
    )
    
    parser.add_argument(
        "--memory-limit",
        type=str,
        help="Memory limit per worker (e.g., '4GB')"
    )
    
    parser.add_argument(
        "--chunk-size-mb",
        type=int,
        help="Processing chunk size in MB"
    )
    
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Number of files per processing batch"
    )
    
    # Control options
    parser.add_argument(
        "--no-init",
        action="store_true",
        help="Skip database initialization"
    )
    
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Only show database statistics, don't run pipeline"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show configuration and exit without processing"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Get base configuration
        config = get_config(args.environment)
        
        # Override with command-line arguments
        if args.json_root:
            config.json_root = args.json_root
        if args.db_path:
            config.db_path = args.db_path
        if args.max_workers:
            config.max_workers = args.max_workers
        if args.memory_limit:
            config.memory_limit = args.memory_limit
        if args.chunk_size_mb:
            config.chunk_size_mb = args.chunk_size_mb
        if args.batch_size:
            config.batch_size = args.batch_size
        
        # Show configuration
        logger.info(f"Configuration: {args.environment}")
        logger.info(f"JSON Root: {config.json_root}")
        logger.info(f"Database: {config.db_path}")
        logger.info(f"Workers: {config.max_workers}")
        logger.info(f"Memory per worker: {config.memory_limit}")
        logger.info(f"Chunk size: {config.chunk_size_mb} MB")
        logger.info(f"Batch size: {config.batch_size} files")
        
        # Check if we should just show stats
        if args.stats_only:
            logger.info("Database statistics:")
            stats = get_table_stats(config.db_path, config.table_name)
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
            return
        
        # Check if dry run
        if args.dry_run:
            logger.info("Dry run completed - configuration shown above")
            return
        
        # Run the pipeline
        run_pipeline(config, init_db=not args.no_init)
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()