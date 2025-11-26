#!/usr/bin/env python3
"""
Updated database initialization with optimized schema for high-performance operations.
"""

import duckdb
import logging
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)


def init_optimized_duckdb(
    db_path: str = "./output/leads.db",
    table_name: str = "leads",
    memory_limit: str = "8GB",
    threads: int = 8
) -> None:
    """
    Initialize DuckDB with optimized settings for high-performance data ingestion.
    
    Args:
        db_path: Path to the DuckDB database file
        table_name: Name of the table to create
        memory_limit: Memory limit for DuckDB operations
        threads: Number of threads for parallel processing
    """
    try:
        # Connect to DuckDB
        conn = duckdb.connect(db_path)
        
        # Apply performance optimizations
        conn.execute(f"SET memory_limit='{memory_limit}'")
        conn.execute(f"SET threads={threads}")
        conn.execute("SET enable_progress_bar=true")
        conn.execute("SET preserve_insertion_order=false")
        conn.execute("SET enable_object_cache=true")
        
        # Create optimized table with simple schema (no primary key)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id VARCHAR,
            company_id VARCHAR,
            name VARCHAR,
            email VARCHAR,
            email_score BIGINT,
            phone VARCHAR,
            work_phone VARCHAR,
            lead_location VARCHAR,  -- JSON stored as VARCHAR for flexibility
            lead_divison VARCHAR,
            lead_titles VARCHAR,
            linkedin_url VARCHAR,
            skills VARCHAR,  -- JSON stored as VARCHAR
            company_name VARCHAR,
            company_size VARCHAR,
            company_website VARCHAR,
            company_phone_numbers VARCHAR,
            company_location_text VARCHAR,
            company_type VARCHAR,
            company_function VARCHAR,
            company_sector VARCHAR,
            company_industry VARCHAR,
            company_funding_range VARCHAR,
            revenue_range VARCHAR,
            company_facebook_page VARCHAR,
            company_twitter_page VARCHAR,
            company_linkedin_page VARCHAR,
            company_products_services VARCHAR,
            company_description VARCHAR
        )
        """
        
        conn.execute(create_table_sql)
        
        # Create performance-oriented indexes (no primary key needed)
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_email ON {table_name}(email)",
            f"CREATE INDEX IF NOT EXISTS idx_company_name ON {table_name}(company_name)",
            f"CREATE INDEX IF NOT EXISTS idx_company_id ON {table_name}(company_id)",
            f"CREATE INDEX IF NOT EXISTS idx_composite_company ON {table_name}(company_name, company_size)",
        ]
        
        for index_sql in indexes:
            try:
                conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Index creation failed (might already exist): {e}")
        
        conn.close()
        
        # Log success
        db_size = Path(db_path).stat().st_size if Path(db_path).exists() else 0
        logger.info(f"✓ Database initialized: {db_path} (Size: {db_size / 1024 / 1024:.1f} MB)")
        logger.info(f"✓ Table '{table_name}' created with optimized schema")
        logger.info(f"✓ Performance settings applied: {memory_limit} memory, {threads} threads")
        
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise


def get_table_stats(db_path: str, table_name: str = "leads") -> Dict[str, any]:
    """
    Get comprehensive statistics about the table.
    
    Args:
        db_path: Path to the DuckDB database
        table_name: Name of the table
        
    Returns:
        Dictionary with table statistics
    """
    try:
        conn = duckdb.connect(db_path, read_only=True)
        
        # Basic table info
        count_result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        row_count = count_result[0] if count_result else 0
        
        # Schema info
        schema_result = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        columns = [row[1] for row in schema_result]
        
        # Sample data quality metrics
        quality_metrics = {}
        for col in ["email", "company_name", "name"]:
            if col in columns:
                null_count = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL").fetchone()[0]
                quality_metrics[f"{col}_null_percentage"] = (null_count / row_count * 100) if row_count > 0 else 0
        
        conn.close()
        
        return {
            "row_count": row_count,
            "column_count": len(columns),
            "columns": columns,
            "quality_metrics": quality_metrics,
            "table_exists": True
        }
        
    except Exception as e:
        logger.error(f"Failed to get table stats: {e}")
        return {"table_exists": False, "error": str(e)}


if __name__ == "__main__":
    """Command-line interface for database initialization."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Initialize optimized DuckDB for leads data")
    parser.add_argument("--db-path", default="leads_optimized.db", help="Database file path")
    parser.add_argument("--table-name", default="leads", help="Table name")
    parser.add_argument("--memory-limit", default="8GB", help="Memory limit")
    parser.add_argument("--threads", type=int, default=8, help="Number of threads")
    parser.add_argument("--stats", action="store_true", help="Show table statistics")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    
    if args.stats:
        stats = get_table_stats(args.db_path, args.table_name)
        print(f"\nTable Statistics for '{args.table_name}':")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    else:
        init_optimized_duckdb(
            db_path=args.db_path,
            table_name=args.table_name,
            memory_limit=args.memory_limit,
            threads=args.threads
        )