
## Architecture

```
JSON Files → Dask Workers → PyArrow Tables → DuckDB
     ↓              ↓              ↓           ↓
File Discovery → Parallel ETL → Schema → Bulk Insert
```

### Components

1. **OptimizedDataPipeline**: Main orchestrator class
2. **SchemaManager**: Handles data validation and type coercion  
3. **DataNormalizer**: High-performance data cleaning and normalization
4. **DuckDBManager**: Optimized database operations with bulk loading
5. **PipelineConfig**: Environment-specific configuration management

## 🛠️ Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Alternative: Install specific versions for production
pip install "dask[complete]>=2023.12.0" "pyarrow>=14.0.0" "duckdb>=0.9.0"
```
