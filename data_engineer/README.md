# Building Materials Sales - ETL Pipeline

**Data Engineering Project** for procurement intelligence analytics.

## Overview

ETL pipeline for loading and transforming building materials sales data (2019-2024) into a PostgreSQL Star Schema database.

### Data Sources

| Dataset                               | Rows    | Description               |
| ------------------------------------- | ------- | ------------------------- |
| `building_materials_transactions.csv` | 340,000 | Weekly sales transactions |
| `macro_drivers_weekly.csv`            | 313     | Macroeconomic indicators  |
| `data_dictionary_materials.csv`       | 15      | Data dictionary           |

### Target Schema

Star Schema with:

- **Fact table**: `fact_sales` (340K rows)
- **Dimension tables**: `dim_date`, `dim_product`, `dim_region`, `dim_channel`, `dim_customer`, `dim_category`, `dim_macro`

## Project Structure

```bash
data_engineer/
├── README.md             # <- This documentation
├── TASK.md               # <- Project task
├── db/schemas/           # <- PostgreSQL Star Schema
├── src/etl/              # <- ETL pipeline код
│   │   ├── extract.py    # CSV → DataFrame
│   │   ├── transform.py  # Validation & cleaning
│   │   ├── load.py       # DB loading + DLQ
│   │   └── main.py       # Pipeline orchestrator
├── tests/
│   └── test_etl.py       # 18 unit tests
├── notebooks/
│   └── 01_eda_analysis.ipynb
└── data/
    └── *.csv             # Source data files
```

## Quick Start

### 1. Install Dependencies

```bash
pip install pandas numpy psycopg2-binary pytest
```

### 2. Download Data

```bash
# Download from Kaggle
# https://www.kaggle.com/datasets/sergionefedov/us-building-materials-sales-transactions-20192024

# Place files in data_engineer/data/
```

### 3. Run ETL Pipeline

**Dry-run mode** (extract + transform only):

```bash
cd data_engineer
python -m src.etl.main
```

**Full mode** (with database write):

```bash
cd data_engineer
python -m src.etl.main --db
```

**Incremental mode** (only new data):

```bash
cd data_engineer
python -m src.etl.main --db --incremental
```

**Full reload** (ignore watermark):

```bash
cd data_engineer
python -m src.etl.main --db --full
```

### 4. Run Tests

```bash
pytest data_engineer/tests/test_etl.py -v
```

## Configuration

Edit `src/config/settings.py`:

```python
# Database connection
db_config = DatabaseConfig(
    host="localhost",
    port=5432,
    database="building_materials",
    user="postgres",
    password="your_password"
)

# Validation rules
validation_rules = DataValidationRules(
    units_min=0,
    unit_price_min=0.01,
    lumber_price_max=2000.0
)
```

## ETL Pipeline Phases

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   SOURCE    │────▶│   STAGING   │────▶│     RAW     │
│   (CSV)     │     │  (raw copy) │     │ (validated) │
└─────────────┘     └─────────────┘     └─────────────┘
                                              │
                                              ▼
                                         ┌─────────────┐
                                         │  PROCESSED   │
                                         │  (Star Schema)│
                                         └─────────────┘
```

### Phase 1: Extract

- Load CSV files into pandas DataFrames
- Validate file existence
- Generate extraction summary

### Phase 2: Transform

- Validate data types
- Check ranges (units, prices, macro indicators)
- Validate categorical values
- Extract SKU prefix
- Handle errors via Dead Letter Queue

### Phase 3: Load

- Connect to PostgreSQL
- Load in batches with transaction support
- UPSERT for incremental updates
- Rollback on critical errors

## Incremental Loading

```python
from data_engineer.src.config.settings import get_watermark, set_watermark

# Check current watermark
print(get_watermark())  # "2024-12-30" or None

# Reset for full reload
from data_engineer.src.etl.load import IncrementalLoader
IncrementalLoader().reset_watermark()
```

## Error Handling

### Dead Letter Queue

Failed records are logged to `data/errors/dead_letter_queue.jsonl`:

```json
{"row_index": 0, "column": "units", "error_type": "OUT_OF_RANGE", ...}
```

### Transaction Rollback

If a critical error occurs during load, all changes are rolled back:

```python
try:
    load_batch(batch)
    db.commit()
except Exception as e:
    db.rollback()  # All changes reverted
```

## Data Quality Issues Identified

| Issue                             | Severity | Recommendation       |
| --------------------------------- | -------- | -------------------- |
| 60 business key duplicates        | High     | UPSERT aggregation   |
| 15,978 lumber price outliers      | Medium   | Flag during analysis |
| season_factor not in transactions | Medium   | Join via date        |
| High SKU cardinality (84K)        | Medium   | Separate dim_product |

## Testing

```bash
# Run all tests
pytest data_engineer/tests/test_etl.py -v

# Run specific test
pytest data_engineer/tests/test_etl.py::TestIncrementalLoader -v

# Run with coverage
pytest data_engineer/tests/test_etl.py --cov=src.etl
```

## Database Schema

```sql
-- Star Schema (see db/schemas/01_star_schema.sql for full DDL)

-- Dimension Tables (3NF)
dim_date, dim_product, dim_region, dim_channel, dim_customer, dim_category

-- Macro Dimension
dim_macro (housing_starts_index, lumber_price_index, mortgage_rate, season_factor)

-- Fact Table (Denormalized)
fact_sales (transaction_id, date_id, product_id, region_id,
            channel_id, customer_id, macro_id, units, unit_price, revenue)

-- Error Handling
etl_errors (dead letter queue)
etl_batch_log (execution history)
etl_watermarks (incremental tracking)
```

## Requirements

- Python 3.10+
- PostgreSQL 14+ (for full pipeline)
- pandas, numpy, psycopg2-binary, pytest

## License

MIT License

## Authors

Data Engineering Project
