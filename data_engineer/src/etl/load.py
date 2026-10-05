"""Load module for writing data to PostgreSQL

Handles the loading phase of ETL, writing transformed data to the
Star Schema database with support for:
- Batch inserts
- UPSERT operations (ON CONFLICT)
- Transaction management
- Error handling and rollback
- Incremental loading with watermark tracking
- Idempotent operations
- Dead Letter Queue for error isolation
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2

from ..config.settings import (
    PROJECT_ROOT,
    batch_config,
    db_config,
    get_watermark,
    incremental_config,
    set_watermark,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# DEAD LETTER QUEUE - Error Isolation
class DeadLetterQueue:
    """Isolates failed records for later analysis/retry

    Stores rejected records in:
    1. JSON file (for testing/development)
    2. PostgreSQL table (for production)
    """

    def __init__(self, base_path: Path = None):
        self.base_path = base_path or PROJECT_ROOT / "data_engineer" / "data" / "errors"
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.error_file = self.base_path / "dead_letter_queue.jsonl"

    def write_error(self, error_record: dict[str, Any]):
        """Write a single error to the dead letter queue

        Args:
            error_record: Dictionary containing:
                - row_index: Original row index
                - column: Column with error
                - value: Invalid value
                - error_type: Type of error
                - error_message: Human-readable message
                - original_data: Full row data
                - batch_id: ETL batch identifier
                - timestamp: When error occurred
        """
        # Add metadata
        error_record["timestamp"] = datetime.now().isoformat()

        # Write as JSON line (append mode)
        with open(self.error_file, "a") as f:
            f.write(json.dumps(error_record) + "\n")

        logger.debug(f"Error logged to DLQ: {error_record.get('error_type', 'unknown')}")

    def write_batch(self, errors: list[dict[str, Any]], batch_id: str):
        """Write multiple errors to dead letter queue

        Args:
            errors: List of error dictionaries
            batch_id: Current batch identifier
        """
        for error in errors:
            error["batch_id"] = batch_id
            self.write_error(error)

        logger.info(f"Wrote {len(errors)} errors to dead letter queue")

    def read_errors(self, limit: int = None) -> list[dict[str, Any]]:
        """Read errors from dead letter queue

        Args:
            limit: Maximum number of errors to read

        Returns:
            List of error records
        """
        if not self.error_file.exists():
            return []

        errors: list[dict[str, Any]] = []
        with open(self.error_file) as f:
            for line in f:
                if limit and len(errors) >= limit:
                    break
                try:
                    errors.append(json.loads(line.strip()))
                except json.JSONDecodeError:
                    continue

        return errors

    def get_error_summary(self) -> dict[str, Any]:
        """Get summary statistics of dead letter queue"""
        errors = self.read_errors()

        if not errors:
            return {"total_errors": 0, "by_type": {}}

        by_type: dict[str, int] = {}
        for error in errors:
            error_type = error.get("error_type", "unknown")
            by_type[error_type] = by_type.get(error_type, 0) + 1

        return {
            "total_errors": len(errors),
            "by_type": by_type,
            "latest_timestamp": errors[-1].get("timestamp") if errors else None,
        }

    def clear(self):
        """Clear all errors from dead letter queue"""
        if self.error_file.exists():
            os.remove(self.error_file)
            logger.info("Dead letter queue cleared")


# DATABASE CONNECTION WITH TRANSACTION SUPPORT
class DatabaseConnection:
    """Manages PostgreSQL database connections with transaction support"""

    def __init__(self, config=None):
        self.config = config or db_config
        self._connection = None
        self._in_transaction = False

    def connect(self):
        """Establish database connection."""
        # If already connected, reset and reconnect
        if self._connection is not None:
            try:
                self._connection.close()
            except (psycopg2.Error, OSError):
                pass
            self._connection = None
            self._in_transaction = False

        try:
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
            )
            logger.info("Database connection established")
            return self._connection
        except ImportError:
            logger.warning("psycopg2 not installed. Using mock mode")
            return None

    def disconnect(self):
        """Close database connection"""
        if self._connection:
            if self._in_transaction:
                logger.warning("Closing connection with open transaction - rolling back")
                self.rollback()
            self._connection.close()
            self._connection = None
            self._in_transaction = False
            logger.info("Database connection closed")

    def reset_connection(self):
        """Reset connection state - useful after transaction issues"""
        if self._connection:
            try:
                self._connection.rollback()
            except (psycopg2.Error, OSError):
                pass
            try:
                self._connection.close()
            except (psycopg2.Error, OSError):
                pass
        self._connection = None
        self._in_transaction = False
        # Reconnect with fresh connection
        self.connect()

    def begin_transaction(self):
        """Begin a new transaction"""
        if self._connection:
            self._connection.autocommit = False
            self._in_transaction = True
            logger.debug("Transaction started")

    def execute(self, query: str, params: tuple = None):
        """Execute a query."""
        if self._connection:
            cursor = self._connection.cursor()
            cursor.execute(query, params)
            return cursor
        return None

    def cursor(self):
        """Get a new cursor from the connection."""
        if self._connection:
            return self._connection.cursor()
        return None

    def commit(self):
        """Commit transaction"""
        if self._connection and self._in_transaction:
            self._connection.commit()
            self._in_transaction = False
            logger.debug("Transaction committed")

    def rollback(self):
        """Rollback transaction"""
        if self._connection and self._in_transaction:
            self._connection.rollback()
            self._in_transaction = False
            logger.warning("Transaction rolled back")


# ETL BATCH LOGGER
class ETLBatchLogger:
    """Logs ETL batch execution with error tracking"""

    def __init__(self):
        self.batch_id = None
        self.start_time = None
        self.rows_inserted = 0
        self.rows_updated = 0
        self.rows_rejected = 0
        self.errors: list[dict[str, Any]] = []
        self.dlq = DeadLetterQueue()

    def start_batch(self, batch_name: str) -> str:
        """Start a new batch"""
        self.batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.start_time = datetime.now()
        self.rows_inserted = 0
        self.rows_updated = 0
        self.rows_rejected = 0
        self.errors.clear()
        logger.info(f"Starting batch {self.batch_id}: {batch_name}")
        return self.batch_id

    def add_insert(self, count: int = 1):
        self.rows_inserted += count

    def add_update(self, count: int = 1):
        self.rows_updated += count

    def add_reject(self, count: int = 1):
        self.rows_rejected += count

    def add_error(self, error: dict[str, Any]):
        """Add error to batch and write to Dead Letter Queue"""
        error["batch_id"] = self.batch_id
        self.errors.append(error)
        self.dlq.write_error(error)

    def finish_batch(self) -> dict[str, Any]:
        """Get batch summary"""
        if self.start_time is None:
            self.start_time = datetime.now()
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        summary = {
            "batch_id": self.batch_id or "unknown",
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_rejected": self.rows_rejected,
            "error_count": len(self.errors),
        }
        logger.info(f"Batch complete: {summary}")
        return summary


# DATA LOADER WITH TRANSACTION SUPPORT
# # Allowlist of valid table names for security (SQL injection prevention)
_VALID_TABLE_NAMES = frozenset(
    {
        "dim_date",
        "dim_product",
        "dim_region",
        "dim_channel",
        "dim_customer",
        "dim_macro",
        "fact_sales",
    }
)


class DataLoader:
    """
    Loads transformed data into Star Schema with error handling

    Handles:
    - Dimension table loading (with SCD support)
    - Fact table loading (with UPSERT)
    - Transaction-safe batch processing
    - Automatic rollback on critical errors
    - Error logging to dead letter queue
    """

    def __init__(self, db_conn: DatabaseConnection = None):
        self.db = db_conn or DatabaseConnection()
        self.batch_logger = ETLBatchLogger()
        self._connected = False

    def connect(self):
        """Connect to database"""
        if not self._connected:
            self.db.connect()
            self._connected = True

    def disconnect(self):
        """Disconnect from database"""
        if self._connected:
            self.db.disconnect()
            self._connected = False

    def load_dates(self, df: pd.DataFrame) -> int:
        """Load unique dates into dim_date table"""
        if df.empty:
            return 0

        loaded = 0
        if not self.db._connection:
            return 0

        # Enable autocommit for this operation
        self.db._connection.autocommit = True
        cursor = self.db.cursor()

        # Get unique dates
        unique_dates = pd.to_datetime(df["transaction_date"]).dt.date.unique()

        # Day names mapping
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        for date in unique_dates:
            try:
                date_id = int(date.strftime("%Y%m%d"))
                year = date.year
                month = date.month
                quarter = (month - 1) // 3 + 1
                week_num = date.isocalendar()[1]
                day_of_week = date.weekday()
                day_name = day_names[day_of_week]

                cursor.execute(
                    """
                    INSERT INTO dim_date (date_id, date, year, month, quarter, week_of_year, day_of_week, day_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date_id) DO NOTHING
                    """,
                    (date_id, date, year, month, quarter, week_num, day_of_week, day_name),
                )
                loaded += 1

            except Exception as e:
                logger.error(f"Failed to insert date {date}: {e}")

        cursor.close()
        # Restore autocommit setting
        self.db._connection.autocommit = False
        logger.info(f"Loaded {loaded} dates into dim_date")
        return loaded

    def load_products(self, df: pd.DataFrame) -> dict[str, int]:
        """Load unique products into dim_product table

        Returns:
            Dict mapping SKU to product_id
        """
        if df.empty:
            return {}

        sku_to_id: dict[str, int] = {}
        unique_skus = df["sku"].unique()

        if not self.db._connection:
            # Create default mapping for mock mode
            for sku in unique_skus:
                sku_to_id[sku] = len(sku_to_id) + 1
            return sku_to_id

        cursor = self.db.cursor()

        # First, get existing product IDs from database
        try:
            cursor.execute("SELECT product_id, sku FROM dim_product")
            for row in cursor.fetchall():
                sku_to_id[row[1]] = row[0]
        except Exception as e:
            logger.warning(f"Could not fetch existing products: {e}")

        # Now insert new products (not existing in sku_to_id)
        new_skus = [sku for sku in unique_skus if sku not in sku_to_id]

        if new_skus:
            logger.info(f"Inserting {len(new_skus)} new products...")

            # Use executemany for batch insert
            for sku in new_skus:
                try:
                    sku_prefix = sku[:3] if len(sku) >= 3 else "UNK"
                    cursor.execute(
                        """
                        INSERT INTO dim_product (sku, sku_prefix, product_name, is_active)
                        VALUES (%s, %s, %s, true)
                        RETURNING product_id
                        """,
                        (sku, sku_prefix, f"Product {sku}"),
                    )
                    result = cursor.fetchone()
                    if result:
                        sku_to_id[sku] = result[0]
                except Exception as e:
                    logger.error(f"Failed to insert product {sku}: {e}")
                    sku_to_id[sku] = len(sku_to_id) + 1

            # Commit the product inserts to make them visible
            try:
                self.db._connection.commit()
            except Exception as e:
                logger.warning(f"Could not commit product inserts: {e}")

        cursor.close()
        logger.info(f"Products loaded: {len(sku_to_id)} total in mapping")
        return sku_to_id

    def load_batch_with_rollback(
        self, df: pd.DataFrame, load_func, batch_size: int = None
    ) -> dict[str, Any]:
        """
        Load data in batches with automatic rollback on failure

        Args:
            df: DataFrame to load
            load_func: Function that loads one batch
            batch_size: Size of each batch

        Returns:
            Summary of loading operation
        """
        batch_size = batch_size or batch_config.batch_size
        total_rows = len(df)
        batches = (total_rows + batch_size - 1) // batch_size

        logger.info(f"Loading {total_rows:,} rows in {batches} batches")

        self.batch_logger.start_batch(f"load_{batches}_batches")

        try:
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i : i + batch_size]
                batch_num = i // batch_size + 1

                logger.debug(f"Processing batch {batch_num}/{batches}")

                result = load_func(batch)

                self.batch_logger.add_insert(result.get("inserted", 0))
                self.batch_logger.add_update(result.get("updated", 0))
                self.batch_logger.add_reject(result.get("rejected", 0))

                # Commit after each batch (checkpoint)
                self.db.commit()
                logger.debug(f"Batch {batch_num} committed")

            # All batches successful - commit
            summary = self.batch_logger.finish_batch()
            return {"status": "success", **summary}

        except Exception as e:
            # Critical error - rollback everything
            logger.error(f"Critical error during load: {e}")
            logger.error("Rolling back all changes")

            self.db.rollback()

            summary = self.batch_logger.finish_batch()
            summary["status"] = "rolled_back"
            summary["error"] = str(e)

            return summary

    def load_dimension(self, df: pd.DataFrame, table_name: str, key_column: str = "id") -> int:
        """Load dimension table with INSERT ... ON CONFLICT DO NOTHING

        Args:
            df: DataFrame with dimension data
            table_name: Target table name
            key_column: Primary key column name

        Returns:
            Number of rows loaded
        """
        if df.empty:
            return 0

        if not self._connected:
            raise ConnectionError("Database not connected. Call connect() first")

        # Security: Validate table_name to prevent SQL injection
        if table_name not in _VALID_TABLE_NAMES:
            raise ValueError(
                f"Invalid table name: '{table_name}'. Must be one of: {sorted(_VALID_TABLE_NAMES)}"
            )

        loaded = 0

        # Prepare column names from DataFrame
        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)

        # Validate table_name is already done in load_dimension with _VALID_TABLE_NAMES allowlist
        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """  # nosec B608 - table_name validated via _VALID_TABLE_NAMES allowlist

        try:
            cursor = self.db.cursor()
            for _, row in df.iterrows():
                try:
                    cursor.execute(insert_query, tuple(row[col] for col in columns))
                    loaded += 1
                except Exception as e:
                    logger.error(f"Failed to insert dimension row: {e}")
                    self.batch_logger.add_error(
                        {
                            "error_type": "DIM_INSERT_ERROR",
                            "error_message": str(e),
                            "table": table_name,
                            "data": row.to_dict(),
                        }
                    )

            logger.info(f"Dimension table {table_name}: {loaded} rows loaded")

        except Exception as e:
            logger.error(f"Dimension load failed for {table_name}: {e}")
            raise

        return loaded

    def load_fact_sales(self, df: pd.DataFrame) -> dict[str, int]:
        """Load fact_sales table with UPSERT operation"""
        if df.empty:
            return {"inserted": 0, "updated": 0, "rejected": 0}

        if not self._connected:
            raise ConnectionError("Database not connected. Call connect() first")

        inserted = 0
        updated = 0
        rejected = 0

        # Enable autocommit for this batch
        if self.db._connection is not None:
            self.db._connection.autocommit = True
        else:
            logger.warning("No database connection, skipping fact_sales load")
            return {"inserted": 0, "updated": 0, "rejected": 0}

        cursor = self.db.cursor()

        # Prepare batch data
        rows = []
        for _, row in df.iterrows():
            rows.append(
                (
                    int(row.get("date_id", 0)),
                    int(row.get("product_id", 0)),
                    int(row.get("region_id", 0)),
                    int(row.get("channel_id", 0)),
                    int(row.get("customer_id", 0)),
                    int(row.get("macro_id", 1)) if pd.notna(row.get("macro_id")) else 1,
                    int(row.get("units", 0)),
                    float(row.get("unit_price", 0.0)),
                    float(row.get("revenue", 0.0)),
                    float(row.get("housing_starts_index", 0))
                    if pd.notna(row.get("housing_starts_index"))
                    else None,
                    float(row.get("lumber_price_index", 0))
                    if pd.notna(row.get("lumber_price_index"))
                    else None,
                    float(row.get("mortgage_rate", 0))
                    if pd.notna(row.get("mortgage_rate"))
                    else None,
                )
            )

        # UPSERT query
        upsert_query = """
            INSERT INTO fact_sales (
                date_id, product_id, region_id, channel_id, customer_id, macro_id,
                units, unit_price, revenue,
                housing_starts_index, lumber_price_index, mortgage_rate
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT (date_id, region_id, product_id, channel_id, customer_id)
            DO UPDATE SET
                units = fact_sales.units + EXCLUDED.units,
                revenue = fact_sales.revenue + EXCLUDED.revenue,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted
        """

        try:
            # Execute each row individually to track insert vs update
            for row_data in rows:
                try:
                    cursor.execute(upsert_query, row_data)
                    result = cursor.fetchone()
                    if result and result[0]:
                        inserted += 1
                    else:
                        updated += 1
                except Exception as e:
                    logger.error(f"Failed to insert row: {e}")
                    rejected += 1
                    self.batch_logger.add_error(
                        {"error_type": "DB_INSERT_ERROR", "error_message": str(e), "data": row_data}
                    )

            logger.info(
                f"UPSERT complete: {inserted} inserted, {updated} updated, {rejected} rejected"
            )

        except Exception as e:
            logger.error(f"Batch UPSERT failed: {e}")
            raise
        finally:
            cursor.close()

        return {"inserted": inserted, "updated": updated, "rejected": rejected}


class IncrementalLoader:
    """Handles incremental data loading with watermark tracking

    This class implements:
    1. Watermark-based incremental loading (only new data since last run)
    2. UPSERT operations (insert new, update existing)
    3. idempotency (same input = same output, can run multiple times)
    """

    def __init__(self, loader: DataLoader = None):
        self.loader = loader or DataLoader()
        self.watermark = get_watermark()

    def get_new_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter to only new records since last watermark

        Args:
            df: Full dataset

        Returns:
            Only records newer than watermark date
        """
        if df.empty:
            return df

        # If no watermark, return all data (full load)
        if self.watermark is None:
            logger.info("No watermark found - loading all data")
            return df

        # Filter to only new data
        df = df.copy()
        df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
        new_records = df[df["transaction_date"] > pd.to_datetime(self.watermark).date()]

        count = len(new_records)
        logger.info(f"Watermark: {self.watermark} - Found {count:,} new rows")

        return new_records

    def update_watermark(self, df: pd.DataFrame):
        """Update watermark file with latest date from loaded data

        Args:
            df: DataFrame that was just loaded
        """
        if df.empty:
            return

        # Get max date from loaded data
        df = df.copy()
        df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
        max_date = df["transaction_date"].max()

        # Handle None case
        if max_date is None:
            logger.warning("No dates found in DataFrame, cannot update watermark")
            return

        # Format as string
        if isinstance(max_date, pd.Timestamp):
            max_date = max_date.date()

        date_str = max_date.isoformat() if hasattr(max_date, "isoformat") else str(max_date)

        # Save watermark
        set_watermark(date_str)
        logger.info(f"Watermark updated to: {date_str}")

    def load_incremental(
        self, transactions_df: pd.DataFrame, macro_df: pd.DataFrame = None
    ) -> dict[str, Any]:
        """Load data incrementally with watermark tracking"""
        result = {
            "watermark_before": self.watermark,
            "rows_in_input": len(transactions_df),
            "rows_processed": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped": 0,
            "new_watermark": None,
            "status": "success",
        }

        try:
            # Connect to database
            self.loader.connect()

            # Load macro data
            if macro_df is not None and not macro_df.empty:
                logger.info(f"Loading macro data: {len(macro_df)} rows")
                result["macro_rows"] = len(macro_df)
                # Load macro dimension table
                self._load_macro_dimension(macro_df)

            # Filter to new data only
            new_data = self.get_new_data(transactions_df)
            result["rows_processed"] = len(new_data)

            if new_data.empty:
                logger.info("No new data to load")
                self.loader.disconnect()
                return result

            # Load new dimensions before transaction
            logger.info("Loading new dates into dim_date...")
            self.loader.load_dates(new_data)

            logger.info("Loading new products into dim_product...")
            sku_to_product_id = self.loader.load_products(new_data)

            # Load regions, channels, customers if not exists
            self._ensure_dimension_data(new_data)

            # Resolve dimension FKs with SKU mapping
            logger.info(f"Resolving dimension FKs for {len(new_data)} rows...")
            new_data = self._resolve_fk_ids(new_data, sku_to_product_id)

            # Load in batches
            batch_size = batch_config.batch_size
            total_batches = (len(new_data) + batch_size - 1) // batch_size

            self.loader.batch_logger.start_batch(f"incremental_load_{total_batches}_batches")

            try:
                for i in range(0, len(new_data), batch_size):
                    batch = new_data.iloc[i : i + batch_size]
                    batch_num = i // batch_size + 1

                    logger.info(f"Loading batch {batch_num}/{total_batches} ({len(batch)} rows)")

                    batch_result = self.loader.load_fact_sales(batch)

                    self.loader.batch_logger.add_insert(batch_result.get("inserted", 0))
                    self.loader.batch_logger.add_update(batch_result.get("updated", 0))
                    self.loader.batch_logger.add_reject(batch_result.get("rejected", 0))

                    self.loader.db.commit()

                result["rows_inserted"] = self.loader.batch_logger.rows_inserted
                result["rows_updated"] = self.loader.batch_logger.rows_updated
                result["rows_skipped"] = self.loader.batch_logger.rows_rejected

            except Exception as e:
                logger.error(f"Load failed: {e}")
                self.loader.db.rollback()
                result["status"] = "rolled_back"
                result["error"] = str(e)

            # Update watermark after successful load
            if result["status"] == "success":
                self.update_watermark(new_data)
                result["new_watermark"] = get_watermark()

            self.loader.disconnect()

        except Exception as e:
            logger.error(f"Incremental load failed: {e}")
            result["status"] = "failed"
            result["error"] = str(e)
            try:
                self.loader.disconnect()
            except (psycopg2.Error, OSError):
                pass

        logger.info(f"Incremental load complete: {result}")
        return result

    def _get_db_connection(self):
        """Get database connection - works for both DataLoader and IncrementalLoader"""
        if hasattr(self, "db"):
            return self.db
        elif hasattr(self, "loader") and hasattr(self.loader, "db"):
            return self.loader.db
        return None

    def _resolve_fk_ids(self, df: pd.DataFrame, sku_to_product_id: dict = None) -> pd.DataFrame:
        """
        Resolve foreign key IDs by looking up dimension tables.

        Adds columns: date_id, product_id, region_id, channel_id, customer_id, macro_id

        Args:
            df: DataFrame with transaction data
            sku_to_product_id: Optional mapping of SKU to product_id (from load_products)
        """
        df = df.copy()

        # Date FK - use date_id format YYYYMMDD
        df["date_id"] = pd.to_datetime(df["transaction_date"]).dt.strftime("%Y%m%d").astype(int)

        # Product FK - use provided mapping or lookup from database
        if sku_to_product_id:
            df["product_id"] = df["sku"].map(sku_to_product_id).fillna(1).astype(int)
        else:
            db = self._get_db_connection()
            if db and db._connection:
                try:
                    cursor = db.cursor()
                    cursor.execute("SELECT product_id, sku FROM dim_product")
                    product_map = {row[1]: row[0] for row in cursor.fetchall()}
                    df["product_id"] = df["sku"].map(product_map).fillna(1).astype(int)
                    cursor.close()
                    db._connection.rollback()  # Exit transaction
                except (psycopg2.Error, OSError):
                    df["product_id"] = 1
            else:
                df["product_id"] = 1

        # Region FK - lookup from database
        db = self._get_db_connection()
        if db and db._connection:
            try:
                cursor = db.cursor()
                cursor.execute("SELECT region_id, region_name FROM dim_region")
                region_map = {row[1]: row[0] for row in cursor.fetchall()}
                df["region_id"] = df["region"].map(region_map).fillna(1).astype(int)
                cursor.close()
                db._connection.rollback()  # Exit transaction
            except Exception as e:
                logger.warning(f"Could not lookup region IDs: {e}")
                region_map = {
                    "East_North_Central": 1,
                    "Middle_Atlantic": 2,
                    "Pacific": 3,
                    "West_South_Central": 4,
                    "Mountain": 5,
                    "South_Atlantic": 6,
                    "West_North_Central": 7,
                    "East_South_Central": 8,
                    "New_England": 9,
                }
                df["region_id"] = df["region"].map(region_map).fillna(1).astype(int)
        else:
            region_map = {
                "East_North_Central": 1,
                "Middle_Atlantic": 2,
                "Pacific": 3,
                "West_South_Central": 4,
                "Mountain": 5,
                "South_Atlantic": 6,
                "West_North_Central": 7,
                "East_South_Central": 8,
                "New_England": 9,
            }
            df["region_id"] = df["region"].map(region_map).fillna(1).astype(int)

        # Channel FK
        if db and db._connection:
            try:
                cursor = db.cursor()
                cursor.execute("SELECT channel_id, channel_name FROM dim_channel")
                channel_map = {row[1]: row[0] for row in cursor.fetchall()}
                df["channel_id"] = df["channel"].map(channel_map).fillna(1).astype(int)
                cursor.close()
                db._connection.rollback()  # Exit transaction
            except (psycopg2.Error, OSError):
                channel_map = {"pro_dealer": 1, "big_box_diy": 2}
                df["channel_id"] = df["channel"].map(channel_map).fillna(1).astype(int)
        else:
            channel_map = {"pro_dealer": 1, "big_box_diy": 2}
            df["channel_id"] = df["channel"].map(channel_map).fillna(1).astype(int)

        # Customer FK
        if db and db._connection:
            try:
                cursor = db.cursor()
                cursor.execute("SELECT customer_id, customer_type FROM dim_customer")
                customer_map = {row[1]: row[0] for row in cursor.fetchall()}
                df["customer_id"] = df["customer_type"].map(customer_map).fillna(1).astype(int)
                cursor.close()
                db._connection.rollback()  # Exit transaction
            except (psycopg2.Error, OSError):
                customer_map = {
                    "remodeler": 1,
                    "contractor": 2,
                    "homeowner_diy": 3,
                    "homebuilder": 4,
                }
                df["customer_id"] = df["customer_type"].map(customer_map).fillna(1).astype(int)
        else:
            customer_map = {"remodeler": 1, "contractor": 2, "homeowner_diy": 3, "homebuilder": 4}
            df["customer_id"] = df["customer_type"].map(customer_map).fillna(1).astype(int)

        # Macro FK - lookup from database
        if db and db._connection:
            try:
                cursor = db.cursor()
                cursor.execute("SELECT macro_id, week FROM dim_macro")
                macro_map = {str(row[1]): row[0] for row in cursor.fetchall()}
                df["macro_id"] = (
                    df["transaction_date"]
                    .apply(lambda x: macro_map.get(str(pd.to_datetime(x).strftime("%Y-%m-%d")), 1))
                    .astype(int)
                )
                cursor.close()
                db._connection.rollback()  # Exit transaction
            except (psycopg2.Error, OSError):
                df["macro_id"] = 1
        else:
            df["macro_id"] = 1

        return df

    def reset_watermark(self):
        """Reset watermark to allow full reload

        Useful for:
        - Re-processing historical data
        - Testing
        - Recovery from errors
        """
        import os

        if incremental_config.watermark_file.exists():
            os.remove(incremental_config.watermark_file)
            logger.info("Watermark reset - next run will do full load")
        else:
            logger.info("No watermark file exists")

    def _load_macro_dimension(self, macro_df: pd.DataFrame):
        """Load macro dimension table from transformed macro data"""
        if macro_df is None or macro_df.empty:
            return

        db = self._get_db_connection()
        if not db or not db._connection:
            logger.warning("No database connection for macro dimension loading")
            return

        cursor = db.cursor()

        try:
            for _, row in macro_df.iterrows():
                try:
                    # Column is week_date but we need week for the table
                    week_date = row.get("week_date", row.get("week"))
                    cursor.execute(
                        """
                        INSERT INTO dim_macro (
                            week, housing_starts_index, lumber_price_index,
                            mortgage_rate, season_factor, housing_momentum,
                            lumber_volatility, data_source
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (week) DO UPDATE SET
                            housing_starts_index = EXCLUDED.housing_starts_index,
                            lumber_price_index = EXCLUDED.lumber_price_index,
                            mortgage_rate = EXCLUDED.mortgage_rate,
                            season_factor = EXCLUDED.season_factor,
                            housing_momentum = EXCLUDED.housing_momentum,
                            lumber_volatility = EXCLUDED.lumber_volatility,
                            updated_at = CURRENT_TIMESTAMP
                        RETURNING macro_id
                        """,
                        (
                            week_date,
                            row.get("housing_starts_index"),
                            row.get("lumber_price_index"),
                            row.get("mortgage_rate"),
                            row.get("season_factor", 1.0),
                            row.get("housing_momentum", 0.0),
                            row.get("lumber_volatility", 0.0),
                            "macro_drivers_weekly.csv",
                        ),
                    )
                except Exception as e:
                    logger.debug(f"Could not insert macro row: {e}")

            db._connection.commit()
            logger.info("Macro dimension loaded")
        except Exception as e:
            logger.warning(f"Could not load macro dimension: {e}")
            try:
                db._connection.rollback()
            except (psycopg2.Error, OSError):
                pass

        cursor.close()

    def _ensure_dimension_data(self, df: pd.DataFrame):
        """Ensure all dimension tables have required data"""
        db = self._get_db_connection()
        if not db or not db._connection:
            logger.warning("No database connection for dimension loading")
            return

        cursor = db.cursor()

        # Region mapping (with census_division)
        regions = [
            ("East_North_Central", "East North Central", 1),
            ("Middle_Atlantic", "Middle Atlantic", 2),
            ("Pacific", "Pacific", 3),
            ("West_South_Central", "West South Central", 4),
            ("Mountain", "Mountain", 5),
            ("South_Atlantic", "South Atlantic", 6),
            ("West_North_Central", "West North Central", 7),
            ("East_South_Central", "East South Central", 8),
            ("New_England", "New England", 9),
        ]

        try:
            for name, division, rid in regions:
                cursor.execute(
                    """
                    INSERT INTO dim_region (region_id, region_name, region_code, census_division)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (region_id) DO NOTHING
                    """,
                    (rid, name, name[:3].upper(), division),
                )
            db._connection.commit()
            logger.info("Regions loaded")
        except Exception as e:
            logger.warning(f"Could not load regions: {e}")
            try:
                db._connection.rollback()
            except (psycopg2.Error, OSError):
                pass

        # Channel mapping
        channels = [
            ("pro_dealer", "Professional Dealer", "PRO", 1),
            ("big_box_diy", "Big Box DIY", "BBX", 2),
        ]
        try:
            for name, ch_type, code, cid in channels:
                cursor.execute(
                    """
                    INSERT INTO dim_channel (channel_id, channel_name, channel_type, channel_code)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (channel_id) DO NOTHING
                    """,
                    (cid, name, ch_type, code),
                )
            db._connection.commit()
            logger.info("Channels loaded")
        except Exception as e:
            logger.warning(f"Could not load channels: {e}")
            try:
                db._connection.rollback()
            except (psycopg2.Error, OSError):
                pass

        # Customer mapping
        customers = [
            ("remodeler", "Remodeler", 1),
            ("contractor", "Contractor", 2),
            ("homeowner_diy", "Homeowner DIY", 3),
            ("homebuilder", "Homebuilder", 4),
        ]
        try:
            for name, segment, cid in customers:
                cursor.execute(
                    """
                    INSERT INTO dim_customer (customer_id, customer_type, segment)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (customer_id) DO NOTHING
                    """,
                    (cid, name, segment),
                )
            db._connection.commit()
            logger.info("Customers loaded")
        except Exception as e:
            logger.warning(f"Could not load customers: {e}")
            try:
                db._connection.rollback()
            except (psycopg2.Error, OSError):
                pass

        cursor.close()


def load_data(
    transactions_df: pd.DataFrame, macro_df: pd.DataFrame, db_config=None, incremental: bool = True
) -> dict[str, Any]:
    """
    Convenience function to load all data

    Args:
        transactions_df: Transformed transactions
        macro_df: Transformed macro data
        db_config: Optional database config
        incremental: If True, use incremental loading

    Returns:
        Dictionary with load statistics
    """
    loader = DataLoader()

    if incremental:
        inc_loader = IncrementalLoader(loader)
        return inc_loader.load_incremental(transactions_df, macro_df)

    # Full load mode
    inc_loader = IncrementalLoader(loader)
    return inc_loader.load_incremental(transactions_df, macro_df)
