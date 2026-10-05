"""ETL Pipeline Orchestrator

Main entry point for the ETL pipeline that orchestrates the extract,
transform, and load phases in the correct order

Supports:
- Full load mode (load all data)
- Incremental mode (load only new data since last run)
- Dry-run mode (extract + transform only, no database)
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from ..config.settings import batch_config, get_data_dir, get_watermark
from .extract import DataExtractor
from .load import DataLoader, IncrementalLoader
from .transform import DataTransformer, ValidationResult, transform_data

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Orchestrates the complete ETL pipeline

    This class manages the flow of data from source files through
    transformation to database loading.
    Supports both full and incremental loading modes
    """

    def __init__(self, data_dir: Path = None, use_database: bool = True, incremental: bool = True):
        """
        Initialize ETL pipeline

        Args:
            data_dir: Path to data directory
            use_database: Whether to load to database (False for dry run)
            incremental: Whether to use incremental loading mode
        """
        self.data_dir = data_dir or get_data_dir()
        self.use_database = use_database
        self.incremental = incremental

        self.extractor = DataExtractor(self.data_dir)
        self.transformer = DataTransformer()
        self.loader = DataLoader() if use_database else None

        self.transactions_raw = None
        self.macro_raw = None
        self.transactions_clean = None
        self.macro_clean = None
        self.validation_result: ValidationResult | None = None

        self.start_time: datetime | None = None
        self.end_time: datetime | None = None

    def run(self) -> dict[str, Any]:
        """Execute the complete ETL pipeline"""
        self.start_time = datetime.now()

        logger.info("=" * 60)
        logger.info("ETL PIPELINE STARTED")
        logger.info(f"Mode: {'incremental' if self.incremental else 'full'} load")
        if self.incremental:
            current_watermark = get_watermark()
            logger.info(f"Current watermark: {current_watermark or 'None (full load)'}")
        logger.info("=" * 60)

        result = {
            "status": "running",
            "start_time": self.start_time.isoformat(),
            "mode": "incremental" if self.incremental else "full",
            "watermark": get_watermark(),
            "phases": {},
        }

        try:
            result["phases"]["extract"] = self._extract_phase()
            result["phases"]["transform"] = self._transform_phase()

            if self.use_database:
                result["phases"]["load"] = self._load_phase()
            else:
                result["phases"]["load"] = {"status": "skipped", "reason": "dry_run"}

            result["status"] = "success"

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            result["status"] = "failed"
            result["error"] = str(e)

        self.end_time = datetime.now()
        result["end_time"] = self.end_time.isoformat()
        result["duration_seconds"] = (self.end_time - self.start_time).total_seconds()

        logger.info("=" * 60)
        logger.info(f"ETL PIPELINE FINISHED: {result['status']}")
        logger.info(f"Duration: {result['duration_seconds']:.2f} seconds")
        logger.info("=" * 60)

        return result

    def _extract_phase(self) -> dict[str, Any]:
        """Execute extraction phase"""
        logger.info("\n[PHASE 1/3] EXTRACT")
        logger.info("-" * 40)

        transactions, macro, dictionary = self.extractor.extract_all()
        self.transactions_raw = transactions
        self.macro_raw = macro

        summary = self.extractor.get_extraction_summary()

        logger.info(f"Extracted {summary['transactions']['rows']:,} transactions")
        logger.info(f"Extracted {summary['macro']['rows']} macro records")

        return {"status": "success", **summary}

    def _transform_phase(self) -> dict[str, Any]:
        """Execute transformation phase"""
        logger.info("\n[PHASE 2/3] TRANSFORM")
        logger.info("-" * 40)

        transactions, macro, validation = transform_data(self.transactions_raw, self.macro_raw)

        self.transactions_clean = transactions
        self.macro_clean = macro
        self.validation_result = validation

        summary = {
            "status": "success",
            "valid_rows": validation.valid_count,
            "invalid_rows": validation.invalid_count,
            "error_rate": validation.error_rate,
            "transactions_shape": list(transactions.shape),
            "macro_shape": list(macro.shape),
        }

        logger.info(f"Valid rows: {validation.valid_count:,}")
        logger.info(f"Invalid rows: {validation.invalid_count:,}")
        logger.info(f"Error rate: {validation.error_rate:.2f}%")

        if validation.errors:
            error_df = validation.get_errors_dataframe()
            error_summary = error_df["error_type"].value_counts()
            logger.info("\nError distribution:")
            for error_type, count in error_summary.items():
                logger.info(f"  {error_type}: {count}")

        return summary

    def _load_phase(self) -> dict[str, Any]:
        """Execute load phase."""
        logger.info("\n[PHASE 3/3] LOAD")
        logger.info("-" * 40)

        if self.transactions_clean is None:
            raise ValueError("No transformed data to load")

        if not self.use_database or not self.loader:
            return {"status": "skipped", "reason": "dry_run"}

        if self.incremental:
            # Incremental load with watermark
            # Load new dimensions first
            self.loader.load_dates(self.transactions_clean)
            self.loader.load_products(self.transactions_clean)

            inc_loader = IncrementalLoader(self.loader)
            load_result = inc_loader.load_incremental(self.transactions_clean, self.macro_clean)

            logger.info("Incremental load statistics:")
            for key, value in load_result.items():
                logger.info(f"  {key}: {value}")

            return {"status": "success", **load_result}
        else:
            # Full load mode
            logger.info("Full load mode - loading all data")

            result = {
                "status": "success",
                "transactions_loaded": len(self.transactions_clean),
                "macro_loaded": len(self.macro_clean) if self.macro_clean is not None else 0,
            }

            try:
                # Connect to database
                self.loader.connect()

                # Load dimensions BEFORE transaction
                logger.info("Loading dimension tables...")
                self.loader.load_dates(self.transactions_clean)

                self.loader.load_products(self.transactions_clean)

                # Load fact table
                logger.info("Loading fact_sales table...")

                # Set up batch loading
                self.loader.batch_logger.start_batch("full_load")

                # Resolve FKs
                inc_loader = IncrementalLoader(self.loader)
                data_with_fks = inc_loader._resolve_fk_ids(self.transactions_clean)

                try:
                    batch_size = batch_config.batch_size
                    total_rows = len(data_with_fks)
                    total_batches = (total_rows + batch_size - 1) // batch_size

                    for i in range(0, total_rows, batch_size):
                        batch = data_with_fks.iloc[i : i + batch_size]
                        batch_num = i // batch_size + 1

                        logger.info(f"Loading batch {batch_num}/{total_batches}")

                        batch_result = self.loader.load_fact_sales(batch)

                        self.loader.batch_logger.add_insert(batch_result["inserted"])
                        self.loader.batch_logger.add_update(batch_result["updated"])
                        self.loader.batch_logger.add_reject(batch_result["rejected"])

                    result.update(self.loader.batch_logger.finish_batch())
                    logger.info(f"Full load complete: {result}")

                except Exception as e:
                    logger.error(f"Load failed: {e}")
                    result["status"] = "failed"
                    result["error"] = str(e)

                self.loader.disconnect()

            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                result["status"] = "failed"
                result["error"] = str(e)

            return result

    def get_data_summary(self) -> dict[str, Any]:
        """Get summary of current pipeline state"""
        return {
            "raw_transactions": len(self.transactions_raw)
            if self.transactions_raw is not None
            else None,
            "clean_transactions": len(self.transactions_clean)
            if self.transactions_clean is not None
            else None,
            "raw_macro": len(self.macro_raw) if self.macro_raw is not None else None,
            "clean_macro": len(self.macro_clean) if self.macro_clean is not None else None,
            "validation_errors": len(self.validation_result.errors)
            if self.validation_result
            else None,
            "current_watermark": get_watermark(),
        }


def run_pipeline(
    data_dir: Path | None = None, use_database: bool = False, incremental: bool = True
) -> dict[str, Any]:
    """Run the ETL pipeline

    Args:
        data_dir: Path to data directory
        use_database: Whether to load to database
        incremental: Whether to use incremental loading

    Returns:
        Pipeline execution result
    """
    pipeline = ETLPipeline(data_dir=data_dir, use_database=use_database, incremental=incremental)
    return pipeline.run()


if __name__ == "__main__":
    import sys

    # Parse command line arguments
    incremental = "--full" not in sys.argv  # Default to incremental
    use_db = "--db" in sys.argv

    if "--full" in sys.argv:
        logger.info("Running in FULL LOAD mode")
    else:
        logger.info("Running in INCREMENTAL mode")

    if not use_db:
        logger.info("(Dry-run mode: no database write)")

    result = run_pipeline(use_database=use_db, incremental=incremental)

    print("\n" + "=" * 60)
    print("PIPELINE RESULT")
    print("=" * 60)
    print(f"Status: {result['status']}")
    print(f"Mode: {result.get('mode', 'N/A')}")
    print(f"Duration: {result.get('duration_seconds', 'N/A'):.2f} seconds")

    if "transform" in result.get("phases", {}):
        trn = result["phases"]["transform"]
        print(f"\nTransformed: {trn.get('valid_rows', 'N/A'):,} valid rows")
        print(f"Error rate: {trn.get('error_rate', 'N/A'):.2f}%")

    if "load" in result.get("phases", {}):
        load = result["phases"]["load"]
        if "rows_processed" in load:
            print(f"\nLoad: {load.get('rows_processed', 0):,} rows processed")
            print(f"Watermark after: {load.get('new_watermark', 'N/A')}")
