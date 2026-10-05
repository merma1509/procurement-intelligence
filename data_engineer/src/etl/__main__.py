"""ETL Pipeline entry point

Run with:
    python -m src.etl
    python -m src.etl --db --full
    python -m src.etl --db --incremental
"""

import sys

# Use the main module directly
from src.etl.main import run_pipeline


def main():
    """Main entry point for ETL pipeline"""
    import argparse

    parser = argparse.ArgumentParser(description="ETL Pipeline for Building Materials")
    parser.add_argument("--db", action="store_true", help="Write to database (default is dry-run)")
    parser.add_argument(
        "--full", action="store_true", help="Full load mode (default is incremental)"
    )
    parser.add_argument(
        "--incremental", action="store_true", help="Incremental load mode (default)"
    )

    args = parser.parse_args()

    # Determine load mode
    incremental = not args.full  # --full disables incremental

    # Run pipeline
    result = run_pipeline(use_database=args.db, incremental=incremental)

    # Print results
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

    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
