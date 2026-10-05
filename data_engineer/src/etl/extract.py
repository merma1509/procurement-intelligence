"""Extract module for loading data from CSV files

This module handles the extraction phase of ETL, loading data from Kaggle
CSV files into pandas DataFrames for further processing
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from ..config.settings import get_data_dir

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from CSV files

    This class is responsible for loading raw data from source files
    and performing initial data quality checks
    """

    def __init__(self, data_dir: Path | None = None):
        """Initialize the data extractor

        Args:
            data_dir: Path to data directory. Defaults to configured path
        """
        self.data_dir = data_dir or get_data_dir()
        self.transactions_path = self.data_dir / "building_materials_transactions.csv"
        self.macro_path = self.data_dir / "macro_drivers_weekly.csv"
        self.dictionary_path = self.data_dir / "data_dictionary_materials.csv"

        self.transactions_df = None
        self.macro_df = None
        self.dictionary_df = None

    def extract_transactions(self) -> pd.DataFrame:
        """Load transactions data from CSV file

        Returns:
            pd.DataFrame: Transactions data

        Raises:
            FileNotFoundError: If source file doesn't exist
            ValueError: If data is empty
        """
        logger.info(f"Extracting transactions from {self.transactions_path}")

        if not self.transactions_path.exists():
            raise FileNotFoundError(
                f"Transactions file not found: {self.transactions_path}. "
                "Run download_data.py first."
            )

        df = pd.read_csv(self.transactions_path)

        if df.empty:
            raise ValueError("Transactions data is empty")

        logger.info(f"Loaded {len(df):,} transactions with {len(df.columns)} columns")
        self.transactions_df = df
        return df

    def extract_macro(self) -> pd.DataFrame:
        """Load macro drivers data from CSV file

        Returns:
            pd.DataFrame: Macro drivers data
        """
        logger.info(f"Extracting macro data from {self.macro_path}")

        if not self.macro_path.exists():
            raise FileNotFoundError(
                f"Macro file not found: {self.macro_path}. Run download_data.py first."
            )

        df = pd.read_csv(self.macro_path)

        if df.empty:
            raise ValueError("Macro data is empty")

        logger.info(f"Loaded {len(df)} macro records with {len(df.columns)} columns")
        self.macro_df = df
        return df

    def extract_dictionary(self) -> pd.DataFrame:
        """Load data dictionary from CSV file

        Returns:
            pd.DataFrame: Data dictionary
        """
        logger.info(f"Extracting data dictionary from {self.dictionary_path}")

        if not self.dictionary_path.exists():
            logger.warning(f"Dictionary file not found: {self.dictionary_path}")
            return pd.DataFrame()

        df = pd.read_csv(self.dictionary_path)
        self.dictionary_df = df
        return df

    def extract_all(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load all data sources

        Returns:
            Tuple containing:
                - transactions DataFrame
                - macro DataFrame
                - dictionary DataFrame
        """
        logger.info("Starting data extraction...")

        transactions = self.extract_transactions()
        macro = self.extract_macro()
        dictionary = self.extract_dictionary()

        logger.info("Data extraction complete!")

        return transactions, macro, dictionary

    def get_extraction_summary(self) -> dict[str, Any]:
        """Get summary of extracted data

        Returns:
            Dictionary with extraction statistics
        """
        summary: dict[str, Any] = {}

        if self.transactions_df is not None:
            summary["transactions"] = {
                "rows": len(self.transactions_df),
                "columns": len(self.transactions_df.columns),
                "date_range": f"{self.transactions_df['date'].min()} to {self.transactions_df['date'].max()}",
                "memory_mb": self.transactions_df.memory_usage(deep=True).sum() / 1024 / 1024,
            }

        if self.macro_df is not None:
            summary["macro"] = {
                "rows": len(self.macro_df),
                "columns": len(self.macro_df.columns),
                "date_range": f"{self.macro_df['week'].min()} to {self.macro_df['week'].max()}",
                "memory_mb": self.macro_df.memory_usage(deep=True).sum() / 1024 / 1024,
            }

        if self.dictionary_df is not None:
            summary["dictionary"] = {"rows": len(self.dictionary_df)}

        return summary


def extract_data(data_dir: Path = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convenience function to extract both transactions and macro data

    Args:
        data_dir: Optional path to data directory

    Returns:
        Tuple of (transactions_df, macro_df)
    """
    extractor = DataExtractor(data_dir)
    transactions, macro, _ = extractor.extract_all()
    return transactions, macro


if __name__ == "__main__":
    # Test extraction
    extractor = DataExtractor()
    try:
        summary = extractor.extract_all()
        print("\nExtraction Summary:")
        print(extractor.get_extraction_summary())
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please run download_data.py first")
