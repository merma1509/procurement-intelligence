"""Transform module for data validation and cleaning

This module handles the transformation phase of ETL, including:
- Type casting and validation
- Range checking for numeric values
- Business rule validation
- Error handling and dead letter queue generation
"""

import logging
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd

from ..config.settings import (
    CATEGORIES,
    CHANNELS,
    CUSTOMER_TYPES,
    REGIONS,
    validation_rules,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Represents a validation error found in the data"""

    row_index: int
    column: str
    value: Any
    error_type: str
    error_message: str
    original_data: dict[str, Any] = field(default_factory=dict)


class ValidationResult:
    """Holds results of validation operation"""

    def __init__(self):
        self.errors: list[ValidationError] = []
        self.valid_count: int = 0
        self.invalid_count: int = 0

    def add_error(self, error: ValidationError):
        """Add a validation error"""
        self.errors.append(error)
        self.invalid_count += 1

    def mark_valid(self):
        """Mark a row as valid"""
        self.valid_count += 1

    @property
    def total_processed(self) -> int:
        """Total rows processed"""
        return self.valid_count + self.invalid_count

    @property
    def error_rate(self) -> float:
        """Error rate as percentage"""
        if self.total_processed == 0:
            return 0.0
        return (self.invalid_count / self.total_processed) * 100

    def get_errors_dataframe(self) -> pd.DataFrame:
        """Convert errors to DataFrame for analysis"""
        if not self.errors:
            return pd.DataFrame()

        error_dicts = [
            {
                "row_index": e.row_index,
                "column": e.column,
                "value": str(e.value),
                "error_type": e.error_type,
                "error_message": e.error_message,
            }
            for e in self.errors
        ]
        return pd.DataFrame(error_dicts)

    def get_dead_letter_data(self) -> list[dict[str, Any]]:
        """Get data for dead letter queue"""
        return [e.original_data for e in self.errors]


class DataValidator:
    """Validates incoming data against business rules

    This class performs comprehensive validation including:
    - Data type validation
    - Range validation for numeric fields
    - Categorical value validation
    - Date format validation
    - Required field validation
    """

    def __init__(self):
        self.rules = validation_rules
        self.result = ValidationResult()

    def validate_row(self, row: pd.Series, index: int) -> tuple[bool, list[ValidationError]]:
        """Validate a single row of data

        Args:
            row: Data row as pandas Series
            index: Row index for error reporting

        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []

        # 1. Validate date format
        if pd.notna(row.get("date")):
            try:
                pd.to_datetime(row["date"], format=self.rules.date_format)
            except Exception:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="date",
                        value=row["date"],
                        error_type="INVALID_DATE_FORMAT",
                        error_message=f"Invalid date format: {row['date']}",
                        original_data=row.to_dict(),
                    )
                )

        # 2. Validate units
        if pd.notna(row.get("units")):
            units = row["units"]
            if not isinstance(units, (int, float)) or np.isnan(units):
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="units",
                        value=units,
                        error_type="INVALID_TYPE",
                        error_message="Units must be numeric",
                        original_data=row.to_dict(),
                    )
                )
            elif units < self.rules.units_min or units > self.rules.units_max:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="units",
                        value=units,
                        error_type="OUT_OF_RANGE",
                        error_message=f"Units {units} out of range [{self.rules.units_min}, {self.rules.units_max}]",
                        original_data=row.to_dict(),
                    )
                )

        # 3. Validate unit_price
        if pd.notna(row.get("unit_price")):
            price = row["unit_price"]
            if not isinstance(price, (int, float)) or np.isnan(price):
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="unit_price",
                        value=price,
                        error_type="INVALID_TYPE",
                        error_message="Unit price must be numeric",
                        original_data=row.to_dict(),
                    )
                )
            elif price <= self.rules.unit_price_min or price > self.rules.unit_price_max:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="unit_price",
                        value=price,
                        error_type="OUT_OF_RANGE",
                        error_message=f"Price {price} out of range [{self.rules.unit_price_min}, {self.rules.unit_price_max}]",
                        original_data=row.to_dict(),
                    )
                )

        # 4. Validate region
        if pd.notna(row.get("region")):
            if row["region"] not in REGIONS:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="region",
                        value=row["region"],
                        error_type="INVALID_CATEGORY",
                        error_message=f"Invalid region: {row['region']}. Valid: {REGIONS}",
                        original_data=row.to_dict(),
                    )
                )

        # 5. Validate product_category
        if pd.notna(row.get("product_category")):
            if row["product_category"] not in CATEGORIES:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="product_category",
                        value=row["product_category"],
                        error_type="INVALID_CATEGORY",
                        error_message=f"Invalid category: {row['product_category']}. Valid: {CATEGORIES}",
                        original_data=row.to_dict(),
                    )
                )

        # 6. Validate channel
        if pd.notna(row.get("channel")):
            if row["channel"] not in CHANNELS:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="channel",
                        value=row["channel"],
                        error_type="INVALID_CATEGORY",
                        error_message=f"Invalid channel: {row['channel']}. Valid: {CHANNELS}",
                        original_data=row.to_dict(),
                    )
                )

        # 7. Validate customer_type
        if pd.notna(row.get("customer_type")):
            if row["customer_type"] not in CUSTOMER_TYPES:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="customer_type",
                        value=row["customer_type"],
                        error_type="INVALID_CATEGORY",
                        error_message=f"Invalid customer type: {row['customer_type']}. Valid: {CUSTOMER_TYPES}",
                        original_data=row.to_dict(),
                    )
                )

        # 8. Validate macro indicators
        if pd.notna(row.get("lumber_price_index")):
            lumber = row["lumber_price_index"]
            if lumber < self.rules.lumber_price_min or lumber > self.rules.lumber_price_max:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="lumber_price_index",
                        value=lumber,
                        error_type="OUT_OF_RANGE",
                        error_message=f"Lumber price {lumber} outside reasonable range",
                        original_data=row.to_dict(),
                    )
                )

        # 9. Validate SKU format
        if pd.notna(row.get("sku")):
            sku = str(row["sku"])
            if "-" not in sku:
                errors.append(
                    ValidationError(
                        row_index=index,
                        column="sku",
                        value=sku,
                        error_type="INVALID_FORMAT",
                        error_message=f"SKU should contain hyphen: {sku}",
                        original_data=row.to_dict(),
                    )
                )

        return len(errors) == 0, errors

    def validate_dataframe(self, df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationResult]:
        """Validate entire DataFrame

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (cleaned DataFrame, ValidationResult)
        """
        logger.info(f"Validating {len(df):,} rows...")

        result = ValidationResult()
        valid_rows = []

        for index, row in df.iterrows():
            is_valid, errors = self.validate_row(row, index)

            if is_valid:
                valid_rows.append(row)
                result.mark_valid()
            else:
                for error in errors:
                    result.add_error(error)

        valid_df = pd.DataFrame(valid_rows)

        logger.info(
            f"Validation complete: {result.valid_count:,} valid, "
            f"{result.invalid_count:,} invalid ({result.error_rate:.2f}% error rate)"
        )

        return valid_df, result


class DataTransformer:
    """Transforms and cleans data for Star Schema

    This class handles all data transformations including:
    - Type casting
    - Feature engineering (extracting SKU prefix)
    - Deriving FK values
    - Revenue validation
    """

    def __init__(self):
        self.validator = DataValidator()

    def transform_transactions(self, df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationResult]:
        """Transform transactions data

        Args:
            df: Raw transactions DataFrame

        Returns:
            Tuple of (transformed DataFrame, ValidationResult)
        """
        logger.info("Transforming transactions data...")

        # 1. Validate first
        clean_df, validation_result = self.validator.validate_dataframe(df)

        if clean_df.empty:
            logger.warning("No valid rows after validation!")
            return clean_df, validation_result

        # 2. Type casting
        clean_df = clean_df.copy()

        # Cast numeric columns
        clean_df["units"] = clean_df["units"].astype(int)
        clean_df["unit_price"] = clean_df["unit_price"].astype(float)
        clean_df["revenue"] = clean_df["revenue"].astype(float)

        # Cast date
        clean_df["date"] = pd.to_datetime(clean_df["date"]).dt.date

        # 3. Feature engineering: Extract SKU prefix
        clean_df["sku_prefix"] = clean_df["sku"].str.split("-").str[0]

        # 4. Validate revenue calculation
        clean_df["revenue_calc"] = clean_df["units"] * clean_df["unit_price"]
        revenue_diff = abs(clean_df["revenue"] - clean_df["revenue_calc"].round(2))

        # Flag but don't reject - minor floating point differences are OK
        if revenue_diff.max() > 0.01:
            logger.warning(f"Revenue calculation mismatch detected. Max diff: {revenue_diff.max()}")

        # Use calculated revenue (more accurate)
        clean_df["revenue"] = clean_df["revenue_calc"]
        clean_df = clean_df.drop(columns=["revenue_calc"])

        # 5. Rename date column to match schema
        clean_df = clean_df.rename(columns={"date": "transaction_date"})

        # Sort by date for better insert order
        clean_df = clean_df.sort_values("transaction_date")

        logger.info(f"Transformation complete: {len(clean_df):,} rows")

        return clean_df, validation_result

    def transform_macro(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform macro drivers data

        Args:
            df: Raw macro DataFrame

        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming macro data...")

        result = df.copy()

        # Rename week to date for consistency
        result = result.rename(columns={"week": "week_date"})

        # Cast date
        result["week_date"] = pd.to_datetime(result["week_date"]).dt.date

        # Validate season_factor range
        if "season_factor" in result.columns:
            result["season_factor"] = result["season_factor"].fillna(1.0)  # Default if missing

        logger.info(f"Macro transformation complete: {len(result):,} rows")

        return result


def transform_data(
    transactions_df: pd.DataFrame, macro_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, ValidationResult]:
    """Convenience function to transform both datasets

    Args:
        transactions_df: Raw transactions DataFrame
        macro_df: Raw macro DataFrame

    Returns:
        Tuple of (transformed_transactions, transformed_macro, validation_result)
    """
    transformer = DataTransformer()

    transactions, validation_result = transformer.transform_transactions(transactions_df)
    macro = transformer.transform_macro(macro_df)

    return transactions, macro, validation_result


if __name__ == "__main__":
    # Test transformation
    from .extract import extract_data

    try:
        transactions, macro = extract_data()
        trans_clean, macro_clean, result = transform_data(transactions, macro)

        print("\nTransformation Results:")
        print(f"  Valid rows: {result.valid_count:,}")
        print(f"  Invalid rows: {result.invalid_count:,}")
        print(f"  Error rate: {result.error_rate:.2f}%")

        if result.errors:
            print("\nSample errors:")
            print(result.get_errors_dataframe().head())
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure data files exist")
