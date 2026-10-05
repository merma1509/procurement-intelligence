"""Configuration settings for ETL pipeline

This module centralizes all configuration parameters for the data pipeline,
including database connections, file paths, validation rules, and batch settings.

Security: Sensitive values (passwords, API keys) should be stored in .env file.
See .env.example for required variables.
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
# This allows storing secrets outside of version control
load_dotenv()

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data_engineer" / "data"


def _get_env(key: str, default: str = "") -> str:
    """Safely get environment variable with fallback."""
    return os.getenv(key, default)


@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration

    Values are loaded from environment variables for security.
    See .env.example for required variables.
    """

    host: str = ""
    port: int = 5432
    database: str = ""
    user: str = ""
    password: str = ""
    schema: str = "public"

    def __post_init__(self):
        """Load values from environment variables."""
        # Only override if not already set (allows programmatic override)
        if not self.host:
            self.host = _get_env("DB_HOST", "localhost")
        if not self.port:
            self.port = int(_get_env("DB_PORT", "5432"))
        if not self.database:
            self.database = _get_env("DB_NAME", "building_materials")
        if not self.user:
            self.user = _get_env("DB_USER", "")
        if not self.password:
            self.password = _get_env("DB_PASSWORD", "")


@dataclass
class DataValidationRules:
    """Validation rules for incoming data"""

    # Date validation
    date_format: str = "%Y-%m-%d"

    # Numeric ranges (based on EDA)
    units_min: int = 0
    units_max: int = 100000
    unit_price_min: float = 0.01
    unit_price_max: float = 100000.0
    revenue_min: float = 0.0

    # Macro indicators (known ranges from data analysis)
    housing_starts_min: float = 50.0
    housing_starts_max: float = 200.0
    lumber_price_min: float = 100.0
    lumber_price_max: float = 2000.0
    mortgage_rate_min: float = 0.0
    mortgage_rate_max: float = 15.0
    season_factor_min: float = 0.0
    season_factor_max: float = 2.0


@dataclass
class FileConfig:
    """Input file configuration"""

    transactions_file: str = "building_materials_transactions.csv"
    macro_file: str = "macro_drivers_weekly.csv"
    dictionary_file: str = "data_dictionary_materials.csv"


@dataclass
class BatchConfig:
    """Batch processing configuration"""

    batch_size: int = 10000
    max_errors: int = 100
    commit_interval: int = 5000


# Initialize configurations
db_config = DatabaseConfig()
validation_rules = DataValidationRules()
file_config = FileConfig()
batch_config = BatchConfig()


# Dimension tables reference data
REGIONS = [
    "East_North_Central",
    "Middle_Atlantic",
    "Pacific",
    "West_South_Central",
    "Mountain",
    "South_Atlantic",
    "West_North_Central",
    "East_South_Central",
    "New_England",
]

CATEGORIES = [
    "roofing",
    "lumber",
    "concrete_cement",
    "insulation",
    "plumbing",
    "windows_doors",
    "paint_coatings",
    "drywall",
    "hardware_fasteners",
    "electrical",
]

CHANNELS = ["pro_dealer", "big_box_diy"]

CUSTOMER_TYPES = ["remodeler", "contractor", "homeowner_diy", "homebuilder"]

SKU_PREFIX_MAP = {
    "ROO": "roofing",
    "LUM": "lumber",
    "CON": "concrete_cement",
    "INS": "insulation",
    "PLU": "plumbing",
    "WIN": "windows_doors",
    "PNT": "paint_coatings",
    "DRY": "drywall",
    "HRD": "hardware_fasteners",
    "ELC": "electrical",
}

# INCREMENTAL LOADING CONFIGURATION


@dataclass
class IncrementalConfig:
    """Configuration for incremental loading"""

    # Track watermark in this file
    watermark_file: Path = PROJECT_ROOT / "data_engineer" / ".watermark"
    # Default to load all data if no watermark
    load_all_if_no_watermark: bool = True
    # Batch size for incremental loads
    incremental_batch_size: int = 50000


incremental_config = IncrementalConfig()


def get_watermark() -> Optional[str]:
    """Get the last processed date from watermark file

    Returns:
        Date string (YYYY-MM-DD) or None if no watermark
    """
    if not incremental_config.watermark_file.exists():
        return None

    try:
        with open(incremental_config.watermark_file) as f:
            content = f.read().strip()
            if content:
                return content
    except OSError as e:
        logging.warning(f"Could not read watermark file: {e}")
    except UnicodeDecodeError as e:
        logging.warning(f"Watermark file has invalid encoding: {e}")

    return None


def set_watermark(date_str: str):
    """Save the last processed date to watermark file

    Args:
        date_str: Date string in YYYY-MM-DD format
    """
    try:
        with open(incremental_config.watermark_file, "w") as f:
            f.write(date_str)
    except Exception as e:
        logging.warning(f"Could not save watermark: {e}")


def get_data_dir() -> Path:
    """Get the data directory path"""
    return DATA_DIR


def get_transactions_path() -> Path:
    """Get path to transactions data file"""
    return DATA_DIR / file_config.transactions_file


def get_macro_path() -> Path:
    """Get path to macro drivers data file"""
    return DATA_DIR / file_config.macro_file
