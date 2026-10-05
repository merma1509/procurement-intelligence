"""Script for downloading data from Kaggle

Installation:
    pip install kaggle

Setup:
    1. Download kaggle.json from https://www.kaggle.com/settings
    2. Place kaggle.json in ~/.kaggle/
    3. Set permissions: chmod 600 ~/.kaggle/kaggle.json

Usage:
    python scripts/download_data.py
"""

import sys
import argparse
from pathlib import Path

try:
    from kaggle.api.kaggle_api_extended import KaggleApi
except ImportError:
    print("Error: kaggle not installed. Install with: pip install kaggle")
    sys.exit(1)


def setup_argparse() -> argparse.ArgumentParser:
    """Configure command-line argument parser"""
    parser = argparse.ArgumentParser(
        description="Download US Building Materials Sales dataset from Kaggle",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="data",
        help="Output folder for data (default: data)"
    )
    parser.add_argument(
        "--dataset",
        type=str,
        default="sergionefedov/us-building-materials-sales-transactions-20192024",
        help="Kaggle dataset slug"
    )
    return parser


def init_kaggle_api() -> KaggleApi:
    """Initialize and authenticate Kaggle API
    
    Returns:
        KaggleApi: Authenticated API instance
        
    Raises:
        Exception: If authentication fails
    """
    api = KaggleApi()
    api.authenticate()
    return api


def download_dataset(
    api: KaggleApi,
    dataset_slug: str,
    output_path: str,
) -> None:
    """Download dataset from Kaggle
    
    Args:
        api: Authenticated Kaggle API instance
        dataset_slug: Dataset slug (owner/name)
        output_path: Destination path
    """
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading dataset: {dataset_slug}")
    print(f"Output folder: {output_dir.absolute()}")
    
    # Correct method for downloading dataset
    api.dataset_download_files(
        dataset=dataset_slug,
        path=str(output_dir),
        unzip=True,
        quiet=False
    )
    
    print("Download complete!")


def verify_download(output_path: str) -> bool:
    """Verify that download was successful
    
    Args:
        output_path: Path to data folder
        
    Returns:
        True if data files were found
    """
    output_dir = Path(output_path)
    csv_files = list(output_dir.glob("*.csv"))
    
    if not csv_files:
        print("Warning: No CSV files found!")
        return False
    
    print(f"\nFound {len(csv_files)} files:")
    for f in csv_files:
        size_kb = f.stat().st_size / 1024
        print(f"   - {f.name} ({size_kb:.1f} KB)")
    
    return True


def main() -> None:
    """Main entry point of the script"""
    parser = setup_argparse()
    args = parser.parse_args()
    
    # Determine project root from script location
    script_dir = Path(__file__).parent.resolve()
    data_engineer_root = script_dir.parent
    
    # If relative path provided, use project root as base
    output_path = args.output
    if not Path(output_path).is_absolute():
        output_path = data_engineer_root / output_path
    
    try:
        print("Authenticating with Kaggle API...")
        api = init_kaggle_api()
        
        download_dataset(
            api=api,
            dataset_slug=args.dataset,
            output_path=str(output_path),
        )
        
        if not verify_download(str(output_path)):
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()