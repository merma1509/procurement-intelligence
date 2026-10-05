"""Tests for ETL pipeline components

This module contains unit tests for:
- Data extraction
- Data validation and transformation
- Incremental loading logic
- Dead letter queue functionality
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
import os
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TestDataExtractor:
    """Tests for DataExtractor class"""
    
    def test_extract_transactions_file_not_found(self):
        """Test that missing file raises FileNotFoundError"""
        from data_engineer.src.etl.extract import DataExtractor
        
        extractor = DataExtractor(data_dir=Path("/nonexistent"))
        
        with pytest.raises(FileNotFoundError):
            extractor.extract_transactions()
    
    def test_extract_macro_file_not_found(self):
        """Test that missing macro file raises FileNotFoundError."""
        from data_engineer.src.etl.extract import DataExtractor
        
        extractor = DataExtractor(data_dir=Path("/nonexistent"))
        
        with pytest.raises(FileNotFoundError):
            extractor.extract_macro()


class TestDataTransformer:
    """Tests for DataTransformer class"""
    
    def test_validate_valid_row(self):
        """Test that valid row passes validation"""
        from data_engineer.src.etl.transform import DataValidator
        
        validator = DataValidator()
        
        valid_row = pd.Series({
            'date': '2024-01-01',
            'units': 100,
            'unit_price': 10.50,
            'region': 'Pacific',
            'product_category': 'lumber',
            'channel': 'pro_dealer',
            'customer_type': 'homebuilder',
            'sku': 'LUM-1234'
        })
        
        is_valid, errors = validator.validate_row(valid_row, 0)
        
        assert is_valid is True
        assert len(errors) == 0
    
    def test_validate_invalid_units(self):
        """Test that negative units fail validation"""
        from data_engineer.src.etl.transform import DataValidator
        
        validator = DataValidator()
        
        invalid_row = pd.Series({
            'date': '2024-01-01',
            'units': -5,  # Negative units
            'unit_price': 10.50,
            'region': 'Pacific',
            'product_category': 'lumber',
            'channel': 'pro_dealer',
            'customer_type': 'homebuilder',
            'sku': 'LUM-1234'
        })
        
        is_valid, errors = validator.validate_row(invalid_row, 0)
        
        assert is_valid is False
        assert any('OUT_OF_RANGE' in e.error_type for e in errors)
    
    def test_validate_invalid_region(self):
        """Test that unknown region fails validation"""
        from data_engineer.src.etl.transform import DataValidator
        
        validator = DataValidator()
        
        invalid_row = pd.Series({
            'date': '2024-01-01',
            'units': 100,
            'unit_price': 10.50,
            'region': 'InvalidRegion',  # Invalid region
            'product_category': 'lumber',
            'channel': 'pro_dealer',
            'customer_type': 'homebuilder',
            'sku': 'LUM-1234'
        })
        
        is_valid, errors = validator.validate_row(invalid_row, 0)
        
        assert is_valid is False
        assert any('INVALID_CATEGORY' in e.error_type for e in errors)
    
    def test_validate_invalid_sku_format(self):
        """Test that SKU without hyphen fails validation"""
        from data_engineer.src.etl.transform import DataValidator
        
        validator = DataValidator()
        
        invalid_row = pd.Series({
            'date': '2024-01-01',
            'units': 100,
            'unit_price': 10.50,
            'region': 'Pacific',
            'product_category': 'lumber',
            'channel': 'pro_dealer',
            'customer_type': 'homebuilder',
            'sku': 'LUM1234'  # No hyphen
        })
        
        is_valid, errors = validator.validate_row(invalid_row, 0)
        
        assert is_valid is False
        assert any('INVALID_FORMAT' in e.error_type for e in errors)


class TestIncrementalLoader:
    """Tests for IncrementalLoader class"""
    
    def test_get_new_data_no_watermark(self):
        """Test that no watermark returns all data."""
        from data_engineer.src.etl.load import IncrementalLoader
        from data_engineer.src.config.settings import incremental_config
        
        # Clean up any existing watermark
        if incremental_config.watermark_file.exists():
            os.remove(incremental_config.watermark_file)
        
        loader = IncrementalLoader()
        
        df = pd.DataFrame({
            'date': ['2024-01-01', '2024-01-08', '2024-01-15'],
            'value': [1, 2, 3]
        })
        
        new_data = loader.get_new_data(df)
        
        # Should return all rows when no watermark
        assert len(new_data) == 3
    
    def test_get_new_data_with_watermark(self):
        """Test that watermark filters data correctly"""
        from data_engineer.src.etl.load import IncrementalLoader
        from data_engineer.src.config.settings import set_watermark, incremental_config
        
        # Set watermark
        set_watermark('2024-01-07')
        
        loader = IncrementalLoader()
        
        df = pd.DataFrame({
            'transaction_date': ['2024-01-01', '2024-01-08', '2024-01-15'],
            'value': [1, 2, 3]
        })
        
        new_data = loader.get_new_data(df)
        
        # Should only return rows after watermark (2 rows)
        assert len(new_data) == 2
        
        # Convert to string for comparison
        dates = new_data['transaction_date'].astype(str).tolist()
        assert dates == ['2024-01-08', '2024-01-15']
        
        # Clean up
        if incremental_config.watermark_file.exists():
            os.remove(incremental_config.watermark_file)
    
    def test_idempotency(self):
        """Test that running multiple times produces same result"""
        from data_engineer.src.etl.load import IncrementalLoader
        from data_engineer.src.config.settings import incremental_config
        
        # Clean up
        if incremental_config.watermark_file.exists():
            os.remove(incremental_config.watermark_file)
        
        df = pd.DataFrame({
            'transaction_date': ['2024-01-01', '2024-01-08'],
            'value': [1, 2]
        })
        
        # First run
        loader1 = IncrementalLoader()
        result1 = loader1.get_new_data(df)
        
        # Update watermark as if data was loaded
        loader1.update_watermark(result1)
        
        # Second run (with same data)
        loader2 = IncrementalLoader()
        result2 = loader2.get_new_data(df)
        
        # Should return 0 new rows (all already loaded)
        assert len(result2) == 0


class TestDeadLetterQueue:
    """Tests for DeadLetterQueue class"""
    
    def test_write_and_read_error(self):
        """Test writing and reading error records"""
        from data_engineer.src.etl.load import DeadLetterQueue
        
        # Create DLQ with test directory
        test_dir = Path("/tmp/test_etl_dlq")
        test_dir.mkdir(exist_ok=True)
        
        dlq = DeadLetterQueue(base_path=test_dir)
        dlq.clear()  # Start fresh
        
        # Write error
        error_record = {
            'row_index': 0,
            'column': 'units',
            'value': -5,
            'error_type': 'OUT_OF_RANGE',
            'error_message': 'Units cannot be negative',
            'original_data': {'units': -5, 'price': 10}
        }
        
        dlq.write_error(error_record)
        
        # Read back
        errors = dlq.read_errors()
        
        assert len(errors) == 1
        assert errors[0]['column'] == 'units'
        assert errors[0]['timestamp'] is not None
        
        # Clean up
        dlq.clear()
    
    def test_error_summary(self):
        """Test error summary statistics"""
        from data_engineer.src.etl.load import DeadLetterQueue
        
        test_dir = Path("/tmp/test_etl_dlq_summary")
        test_dir.mkdir(exist_ok=True)
        
        dlq = DeadLetterQueue(base_path=test_dir)
        dlq.clear()
        
        # Write multiple errors of different types
        for i in range(3):
            dlq.write_error({
                'row_index': i,
                'column': 'units',
                'error_type': 'OUT_OF_RANGE',
                'value': -i
            })
        
        for i in range(2):
            dlq.write_error({
                'row_index': i + 10,
                'column': 'region',
                'error_type': 'INVALID_CATEGORY',
                'value': 'Unknown'
            })
        
        summary = dlq.get_error_summary()
        
        assert summary['total_errors'] == 5
        assert summary['by_type']['OUT_OF_RANGE'] == 3
        assert summary['by_type']['INVALID_CATEGORY'] == 2
        
        # Clean up
        dlq.clear()


class TestRevenueCalculation:
    """Tests for revenue calculation accuracy"""
    
    def test_revenue_matches_units_times_price(self):
        """Test that revenue = units * unit_price"""
        df = pd.DataFrame({
            'units': [10, 20, 15],
            'unit_price': [100.00, 50.00, 75.00],
            'revenue': [1000.00, 1000.00, 1125.00]
        })
        
        df['revenue_calc'] = df['units'] * df['unit_price']
        diff = abs(df['revenue'] - df['revenue_calc'])
        assert diff.max() < 0.01, "Revenue calculation mismatch"


class TestFKResolution:
    """Tests for FK resolution in IncrementalLoader"""
    
    def test_resolve_fk_ids(self):
        """Test that FK IDs are generated correctly"""
        from data_engineer.src.etl.load import IncrementalLoader
        
        loader = IncrementalLoader()
        
        df = pd.DataFrame({
            'transaction_date': ['2024-01-01', '2024-01-08'],
            'region': ['Pacific', 'East_North_Central'],
            'channel': ['pro_dealer', 'big_box_diy'],
            'customer_type': ['homebuilder', 'contractor'],
            'sku': ['LUM-1234', 'ROO-5678'],
            'units': [10, 20],
            'revenue': [100.0, 200.0]
        })
        
        resolved = loader._resolve_fk_ids(df)
        
        # Check all FK columns exist
        assert 'date_id' in resolved.columns
        assert 'region_id' in resolved.columns
        assert 'channel_id' in resolved.columns
        assert 'customer_id' in resolved.columns
        assert 'product_id' in resolved.columns
        
        # Check channel mapping
        assert resolved['channel_id'].iloc[0] == 1  # pro_dealer
        assert resolved['channel_id'].iloc[1] == 2  # big_box_diy
        
        # Check customer mapping
        assert resolved['customer_id'].iloc[0] == 4  # homebuilder
        assert resolved['customer_id'].iloc[1] == 2  # contractor
    
    def test_date_id_format(self):
        """Test that date_id is in YYYYMMDD integer format"""
        from data_engineer.src.etl.load import IncrementalLoader
        
        loader = IncrementalLoader()
        
        df = pd.DataFrame({
            'transaction_date': ['2024-01-15'],
            'region': ['Pacific'],
            'channel': ['pro_dealer'],
            'customer_type': ['homebuilder'],
            'sku': ['LUM-1234'],
            'units': [10],
            'revenue': [100.0]
        })
        
        resolved = loader._resolve_fk_ids(df)
        
        # 2024-01-15 should become 20240115
        assert resolved['date_id'].iloc[0] == 20240115


class TestWatermarkOperations:
    """Tests for watermark operations"""
    
    def test_update_watermark(self):
        """Test that watermark is updated correctly"""
        from data_engineer.src.etl.load import IncrementalLoader
        from data_engineer.src.config.settings import get_watermark, incremental_config
        
        # Clean up
        if incremental_config.watermark_file.exists():
            os.remove(incremental_config.watermark_file)
        
        loader = IncrementalLoader()
        
        # Use columns that match actual data structure
        df = pd.DataFrame({
            'transaction_date': ['2024-01-01', '2024-01-08', '2024-01-15'],
            'sku': ['LUM-1234', 'LUM-1235', 'LUM-1236'],
            'units': [10, 20, 30],
            'unit_price': [100.0, 100.0, 100.0],
            'revenue': [100.0, 200.0, 300.0],
            'region': ['Pacific', 'Pacific', 'Pacific'],
            'product_category': ['lumber', 'lumber', 'lumber'],
            'channel': ['pro_dealer', 'pro_dealer', 'pro_dealer'],
            'customer_type': ['homebuilder', 'homebuilder', 'homebuilder'],
            'housing_starts_index': [150.0, 150.0, 150.0],
            'lumber_price_index': [500.0, 500.0, 500.0],
            'mortgage_rate': [6.5, 6.5, 6.5]
        })
        
        loader.update_watermark(df)
        
        watermark = get_watermark()
        assert watermark == '2024-01-15'
        
        # Clean up
        if incremental_config.watermark_file.exists():
            os.remove(incremental_config.watermark_file)
    
    def test_reset_watermark(self):
        """Test that watermark can be reset"""
        from data_engineer.src.etl.load import IncrementalLoader
        from data_engineer.src.config.settings import get_watermark, incremental_config
        
        # Create watermark file
        incremental_config.watermark_file.parent.mkdir(parents=True, exist_ok=True)
        with open(incremental_config.watermark_file, 'w') as f:
            f.write('2024-01-01')
        
        loader = IncrementalLoader()
        loader.reset_watermark()
        
        # Check file is deleted
        assert not incremental_config.watermark_file.exists()
        
        # Check new loader reads None
        loader2 = IncrementalLoader()
        assert loader2.watermark is None


class TestDataTransformation:
    """Tests for data transformation accuracy"""
    
    def test_transform_transactions_type_conversion(self):
        """Test that types are converted correctly"""
        from data_engineer.src.etl.transform import DataTransformer
        
        transformer = DataTransformer()
        
        # Data must already be numeric (validation runs first)
        df = pd.DataFrame({
            'date': ['2024-01-01'],
            'year': [2024],
            'month': [1],
            'week_of_year': [1],
            'sku': ['LUM-1234'],
            'units': [100],  # Already numeric
            'unit_price': [10.50],
            'revenue': [1050.00],
            'region': ['Pacific'],
            'product_category': ['lumber'],
            'channel': ['pro_dealer'],
            'customer_type': ['homebuilder'],
            'housing_starts_index': [150.0],
            'lumber_price_index': [500.0],
            'mortgage_rate': [6.5]
        })
        
        transformed, result = transformer.transform_transactions(df)
        
        # Should have valid rows
        assert not transformed.empty, "No valid rows after transformation"
        
        # Check units is int
        assert transformed['units'].dtype in [int, 'int64', 'int32']
        assert transformed['units'].iloc[0] == 100
        
        # Check prices are float
        assert transformed['unit_price'].dtype in [float, 'float64']
        assert transformed['unit_price'].iloc[0] == 10.50
    
    def test_sku_prefix_extraction(self):
        """Test that SKU prefix is extracted"""
        from data_engineer.src.etl.transform import DataTransformer
        
        transformer = DataTransformer()
        
        df = pd.DataFrame({
            'date': ['2024-01-01'],
            'year': [2024],
            'month': [1],
            'week_of_year': [1],
            'sku': ['LUM-1234-AB'],
            'units': [100],
            'unit_price': [10.50],
            'revenue': [1050.00],
            'region': ['Pacific'],
            'product_category': ['lumber'],
            'channel': ['pro_dealer'],
            'customer_type': ['homebuilder'],
            'housing_starts_index': [150.0],
            'lumber_price_index': [500.0],
            'mortgage_rate': [6.5]
        })
        
        transformed, _ = transformer.transform_transactions(df)
        
        # Should have valid rows
        assert not transformed.empty, "No valid rows after transformation"
        
        # Check sku_prefix column exists and is correct
        assert 'sku_prefix' in transformed.columns
        assert transformed['sku_prefix'].iloc[0] == 'LUM'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])