"""
Tests for data loading functionality
"""
import unittest
import os
import shutil
from pyspark.sql import SparkSession
from src.loading.data_loader import DataLoader

class TestDataLoading(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestPharmacyETL") \
            .master("local[*]") \
            .getOrCreate()
        cls.loader = DataLoader(cls.spark)
        cls.test_warehouse_dir = "test_warehouse"
        os.makedirs(cls.test_warehouse_dir, exist_ok=True)
    
    def test_load_to_warehouse(self):
        # Create test data
        test_data = [(1, "Test Product", 10.0)]
        df = self.spark.createDataFrame(test_data, ["id", "name", "price"])
        
        # Test loading
        self.loader.load_to_warehouse(df, "test_table")
        
        # Verify file exists
        self.assertTrue(os.path.exists(f"{self.test_warehouse_dir}/test_table.parquet"))
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        if os.path.exists(cls.test_warehouse_dir):
            shutil.rmtree(cls.test_warehouse_dir)

if __name__ == '__main__':
    unittest.main()
