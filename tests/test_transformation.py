"""
Tests for data transformation functionality
"""
import unittest
from pyspark.sql import SparkSession
from src.transformation.data_transformer import DataTransformer
from src.transformation.data_cleaner import DataCleaner

class TestDataTransformation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestPharmacyETL") \
            .master("local[*]") \
            .getOrCreate()
        cls.transformer = DataTransformer(cls.spark)
        cls.cleaner = DataCleaner(cls.spark)
    
    def test_create_time_dimension(self):
        # Create test data
        test_data = [(1, "2023-01-01", "10:00:00")]
        df = self.spark.createDataFrame(test_data, ["id", "date", "time"])
        
        # Test transformation
        result = self.transformer.create_time_dimension(df, "date", "time")
        self.assertIsNotNone(result)
        self.assertTrue("year" in result.columns)
        self.assertTrue("month" in result.columns)
    
    def test_clean_string_columns(self):
        test_data = [(1, " Test ", "DATA")]
        df = self.spark.createDataFrame(test_data, ["id", "name", "type"])
        
        result = self.cleaner.clean_string_columns(df)
        self.assertEqual(result.first()["name"], "Test")
        self.assertEqual(result.first()["type"], "Data")
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()
