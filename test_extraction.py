"""
Tests for data extraction functionality
"""
import unittest
from pyspark.sql import SparkSession
from src.extraction.data_extractors import PharmacyDataExtractor

class TestDataExtraction(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestPharmacyETL") \
            .master("local[*]") \
            .getOrCreate()
        cls.extractor = PharmacyDataExtractor(cls.spark)
    
    def test_extract_customers(self):
        df = self.extractor.extract_customers()
        self.assertIsNotNone(df)
        self.assertTrue(df.count() > 0)
        self.assertTrue("customer_id" in df.columns)
    
    def test_extract_transactions(self):
        df = self.extractor.extract_transactions()
        self.assertIsNotNone(df)
        self.assertTrue(df.count() > 0)
        self.assertTrue("transaction_id" in df.columns)
    
    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == '__main__':
    unittest.main()