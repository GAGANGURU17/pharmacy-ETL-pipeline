"""
Data Extractors
Contains classes for extracting data from various sources
"""

import os
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from functools import wraps

from config.config import RAW_DATA_DIR, SPARK_CONFIG
from config.logging_config import get_logger

logger = get_logger(__name__)

def retry(max_tries=3, delay_seconds=2, backoff_factor=2, exceptions=(Exception,)):
    """
    Retry decorator with exponential backoff
    
    Args:
        max_tries: Maximum number of attempts
        delay_seconds: Initial delay between retries in seconds
        backoff_factor: Backoff multiplier
        exceptions: Exceptions to catch and retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            mtries, mdelay = max_tries, delay_seconds
            last_exception = None
            
            while mtries > 0:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    mtries -= 1
                    if mtries == 0:
                        logger.error(f"Failed after {max_tries} attempts: {str(e)}")
                        raise
                    
                    logger.warning(f"Retrying in {mdelay} seconds after error: {str(e)}")
                    time.sleep(mdelay)
                    mdelay *= backoff_factor
            
            raise last_exception
        return wrapper
    return decorator

class BaseExtractor:
    """Base class for all data extractors"""
    
    def __init__(self, spark=None):
        """
        Initialize the extractor
        
        Args:
            spark: Optional SparkSession. If not provided, a new one will be created.
        """
        if spark is None:
            self.spark = self._create_spark_session()
        else:
            self.spark = spark
        
        logger.info(f"Initialized {self.__class__.__name__}")
    
    def _create_spark_session(self):
        """Create a new Spark session"""
        logger.info("Creating new Spark session")
        
        return (SparkSession.builder
                .appName(SPARK_CONFIG["app_name"])
                .master(SPARK_CONFIG["master"])
                .config("spark.executor.memory", SPARK_CONFIG["executor_memory"])
                .config("spark.driver.memory", SPARK_CONFIG["driver_memory"])
                .config("spark.sql.shuffle.partitions", SPARK_CONFIG["shuffle_partitions"])
                .config("spark.sql.warehouse.dir", SPARK_CONFIG["warehouse_dir"])
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .getOrCreate())
    
    def extract(self, *args, **kwargs):
        """
        Extract data from source
        
        This method should be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement extract method")

    def validate_source_exists(self, file_path):
        """Validate that the source file exists"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Source file not found: {file_path}")
        return True


class CSVExtractor(BaseExtractor):
    """Extractor for CSV files"""
    
    @retry(max_tries=3, delay_seconds=2)
    def extract(self, file_path, schema=None, header=True, delimiter=","):
        """
        Extract data from a CSV file
        
        Args:
            file_path: Path to the CSV file
            schema: Optional schema for the data
            header: Whether the CSV file has a header row
            delimiter: Delimiter used in the CSV file
            
        Returns:
            Spark DataFrame containing the data
        """
        logger.info(f"Extracting data from CSV file: {file_path}")
        
        try:
            self.validate_source_exists(file_path)
            
            if schema:
                df = self.spark.read.csv(
                    file_path,
                    schema=schema,
                    header=header,
                    sep=delimiter
                )
            else:
                df = self.spark.read.csv(
                    file_path,
                    header=header,
                    sep=delimiter,
                    inferSchema=True
                )
            
            logger.info(f"Successfully extracted {df.count()} rows from {file_path}")
            return df
        
        except Exception as e:
            logger.error(f"Error extracting data from {file_path}: {str(e)}")
            raise


class JSONExtractor(BaseExtractor):
    """Extractor for JSON files"""
    
    def extract(self, file_path, schema=None):
        """
        Extract data from a JSON file
        
        Args:
            file_path: Path to the JSON file
            schema: Optional schema for the data
            
        Returns:
            Spark DataFrame containing the data
        """
        logger.info(f"Extracting data from JSON file: {file_path}")
        
        try:
            if schema:
                df = self.spark.read.json(file_path, schema=schema)
            else:
                df = self.spark.read.json(file_path)
            
            logger.info(f"Successfully extracted {df.count()} rows from {file_path}")
            return df
        
        except Exception as e:
            logger.error(f"Error extracting data from {file_path}: {str(e)}")
            raise


class DatabaseExtractor(BaseExtractor):
    """Extractor for database sources"""
    
    def extract(self, table_name, connection_properties, query=None):
        """
        Extract data from a database table
        
        Args:
            table_name: Name of the table to extract data from
            connection_properties: Connection properties for the database
            query: Optional SQL query to use instead of table name
            
        Returns:
            Spark DataFrame containing the data
        """
        logger.info(f"Extracting data from database table: {table_name}")
        
        try:
            if query:
                df = self.spark.read.jdbc(
                    url=connection_properties["url"],
                    table=f"({query}) as tmp",
                    properties=connection_properties
                )
            else:
                df = self.spark.read.jdbc(
                    url=connection_properties["url"],
                    table=table_name,
                    properties=connection_properties
                )
            
            logger.info(f"Successfully extracted {df.count()} rows from {table_name}")
            return df
        
        except Exception as e:
            logger.error(f"Error extracting data from {table_name}: {str(e)}")
            raise


class PharmacyDataExtractor:
    """Main extractor for pharmacy data"""
    
    def __init__(self, spark=None):
        """
        Initialize the pharmacy data extractor
        
        Args:
            spark: Optional SparkSession. If not provided, a new one will be created.
        """
        self.csv_extractor = CSVExtractor(spark)
        self.spark = self.csv_extractor.spark
        logger.info("Initialized PharmacyDataExtractor")
    
    def validate_dataframe(self, df, name, required_columns=None, min_rows=1):
        """
        Validate a dataframe meets basic quality requirements
        
        Args:
            df: DataFrame to validate
            name: Name of the dataset
            required_columns: List of columns that must be present
            min_rows: Minimum number of rows required
            
        Returns:
            True if valid, raises exception otherwise
        """
        # Check row count
        row_count = df.count()
        if row_count < min_rows:
            raise ValueError(f"Dataset {name} has insufficient rows: {row_count} < {min_rows}")
        
        # Check required columns
        if required_columns:
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Dataset {name} is missing required columns: {missing_columns}")
        
        # Check for null values in key columns
        if required_columns:
            for col in required_columns:
                null_count = df.filter(df[col].isNull()).count()
                if null_count > 0:
                    logger.warning(f"Dataset {name} has {null_count} null values in column {col}")
        
        return True
    
    def extract_all(self, input_dir=None, schemas=None, validate=True):
        """
        Extract all pharmacy data
        
        Args:
            input_dir: Directory containing the data files
            schemas: Dictionary of schemas for each data type
            validate: Whether to validate the extracted data
            
        Returns:
            Dictionary of DataFrames containing the extracted data
        """
        if input_dir is None:
            input_dir = RAW_DATA_DIR
        
        logger.info(f"Extracting all pharmacy data from {input_dir}")
        
        # Import schemas if not provided
        if schemas is None:
            from src.utils.schemas import (
                transaction_schema, transaction_detail_schema, product_schema,
                inventory_schema, customer_schema, store_schema,
                employee_schema, supplier_schema, prescription_schema
            )
            
            schemas = {
                "transactions": transaction_schema,
                "transaction_details": transaction_detail_schema,
                "products": product_schema,
                "inventory": inventory_schema,
                "customers": customer_schema,
                "stores": store_schema,
                "employees": employee_schema,
                "suppliers": supplier_schema,
                "prescriptions": prescription_schema
            }
        
        # Required columns for validation
        required_columns = {
            "transactions": ["transaction_id", "store_id", "transaction_date"],
            "transaction_details": ["detail_id", "transaction_id", "product_id"],
            "products": ["product_id", "product_name", "category"],
            "customers": ["customer_id", "first_name", "last_name"],
            "stores": ["store_id", "store_name", "location"]
        }
        
        # Extract all data
        dataframes = {}
        extraction_errors = []
        
        for name, schema in schemas.items():
            file_path = os.path.join(input_dir, f"{name}.csv")
            
            try:
                if os.path.exists(file_path):
                    df = self.csv_extractor.extract(file_path, schema)
                    
                    # Validate dataframe if required
                    if validate and name in required_columns:
                        self.validate_dataframe(df, name, required_columns[name])
                    
                    dataframes[name] = df
                    
                    # Register as temp view for SQL operations
                    df.createOrReplaceTempView(name)
                else:
                    logger.warning(f"File not found: {file_path}")
            except Exception as e:
                error_msg = f"Error extracting {name}: {str(e)}"
                logger.error(error_msg)
                extraction_errors.append(error_msg)
        
        if extraction_errors:
            logger.error(f"Encountered {len(extraction_errors)} errors during extraction")
        
        logger.info(f"Extracted {len(dataframes)} datasets")
        return dataframes
