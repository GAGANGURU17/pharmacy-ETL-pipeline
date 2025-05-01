"""
Data Quality Utility
Provides tools for validating data quality
"""

from pyspark.sql.functions import col, count, when, isnan, isnull
from config.logging_config import get_logger

logger = get_logger(__name__)

class DataQualityChecker:
    """Class for checking data quality"""
    
    def __init__(self, spark):
        """
        Initialize the data quality checker
        
        Args:
            spark: SparkSession
        """
        self.spark = spark
        logger.info("Initialized DataQualityChecker")
    
    def check_null_values(self, df, columns=None):
        """
        Check for null values in specified columns
        
        Args:
            df: DataFrame to check
            columns: List of columns to check, or None for all columns
            
        Returns:
            Dictionary with column names as keys and null counts as values
        """
        if columns is None:
            columns = df.columns
        
        null_counts = {}
        
        for column in columns:
            null_count = df.filter(
                col(column).isNull() | isnan(col(column))
            ).count()
            
            null_counts[column] = null_count
            
            if null_count > 0:
                logger.warning(f"Column '{column}' has {null_count} null values")
        
        return null_counts
    
    def check_duplicate_keys(self, df, key_columns):
        """
        Check for duplicate keys in the DataFrame
        
        Args:
            df: DataFrame to check
            key_columns: List of columns that should form a unique key
            
        Returns:
            Number of duplicate keys found
        """
        # Count occurrences of each key
        key_counts = df.groupBy(key_columns).count()
        
        # Filter for keys that appear more than once
        duplicates = key_counts.filter(col("count") > 1)
        duplicate_count = duplicates.count()
        
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate keys in columns {key_columns}")
            
            # Show some examples of duplicates
            logger.warning("Examples of duplicate keys:")
            duplicates.show(5, truncate=False)
        
        return duplicate_count
    
    def check_value_distribution(self, df, column, expected_values=None):
        """
        Check the distribution of values in a column
        
        Args:
            df: DataFrame to check
            column: Column to check
            expected_values: Optional list of expected values
            
        Returns:
            DataFrame with value counts
        """
        # Count occurrences of each value
        value_counts = df.groupBy(column).count().orderBy("count", ascending=False)
        
        # Log the distribution
        logger.info(f"Value distribution for column '{column}':")
        value_counts.show(10, truncate=False)
        
        # Check for unexpected values if expected values are provided
        if expected_values:
            unexpected = value_counts.filter(~col(column).isin(expected_values))
            unexpected_count = unexpected.count()
            
            if unexpected_count > 0:
                logger.warning(f"Found {unexpected_count} unexpected values in column '{column}'")
                logger.warning("Examples of unexpected values:")
                unexpected.show(5, truncate=False)
        
        return value_counts
    
    def run_data_quality_checks(self, df, table_name, key_columns, required_columns=None, 
                               expected_values=None, min_rows=1):
        """
        Run a standard set of data quality checks
        
        Args:
            df: DataFrame to check
            table_name: Name of the table (for logging)
            key_columns: List of columns that should form a unique key
            required_columns: List of columns that should not have nulls
            expected_values: Dictionary mapping column names to lists of expected values
            min_rows: Minimum number of rows expected
            
        Returns:
            Dictionary with check results
        """
        results = {
            "table_name": table_name,
            "row_count": df.count(),
            "passed_all_checks": True
        }
        
        # Check row count
        if results["row_count"] < min_rows:
            logger.error(f"Table {table_name} has insufficient rows: {results['row_count']} < {min_rows}")
            results["passed_all_checks"] = False
        
        # Check for null values in required columns
        if required_columns:
            null_counts = self.check_null_values(df, required_columns)
            results["null_counts"] = null_counts
            
            if any(count > 0 for count in null_counts.values()):
                results["passed_all_checks"] = False
        
        # Check for duplicate keys
        if key_columns:
            duplicate_count = self.check_duplicate_keys(df, key_columns)
            results["duplicate_keys"] = duplicate_count
            
            if duplicate_count > 0:
                results["passed_all_checks"] = False
        
        # Check value distributions
        if expected_values:
            for column, values in expected_values.items():
                if column in df.columns:
                    unexpected_count = self.check_value_distribution(df, column, values).filter(
                        ~col(column).isin(values)
                    ).count()
                    
                    results[f"unexpected_{column}"] = unexpected_count
                    
                    if unexpected_count > 0:
                        results["passed_all_checks"] = False
        
        # Log overall result
        if results["passed_all_checks"]:
            logger.info(f"Table {table_name} passed all data quality checks")
        else:
            logger.warning(f"Table {table_name} failed some data quality checks")
        
        return results