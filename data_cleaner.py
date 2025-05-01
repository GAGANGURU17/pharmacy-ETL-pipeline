"""
Data Cleaner
Contains functions for cleaning and validating data
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull, trim, regexp_replace, upper, lower
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, BooleanType

from config.logging_config import get_logger

logger = get_logger(__name__)

class DataCleaner:
    """Class for cleaning and validating data"""
    
    def __init__(self, spark):
        """
        Initialize the data cleaner
        
        Args:
            spark: SparkSession
        """
        self.spark = spark
        logger.info("Initialized DataCleaner")
    
    def clean_string_columns(self, df, columns=None):
        """
        Clean string columns by trimming whitespace and standardizing case
        
        Args:
            df: DataFrame to clean
            columns: List of columns to clean. If None, all string columns are cleaned.
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("Cleaning string columns")
        
        # If no columns specified, find all string columns
        if columns is None:
            columns = [field.name for field in df.schema.fields 
                      if isinstance(field.dataType, StringType)]
        
        # Apply cleaning to each column
        for column in columns:
            if column in df.columns:
                df = df.withColumn(column, trim(col(column)))
                logger.debug(f"Cleaned column: {column}")
            else:
                logger.warning(f"Column not found: {column}")
        
        return df
    
    def handle_missing_values(self, df, strategy="default"):
        """
        Handle missing values in the DataFrame
        
        Args:
            df: DataFrame to process
            strategy: Strategy for handling missing values
                      "default": Replace with type-appropriate defaults
                      "drop_row": Drop rows with any missing values
                      "drop_column": Drop columns with too many missing values
            
        Returns:
            Processed DataFrame
        """
        logger.info(f"Handling missing values with strategy: {strategy}")
        
        if strategy == "drop_row":
            # Drop rows with any null values
            result_df = df.dropna()
            logger.info(f"Dropped {df.count() - result_df.count()} rows with missing values")
            return result_df
        
        elif strategy == "drop_column":
            # Calculate null percentage for each column
            null_counts = {}
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = (null_count / df.count()) * 100
                null_counts[column] = null_percentage
            
            # Drop columns with more than 50% nulls
            columns_to_drop = [column for column, percentage in null_counts.items() 
                              if percentage > 50]
            
            if columns_to_drop:
                result_df = df.drop(*columns_to_drop)
                logger.info(f"Dropped columns with >50% missing values: {columns_to_drop}")
                return result_df
            else:
                return df
        
        else:  # default strategy
            # Replace nulls with type-appropriate defaults
            result_df = df
            
            for field in df.schema.fields:
                column = field.name
                data_type = field.dataType
                
                if isinstance(data_type, StringType):
                    result_df = result_df.withColumn(
                        column, 
                        when(col(column).isNull() | (trim(col(column)) == ""), "Unknown")
                        .otherwise(col(column))
                    )
                
                elif isinstance(data_type, IntegerType):
                    result_df = result_df.withColumn(
                        column,
                        when(col(column).isNull(), 0).otherwise(col(column))
                    )
                
                elif isinstance(data_type, DoubleType):
                    result_df = result_df.withColumn(
                        column,
                        when(col(column).isNull() | isnan(col(column)), 0.0)
                        .otherwise(col(column))
                    )
                
                elif isinstance(data_type, BooleanType):
                    result_df = result_df.withColumn(
                        column,
                        when(col(column).isNull(), False).otherwise(col(column))
                    )
            
            logger.info("Replaced missing values with defaults")
            return result_df
    
    def standardize_phone_numbers(self, df, phone_columns):
        """
        Standardize phone number formats
        
        Args:
            df: DataFrame to process
            phone_columns: List of columns containing phone numbers
            
        Returns:
            Processed DataFrame
        """
        logger.info("Standardizing phone numbers")
        
        for column in phone_columns:
            if column in df.columns:
                # Remove non-digit characters and format as (XXX) XXX-XXXX
                df = df.withColumn(
                    column,
                    when(
                        col(column).isNull() | (trim(col(column)) == ""),
                        None
                    ).otherwise(
                        regexp_replace(col(column), "[^0-9]", "")
                    )
                )
                
                # Format 10-digit numbers
                df = df.withColumn(
                    column,
                    when(
                        length(col(column)) == 10,
                        concat(
                            lit("("), substring(col(column), 1, 3), lit(") "),
                            substring(col(column), 4, 3), lit("-"),
                            substring(col(column), 7, 4)
                        )
                    ).otherwise(col(column))
                )
                
                logger.debug(f"Standardized phone column: {column}")
            else:
                logger.warning(f"Phone column not found: {column}")
        
        return df
    
    def validate_data(self, df, validation_rules):
        """
        Validate data against a set of rules
        
        Args:
            df: DataFrame to validate
            validation_rules: Dictionary of validation rules
                             {column_name: (validation_function, error_message)}
            
        Returns:
            Tuple of (valid_df, invalid_df, validation_results)
        """
        logger.info("Validating data")
        
        # Create a column to track validation status
        df = df.withColumn("is_valid", lit(True))
        df = df.withColumn("validation_errors", array())
        
        # Apply each validation rule
        for column, (validation_func, error_message) in validation_rules.items():
            if column in df.columns:
                # Apply validation function and update tracking columns
                df = df.withColumn(
                    "rule_valid", validation_func(col(column))
                )
                
                df = df.withColumn(
                    "is_valid",
                    when(col("rule_valid") == False, False).otherwise(col("is_valid"))
                )
                
                df = df.withColumn(
                    "validation_errors",
                    when(
                        col("rule_valid") == False,
                        array_union(col("validation_errors"), array(lit(f"{column}: {error_message}")))
                    ).otherwise(col("validation_errors"))
                )
                
                # Drop temporary column
                df = df.drop("rule_valid")
                
                logger.debug(f"Applied validation to column: {column}")
            else:
                logger.warning(f"Validation column not found: {column}")
        
        # Split into valid and invalid DataFrames
        valid_df = df.filter(col("is_valid") == True).drop("is_valid", "validation_errors")
        invalid_df = df.filter(col("is_valid") == False)
        
        # Collect validation results
        validation_results = {
            "total_records": df.count(),
            "valid_records": valid_df.count(),
            "invalid_records": invalid_df.count(),
            "validation_rate": (valid_df.count() / df.count()) * 100 if df.count() > 0 else 100.0
        }
        
        logger.info(f"Validation complete: {validation_results['valid_records']} valid, "
                   f"{validation_results['invalid_records']} invalid")
        
        return valid_df, invalid_df, validation_results