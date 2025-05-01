"""
Data Loader
Contains classes for loading data into target destinations
"""

import os
from pyspark.sql import DataFrame
from config.config import WAREHOUSE_DATA_DIR, DB_CONFIG
from config.logging_config import get_logger

logger = get_logger(__name__)

class DataLoader:
    """Class for loading data into target destinations"""
    
    def __init__(self, spark):
        """
        Initialize the data loader
        
        Args:
            spark: SparkSession
        """
        self.spark = spark
        logger.info("Initialized DataLoader")
    
    def load_to_warehouse(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Load data to the data warehouse
        
        Args:
            df: DataFrame to load
            table_name: Name of the target table
            mode: Write mode (overwrite, append, etc.)
        """
        logger.info(f"Loading data to warehouse table: {table_name}")
        
        try:
            # Save as parquet in warehouse directory
            warehouse_path = os.path.join(WAREHOUSE_DATA_DIR, f"{table_name}.parquet")
            df.write.parquet(warehouse_path, mode=mode)
            
            logger.info(f"Successfully loaded {df.count()} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Error loading data to {table_name}: {str(e)}")
            raise
    
    def load_to_database(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """
        Load data to a database
        
        Args:
            df: DataFrame to load
            table_name: Name of the target table
            mode: Write mode (overwrite, append, etc.)
        """
        logger.info(f"Loading data to database table: {table_name}")
        
        try:
            # Configure database connection properties
            connection_properties = {
                "url": f"jdbc:{DB_CONFIG['type']}://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
                "user": DB_CONFIG["username"],
                "password": DB_CONFIG["password"],
                "driver": self._get_jdbc_driver(DB_CONFIG["type"])
            }
            
            # Write to database
            df.write.jdbc(
                url=connection_properties["url"],
                table=table_name,
                mode=mode,
                properties=connection_properties
            )
            
            logger.info(f"Successfully loaded {df.count()} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Error loading data to {table_name}: {str(e)}")
            raise
    
    def _get_jdbc_driver(self, db_type: str) -> str:
        """Get JDBC driver class for database type"""
        drivers = {
            "postgres": "org.postgresql.Driver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "sqlite": "org.sqlite.JDBC"
        }
        return drivers.get(db_type, "org.postgresql.Driver")