"""
Main ETL Pipeline
Orchestrates the entire ETL process
"""

import time
import concurrent.futures
from pyspark.sql import SparkSession
from config.config import SPARK_CONFIG
from config.logging_config import get_logger
from extraction.data_extractors import PharmacyDataExtractor
from transformation.data_cleaner import DataCleaner
from transformation.data_transformer import DataTransformer
from loading.data_loader import DataLoader
from utils.performance_monitor import PerformanceMonitor

logger = get_logger(__name__)

def run_etl_pipeline(parallel=True, monitor_performance=True):
    """
    Run the complete ETL pipeline
    
    Args:
        parallel: Whether to run transformations in parallel
        monitor_performance: Whether to monitor and log performance metrics
    """
    logger.info("Starting ETL pipeline")
    start_time = time.time()
    performance_monitor = PerformanceMonitor() if monitor_performance else None
    spark = None
    
    try:
        # Initialize Spark session
        if monitor_performance:
            with performance_monitor.measure_time("spark_initialization"):
                spark = (SparkSession.builder
                        .appName(SPARK_CONFIG["app_name"])
                        .master(SPARK_CONFIG["master"])
                        .config("spark.executor.memory", SPARK_CONFIG["executor_memory"])
                        .config("spark.driver.memory", SPARK_CONFIG["driver_memory"])
                        .getOrCreate())
        else:
            spark = (SparkSession.builder
                    .appName(SPARK_CONFIG["app_name"])
                    .master(SPARK_CONFIG["master"])
                    .config("spark.executor.memory", SPARK_CONFIG["executor_memory"])
                    .config("spark.driver.memory", SPARK_CONFIG["driver_memory"])
                    .getOrCreate())
        
        # Initialize components
        extractor = PharmacyDataExtractor(spark)
        cleaner = DataCleaner(spark)
        transformer = DataTransformer(spark)
        loader = DataLoader(spark)
        
        # Extract data
        logger.info("Extracting data")
        if monitor_performance:
            with performance_monitor.measure_time("data_extraction"):
                raw_data = extractor.extract_all(validate=True)
        else:
            raw_data = extractor.extract_all(validate=True)
        
        # Clean data
        logger.info("Cleaning data")
        cleaned_data = {}
        
        if monitor_performance:
            with performance_monitor.measure_time("data_cleaning"):
                for name, df in raw_data.items():
                    cleaned_df = cleaner.handle_missing_values(df)
                    cleaned_df = cleaner.clean_string_columns(cleaned_df)
                    cleaned_data[name] = cleaned_df
        else:
            for name, df in raw_data.items():
                cleaned_df = cleaner.handle_missing_values(df)
                cleaned_df = cleaner.clean_string_columns(cleaned_df)
                cleaned_data[name] = cleaned_df
        
        # Transform data
        logger.info("Transforming data")
        
        # Function to create dimension tables
        def create_dim_time():
            return transformer.create_time_dimension(
                cleaned_data["transactions"], 
                "transaction_date",
                "transaction_time"
            )
        
        def create_dim_customer():
            return transformer.create_dim_customer(
                cleaned_data["customers"]
            )
        
        def create_dim_product():
            return transformer.create_dim_product(
                cleaned_data["products"]
            )
        
        def create_fact_sales():
            return transformer.create_fact_sales(
                cleaned_data["transactions"],
                cleaned_data["transaction_details"]
            )
        
        # Create dimensions and facts
        if parallel and monitor_performance:
            with performance_monitor.measure_time("data_transformation_parallel"):
                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                    future_dim_time = executor.submit(create_dim_time)
                    future_dim_customer = executor.submit(create_dim_customer)
                    future_dim_product = executor.submit(create_dim_product)
                    future_fact_sales = executor.submit(create_fact_sales)
                    
                    dim_time = future_dim_time.result()
                    dim_customer = future_dim_customer.result()
                    dim_product = future_dim_product.result()
                    fact_sales = future_fact_sales.result()
        elif monitor_performance:
            with performance_monitor.measure_time("data_transformation_sequential"):
                dim_time = create_dim_time()
                dim_customer = create_dim_customer()
                dim_product = create_dim_product()
                fact_sales = create_fact_sales()
        else:
            dim_time = create_dim_time()
            dim_customer = create_dim_customer()
            dim_product = create_dim_product()
            fact_sales = create_fact_sales()
        
        # Load data
        logger.info("Loading data to warehouse")
        
        if monitor_performance:
            with performance_monitor.measure_time("data_loading"):
                # Load dimensions
                loader.load_to_warehouse(dim_time, "dim_time")
                loader.load_to_warehouse(dim_customer, "dim_customer")
                loader.load_to_warehouse(dim_product, "dim_product")
                
                # Load facts
                loader.load_to_warehouse(fact_sales, "fact_sales")
        else:
            # Load dimensions
            loader.load_to_warehouse(dim_time, "dim_time")
            loader.load_to_warehouse(dim_customer, "dim_customer")
            loader.load_to_warehouse(dim_product, "dim_product")
            
            # Load facts
            loader.load_to_warehouse(fact_sales, "fact_sales")
        
        end_time = time.time()
        total_time = end_time - start_time
        logger.info(f"ETL pipeline completed successfully in {total_time:.2f} seconds")
        
        if monitor_performance:
            performance_monitor.log_summary()
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise
    
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    run_etl_pipeline(parallel=True, monitor_performance=True)