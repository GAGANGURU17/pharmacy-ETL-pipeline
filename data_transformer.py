"""
Data Transformer
Contains functions for transforming data into analytics-ready formats
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, year, month, dayofmonth, hour, minute, date_format, 
    sum as spark_sum, avg, count, max as spark_max, min as spark_min, 
    round as spark_round, datediff, to_date, lit, when, concat, 
    expr, row_number, dense_rank
)

from config.logging_config import get_logger

logger = get_logger(__name__)

class DataTransformer:
    """Class for transforming data into analytics-ready formats"""
    
    def __init__(self, spark):
        """
        Initialize the data transformer
        
        Args:
            spark: SparkSession
        """
        self.spark = spark
        logger.info("Initialized DataTransformer")
    
    def create_time_dimension(self, df, date_column, time_column=None):
        """
        Create a time dimension table from a date column
        
        Args:
            df: DataFrame containing the date column
            date_column: Name of the date column
            time_column: Optional name of the time column
            
        Returns:
            Time dimension DataFrame
        """
        logger.info(f"Creating time dimension from column: {date_column}")
        
        # Extract distinct dates
        distinct_dates = df.select(col(date_column).alias("date")).distinct()
        
        # Create time dimension
        time_dim = distinct_dates.select(
            col("date"),
            date_format(col("date"), "yyyy-MM-dd").alias("date_string"),
            year(col("date")).alias("year"),
            month(col("date")).alias("month"),
            dayofmonth(col("date")).alias("day"),
            date_format(col("date"), "EEEE").alias("day_of_week"),
            date_format(col("date"), "w").alias("week_of_year"),
            date_format(col("date"), "MMMM").alias("month_name"),
            date_format(col("date"), "Q").alias("quarter"),
            when(date_format(col("date"), "d").isin(["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]), "Early")
            .when(date_format(col("date"), "d").isin(["11", "12", "13", "14", "15", "16", "17", "18", "19", "20"]), "Mid")
            .otherwise("Late").alias("part_of_month"),
            when(date_format(col("date"), "D").between(1, 90), "Q1")
            .when(date_format(col("date"), "D").between(91, 181), "Q2")
            .when(date_format(col("date"), "D").between(182, 273), "Q3")
            .otherwise("Q4").alias("quarter_name"),
            when(date_format(col("date"), "EEEE").isin(["Saturday", "Sunday"]), True)
            .otherwise(False).alias("is_weekend"),
            # Add fiscal year if needed
            year(col("date")).alias("fiscal_year")
        )
        
        # Add time components if time column is provided
        if time_column:
            time_df = df.select(
                col(date_column).alias("date"),
                col(time_column).alias("time")
            ).distinct()
            
            time_df = time_df.select(
                col("date"),
                col("time"),
                hour(col("time")).alias("hour"),
                minute(col("time")).alias("minute"),
                when(hour(col("time")).between(0, 5), "Night")
                .when(hour(col("time")).between(6, 11), "Morning")
                .when(hour(col("time")).between(12, 17), "Afternoon")
                .otherwise("Evening").alias("time_of_day")
            )
            
            # Join with time dimension
            time_dim = time_dim.join(time_df, "date", "left")
        
        logger.info(f"Created time dimension with {time_dim.count()} rows")
        
        return time_dim
    
    def create_fact_sales(self, transactions_df, transaction_details_df):
        """
        Create the fact_sales table
        
        Args:
            transactions_df: Transactions DataFrame
            transaction_details_df: Transaction details DataFrame
            
        Returns:
            Fact sales DataFrame
        """
        logger.info("Creating fact_sales table")
        
        # Join transactions with transaction details
        fact_sales = transactions_df.join(
            transaction_details_df,
            "transaction_id",
            "inner"
        )
        
        # Select relevant columns
        fact_sales = fact_sales.select(
            col("detail_id"),
            col("transaction_id"),
            col("store_id"),
            col("customer_id"),
            col("employee_id"),
            col("product_id"),
            col("transaction_date"),
            col("transaction_time"),
            col("quantity"),
            col("unit_price"),
            col("discount_amount"),
            col("total_price"),
            col("payment_method"),
            col("prescription_flag"),
            col("loyalty_discount_applied"),
            col("transaction_status"),
            # Calculate profit
            (col("total_price") - (col("unit_price") * col("quantity") - col("discount_amount"))).alias("profit")
        )
        
        # Filter out voided transactions
        fact_sales = fact_sales.filter(col("transaction_status") != "Voided")
        
        logger.info(f"Created fact_sales table with {fact_sales.count()} rows")
        
        return fact_sales
    
    def create_dim_customer(self, customers_df):
        """
        Create the customer dimension table
        
        Args:
            customers_df: Customers DataFrame
            
        Returns:
            Customer dimension DataFrame
        """
        logger.info("Creating customer dimension table")
        
        # Select relevant columns and add derived attributes
        dim_customer = customers_df.select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
            col("email"),
            col("phone_number"),
            col("address"),
            col("city"),
            col("state"),
            col("zip_code"),
            col("date_of_birth"),
            col("gender"),
            col("loyalty_member"),
            col("loyalty_points"),
            col("customer_since"),
            col("preferred_store_id"),
            col("insurance_provider"),
            col("insurance_policy_number"),
            # Add age calculation
            when(col("date_of_birth").isNotNull(),
                 datediff(current_date(), col("date_of_birth")) / 365.25
            ).otherwise(None).cast("int").alias("age"),
            # Add customer tenure in days
            when(col("customer_since").isNotNull(),
                 datediff(current_date(), col("customer_since"))
            ).otherwise(0).alias("customer_tenure_days")
        )
        
        # Add age group
        dim_customer = dim_customer.withColumn(
            "age_group",
            when(col("age").isNull(), "Unknown")
            .when(col("age") < 18, "Under 18")
            .when(col("age").between(18, 24), "18-24")
            .when(col("age").between(25, 34), "25-34")
            .when(col("age").between(35, 44), "35-44")
            .when(col("age").between(45, 54), "45-54")
            .when(col("age").between(55, 64), "55-64")
            .when(col("age") >= 65, "65+")
            .otherwise("Unknown")
        )
        
        # Add customer tenure group
        dim_customer = dim_customer.withColumn(
            "tenure_group",
            when(col("customer_tenure_days") < 30, "New (< 30 days)")
            .when(col("customer_tenure_days").between(30, 90), "1-3 months")
            .when(col("customer_tenure_days").between(91, 365), "3-12 months")
            .when(col("customer_tenure_days").between(366, 730), "1-2 years")
            .when(col("customer_tenure_days") > 730, "2+ years")
            .otherwise("Unknown")
        )
        
        logger.info(f"Created customer dimension with {dim_customer.count()} rows")
        
        return dim_customer
    
    def create_dim_product(self, products_df):
        """
        Create the product dimension table
        
        Args:
            products_df: Products DataFrame
            
        Returns:
            Product dimension DataFrame
        """
        logger.info("Creating product dimension table")
        
        # Select relevant columns and add derived attributes
        dim_product = products_df.select(
            col("product_id"),
            col("product_name"),
            col("description"),
            col("category"),
            col("subcategory"),
            col("brand"),
            col("supplier_id"),
            col("unit_price"),
            col("unit_cost"),
            col("tax_rate"),
            col("requires_prescription"),
            # Calculate profit margin
            ((col("unit_price") - col("unit_cost")) / col("unit_price") * 100).alias("profit_margin_percent"),
            # Add price tier
            when(col("unit_price") < 10, "Budget")
            .when(col("unit_price").between(10, 50), "Standard")
            .when(col("unit_price").between(50.01, 100), "Premium")
            .when(col("unit_price") > 100, "Luxury")
            .otherwise("Unknown").alias("price_tier")
        )
        
        logger.info(f"Created product dimension with {dim_product.count()} rows")
        
        return dim_product
    
    def transform_data(self, extracted_data):
        """
        Transform extracted data into dimensional model
        
        Args:
            extracted_data: Dictionary of extracted DataFrames
            
        Returns:
            Dictionary of transformed dimension and fact tables
        """
        logger.info("Starting data transformation process")
        
        # Create dimension tables
        dim_time = self.create_time_dimension(
            extracted_data["transactions"], 
            "transaction_date", 
            "transaction_time"
        )
        
        dim_customer = self.create_dim_customer(extracted_data["customers"])
        dim_product = self.create_dim_product(extracted_data["products"])
        
        # Create fact tables
        fact_sales = self.create_fact_sales(
            extracted_data["transactions"],
            extracted_data["transaction_details"]
        )
        
        # Return transformed data
        transformed_data = {
            "dim_time": dim_time,
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "fact_sales": fact_sales
        }
        
        logger.info("Data transformation complete")
        
        return transformed_data