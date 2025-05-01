"""
Sales Data Generator
Generates synthetic sales transaction data for pharmacy ETL pipeline
"""

import os
import random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker

from config.config import DATA_GEN_CONFIG, RAW_DATA_DIR
from config.logging_config import get_logger

logger = get_logger(__name__)
fake = Faker()

class SalesGenerator:
    """Generates synthetic sales transaction data"""
    
    def __init__(self, stores_df, products_df, customers_df, employees_df):
        """
        Initialize the sales generator
        
        Args:
            stores_df: DataFrame containing store data
            products_df: DataFrame containing product data
            customers_df: DataFrame containing customer data
            employees_df: DataFrame containing employee data
        """
        self.stores_df = stores_df
        self.products_df = products_df
        self.customers_df = customers_df
        self.employees_df = employees_df
        
        # Parse date range from config
        self.start_date = datetime.strptime(
            DATA_GEN_CONFIG["date_range"]["start_date"], "%Y-%m-%d"
        )
        self.end_date = datetime.strptime(
            DATA_GEN_CONFIG["date_range"]["end_date"], "%Y-%m-%d"
        )
        
        # Transaction patterns by hour (0-23)
        self.hourly_patterns = {
            0: 0.01, 1: 0.005, 2: 0.005, 3: 0.005, 4: 0.01, 5: 0.02,
            6: 0.03, 7: 0.05, 8: 0.07, 9: 0.08, 10: 0.09, 11: 0.09,
            12: 0.10, 13: 0.09, 14: 0.08, 15: 0.07, 16: 0.08, 17: 0.09,
            18: 0.08, 19: 0.07, 20: 0.05, 21: 0.03, 22: 0.02, 23: 0.01
        }
        
        # Transaction patterns by day of week (0=Monday, 6=Sunday)
        self.day_of_week_patterns = {
            0: 1.0, 1: 0.9, 2: 0.85, 3: 0.9, 4: 1.1, 5: 1.3, 6: 0.7
        }
        
        # Payment method distribution
        self.payment_methods = {
            "Credit Card": 0.45, 
            "Debit Card": 0.30, 
            "Cash": 0.15, 
            "Insurance": 0.08,
            "Mobile Payment": 0.02
        }
        
        logger.info("Sales generator initialized")
    
    def _generate_transaction_time(self, date):
        """Generate a realistic transaction time based on hour patterns"""
        # Select hour based on distribution
        hour = np.random.choice(
            list(self.hourly_patterns.keys()),
            p=list(self.hourly_patterns.values())
        )
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        return datetime.combine(date, datetime.min.time()) + timedelta(hours=hour, minutes=minute, seconds=second)
    
    def _generate_transaction_count(self, date):
        """Generate number of transactions for a given date"""
        base_count = DATA_GEN_CONFIG["num_transactions_per_day"]
        
        # Apply day of week factor
        day_factor = self.day_of_week_patterns[date.weekday()]
        
        # Apply month factor (busier in winter months)
        month = date.month
        month_factor = 1.0 + 0.2 * (abs(month - 6) / 6)  # Higher in Jan/Dec, lower in Jun/Jul
        
        # Apply random noise (±10%)
        noise_factor = random.uniform(0.9, 1.1)
        
        return int(base_count * day_factor * month_factor * noise_factor)
    
    def _select_payment_method(self):
        """Select a payment method based on distribution"""
        return np.random.choice(
            list(self.payment_methods.keys()),
            p=list(self.payment_methods.values())
        )
    
    def _generate_transaction_items(self, transaction_id, store_id, transaction_date, is_prescription):
        """Generate items for a transaction"""
        items = []
        
        # Determine number of items in transaction (1-8)
        num_items = np.random.choice(
            [1, 2, 3, 4, 5, 6, 7, 8],
            p=[0.25, 0.30, 0.20, 0.10, 0.07, 0.05, 0.02, 0.01]
        )
        
        # Filter products by prescription requirement if needed
        if is_prescription:
            eligible_products = self.products_df[self.products_df["requires_prescription"] == True]
        else:
            eligible_products = self.products_df
        
        # If no eligible products, use all products
        if len(eligible_products) == 0:
            eligible_products = self.products_df
        
        # Select random products
        selected_products = eligible_products.sample(
            n=min(num_items, len(eligible_products)),
            replace=False
        )
        
        detail_id = 1
        for _, product in selected_products.iterrows():
            # Determine quantity (usually 1 for prescriptions, 1-3 for others)
            if is_prescription and product["requires_prescription"]:
                quantity = 1
            else:
                quantity = np.random.choice([1, 2, 3], p=[0.7, 0.2, 0.1])
            
            # Apply discount?
            discount_rate = 0.0
            if random.random() < 0.2:  # 20% chance of discount
                discount_rate = random.choice([0.05, 0.10, 0.15, 0.20])
            
            unit_price = product["unit_price"]
            discount_amount = round(unit_price * discount_rate, 2)
            total_price = round((unit_price - discount_amount) * quantity, 2)
            
            items.append({
                "detail_id": f"{transaction_id}_D{detail_id}",
                "transaction_id": transaction_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": discount_amount,
                "total_price": total_price,
                "refund_flag": False
            })
            
            detail_id += 1
        
        return items
    
    def generate_daily_transactions(self, date):
        """Generate transactions for a specific date"""
        transactions = []
        transaction_details = []
        
        # Determine number of transactions for this date
        num_transactions = self._generate_transaction_count(date)
        logger.info(f"Generating {num_transactions} transactions for {date}")
        
        for i in range(num_transactions):
            # Select store
            store = self.stores_df.sample(1).iloc[0]
            store_id = store["store_id"]
            
            # Select employee from this store
            store_employees = self.employees_df[self.employees_df["store_id"] == store_id]
            if len(store_employees) == 0:
                # If no employees at this store, select any employee
                employee = self.employees_df.sample(1).iloc[0]
            else:
                employee = store_employees.sample(1).iloc[0]
            
            # Select customer (80% chance of having customer record)
            customer_id = None
            if random.random() < 0.8:
                customer = self.customers_df.sample(1).iloc[0]
                customer_id = customer["customer_id"]
            
            # Generate transaction time
            transaction_time = self._generate_transaction_time(date)
            
            # Determine if prescription transaction
            is_prescription = random.random() < 0.4  # 40% are prescription transactions
            
            # Select payment method
            payment_method = self._select_payment_method()
            
            # Generate loyalty discount flag
            loyalty_discount = False
            if customer_id and random.random() < 0.3:  # 30% chance for loyalty members
                loyalty_discount = True
            
            # Generate transaction status
            transaction_status = "Completed"
            if random.random() < 0.02:  # 2% chance of voided transaction
                transaction_status = "Voided"
            
            # Create transaction ID
            transaction_id = f"TRX{date.strftime('%Y%m%d')}{i+1:04d}"
            
            # Generate transaction items
            items = self._generate_transaction_items(
                transaction_id, store_id, date, is_prescription
            )
            
            # Calculate total amount
            total_amount = sum(item["total_price"] for item in items)
            
            # Create transaction record
            transaction = {
                "transaction_id": transaction_id,
                "store_id": store_id,
                "customer_id": customer_id,
                "employee_id": employee["employee_id"],
                "transaction_date": date,
                "transaction_time": transaction_time.strftime("%H:%M:%S"),
                "total_amount": total_amount,
                "payment_method": payment_method,
                "prescription_flag": is_prescription,
                "loyalty_discount_applied": loyalty_discount,
                "transaction_status": transaction_status
            }
            
            transactions.append(transaction)
            transaction_details.extend(items)
        
        return transactions, transaction_details
    
    def generate_data(self):
        """Generate all sales data for the configured date range"""
        all_transactions = []
        all_transaction_details = []
        
        current_date = self.start_date
        while current_date <= self.end_date:
            transactions, transaction_details = self.generate_daily_transactions(current_date)
            all_transactions.extend(transactions)
            all_transaction_details.extend(transaction_details)
            
            current_date += timedelta(days=1)
        
        # Convert to DataFrames
        transactions_df = pd.DataFrame(all_transactions)
        transaction_details_df = pd.DataFrame(all_transaction_details)
        
        # Save to CSV
        transactions_df.to_csv(os.path.join(RAW_DATA_DIR, "transactions.csv"), index=False)
        transaction_details_df.to_csv(os.path.join(RAW_DATA_DIR, "transaction_details.csv"), index=False)
        
        logger.info(f"Generated {len(transactions_df)} transactions with {len(transaction_details_df)} line items")
        
        return transactions_df, transaction_details_df