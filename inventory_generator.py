"""
Inventory Data Generator
Generates synthetic inventory data for pharmacy ETL pipeline
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

class InventoryGenerator:
    """Generates synthetic inventory data"""
    
    def __init__(self, stores_df, products_df, suppliers_df):
        """
        Initialize the inventory generator
        
        Args:
            stores_df: DataFrame containing store data
            products_df: DataFrame containing product data
            suppliers_df: DataFrame containing supplier data
        """
        self.stores_df = stores_df
        self.products_df = products_df
        self.suppliers_df = suppliers_df
        
        # Parse date range from config
        self.start_date = datetime.strptime(
            DATA_GEN_CONFIG["date_range"]["start_date"], "%Y-%m-%d"
        )
        self.end_date = datetime.strptime(
            DATA_GEN_CONFIG["date_range"]["end_date"], "%Y-%m-%d"
        )
        
        logger.info("Inventory generator initialized")
    
    def generate_initial_inventory(self):
        """Generate initial inventory data for all stores and products"""
        inventory_records = []
        inventory_id = 1
        
        logger.info("Generating initial inventory data")
        
        # For each store
        for _, store in self.stores_df.iterrows():
            store_id = store["store_id"]
            
            # Not all products are available in all stores
            # Larger stores have more products
            store_size = store.get("store_size_sqft", 5000)
            product_coverage = min(0.9, store_size / 10000)  # Max 90% coverage
            
            num_products = int(len(self.products_df) * product_coverage)
            store_products = self.products_df.sample(n=num_products)
            
            for _, product in store_products.iterrows():
                product_id = product["product_id"]
                
                # Determine inventory levels based on product type
                is_prescription = product.get("requires_prescription", False)
                
                if is_prescription:
                    # Prescription drugs have lower stock levels
                    quantity = random.randint(5, 30)
                    min_stock = random.randint(3, 10)
                    max_stock = random.randint(30, 50)
                    reorder_point = random.randint(min_stock, min_stock + 5)
                else:
                    # OTC products have higher stock levels
                    quantity = random.randint(10, 100)
                    min_stock = random.randint(5, 20)
                    max_stock = random.randint(50, 150)
                    reorder_point = random.randint(min_stock, min_stock + 10)
                
                # Generate expiry date for pharmaceuticals
                expiry_date = None
                lot_number = None
                if is_prescription or random.random() < 0.7:  # Most products have expiry dates
                    expiry_date = fake.date_between(
                        start_date="+3m", 
                        end_date="+2y"
                    )
                    lot_number = f"LOT{fake.random_number(digits=6)}"
                
                # Last restock date
                last_restock_date = fake.date_between(
                    start_date="-3m",
                    end_date="-1d"
                )
                
                # Last restock quantity
                last_restock_quantity = random.randint(5, 50)
                
                # Calculate inventory value
                unit_cost = product.get("unit_cost", 0)
                inventory_value = round(quantity * unit_cost, 2)
                
                # Determine inventory status
                if quantity <= min_stock:
                    status = "Critical"
                elif quantity <= reorder_point:
                    status = "Low"
                elif quantity >= max_stock * 0.9:
                    status = "Overstocked"
                else:
                    status = "Normal"
                
                inventory_records.append({
                    "inventory_id": f"INV{inventory_id:06d}",
                    "store_id": store_id,
                    "product_id": product_id,
                    "date_recorded": self.start_date,
                    "quantity_on_hand": quantity,
                    "quantity_on_order": 0 if status in ["Normal", "Overstocked"] else random.randint(5, 20),
                    "min_stock_level": min_stock,
                    "max_stock_level": max_stock,
                    "reorder_point": reorder_point,
                    "expiry_date": expiry_date,
                    "lot_number": lot_number,
                    "last_restock_date": last_restock_date,
                    "last_restock_quantity": last_restock_quantity,
                    "inventory_value": inventory_value
                })
                
                inventory_id += 1
        
        # Convert to DataFrame
        inventory_df = pd.DataFrame(inventory_records)
        
        # Save to CSV
        inventory_df.to_csv(os.path.join(RAW_DATA_DIR, "inventory.csv"), index=False)
        
        logger.info(f"Generated {len(inventory_df)} inventory records")
        
        return inventory_df
    
    def generate_inventory_movements(self):
        """Generate inventory movement data (receipts, adjustments, etc.)"""
        # This would generate inventory movement records
        # For simplicity, we'll skip this in the initial implementation
        pass
    
    def generate_data(self):
        """Generate all inventory data"""
        inventory_df = self.generate_initial_inventory()
        self.generate_inventory_movements()
        
        return inventory_df