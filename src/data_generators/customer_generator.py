"""
Customer Data Generator
Generates synthetic customer data for pharmacy ETL pipeline
"""

import os
import random
import pandas as pd
from datetime import datetime, timedelta
from faker import Faker

from config.config import DATA_GEN_CONFIG, RAW_DATA_DIR
from config.logging_config import get_logger

logger = get_logger(__name__)
fake = Faker()

class CustomerGenerator:
    """Generates synthetic customer data"""
    
    def __init__(self, stores_df=None):
        """
        Initialize the customer generator
        
        Args:
            stores_df: Optional DataFrame containing store data for preferred stores
        """
        self.stores_df = stores_df
        
        # Parse date range from config
        self.start_date = datetime.strptime(
            DATA_GEN_CONFIG["date_range"]["start_date"], "%Y-%m-%d"
        )
        self.end_date = datetime.strptime(
            DATA_GEN_CONFIG["date_range"]["end_date"], "%Y-%m-%d"
        )
        
        # Insurance providers
        self.insurance_providers = [
            "BlueCross BlueShield", "Aetna", "UnitedHealthcare", 
            "Cigna", "Humana", "Medicare", "Medicaid", None
        ]
        
        logger.info("Customer generator initialized")
    
    def generate_customers(self):
        """Generate customer data"""
        customers = []
        
        num_customers = DATA_GEN_CONFIG["num_customers"]
        logger.info(f"Generating {num_customers} customers")
        
        for i in range(num_customers):
            customer_id = f"CUS{i+1:06d}"
            
            # Basic customer info
            first_name = fake.first_name()
            last_name = fake.last_name()
            
            # Contact info (some may be missing)
            email = fake.email() if random.random() < 0.8 else None
            phone = fake.phone_number() if random.random() < 0.9 else None
            
            # Address (some may be missing)
            has_address = random.random() < 0.85
            address = fake.street_address() if has_address else None
            city = fake.city() if has_address else None
            state = fake.state_abbr() if has_address else None
            zip_code = fake.zipcode() if has_address else None
            
            # Demographics
            has_demographics = random.random() < 0.7
            dob = fake.date_of_birth(minimum_age=18, maximum_age=90) if has_demographics else None
            gender = random.choice(["M", "F", "Other", None]) if has_demographics else None
            
            # Loyalty program
            is_loyalty_member = random.random() < 0.6
            loyalty_points = random.randint(0, 5000) if is_loyalty_member else 0
            
            # Registration date
            max_reg_date = min(self.end_date, datetime.now())
            customer_since = fake.date_between(
                start_date="-5y", 
                end_date=max_reg_date
            )
            
            # Preferred store
            preferred_store_id = None
            if self.stores_df is not None and random.random() < 0.7:
                preferred_store_id = self.stores_df.sample(1).iloc[0]["store_id"]
            
            # Insurance info
            has_insurance = random.random() < 0.6
            insurance_provider = random.choice(self.insurance_providers) if has_insurance else None
            insurance_policy = f"POL{fake.random_number(digits=8)}" if insurance_provider else None
            
            customers.append({
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "phone_number": phone,
                "address": address,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "date_of_birth": dob,
                "gender": gender,
                "loyalty_member": is_loyalty_member,
                "loyalty_points": loyalty_points,
                "customer_since": customer_since,
                "preferred_store_id": preferred_store_id,
                "insurance_provider": insurance_provider,
                "insurance_policy_number": insurance_policy
            })
        
        # Convert to DataFrame
        customers_df = pd.DataFrame(customers)
        
        # Save to CSV
        customers_df.to_csv(os.path.join(RAW_DATA_DIR, "customers.csv"), index=False)
        
        logger.info(f"Generated {len(customers_df)} customer records")
        
        return customers_df
    
    def generate_data(self):
        """Generate all customer data"""
        return self.generate_customers()
