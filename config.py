"""
Configuration settings for the pharmacy ETL pipeline
"""

import os
from pathlib import Path

# Base directories
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
WAREHOUSE_DATA_DIR = os.path.join(DATA_DIR, "warehouse")

# Ensure directories exist
for directory in [RAW_DATA_DIR, PROCESSED_DATA_DIR, WAREHOUSE_DATA_DIR]:
    os.makedirs(directory, exist_ok=True)

# Database configuration
DB_CONFIG = {
    "warehouse": {
        "type": "postgres",
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": os.environ.get("DB_PORT", 5432),
        "database": os.environ.get("DB_NAME", "pharmacy_warehouse"),
        "user": os.environ.get("DB_USER", "postgres"),
        "password": os.environ.get("DB_PASSWORD", "postgres"),
    }
}

# Data source configuration
DATA_SOURCES = {
    "sales": {
        "format": "csv",
        "path": os.path.join(RAW_DATA_DIR, "sales"),
        "frequency": "daily",
    },
    "inventory": {
        "format": "csv",
        "path": os.path.join(RAW_DATA_DIR, "inventory"),
        "frequency": "daily",
    },
    "customers": {
        "format": "csv",
        "path": os.path.join(RAW_DATA_DIR, "customers"),
        "frequency": "monthly",
    },
    "products": {
        "format": "csv",
        "path": os.path.join(RAW_DATA_DIR, "products"),
        "frequency": "monthly",
    },
    "prescriptions": {
        "format": "csv",
        "path": os.path.join(RAW_DATA_DIR, "prescriptions"),
        "frequency": "daily",
    },
}

# Processing configuration
PROCESSING_CONFIG = {
    "batch_size": 10000,
    "parallel_jobs": 4,
}

# Reporting configuration
REPORTING_CONFIG = {
    "daily_reports": [
        "sales_summary",
        "inventory_status",
        "prescription_fills",
    ],
    "monthly_reports": [
        "sales_by_category",
        "customer_demographics",
        "product_performance",
    ],
    "yearly_reports": [
        "annual_sales_analysis",
        "customer_retention",
        "product_profitability",
    ],
}