"""
Logging configuration for the pharmacy ETL pipeline
"""

import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# Base directory
BASE_DIR = Path(__file__).parent.parent
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Log file paths
MAIN_LOG_FILE = os.path.join(LOG_DIR, "etl_pipeline.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "error.log")

# Log format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Configure logging
def configure_logging(name="pharmacy_etl"):
    """Configure logging for the ETL pipeline"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create formatters
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    
    # Create handlers
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # File handler - main log
    file_handler = RotatingFileHandler(
        MAIN_LOG_FILE, maxBytes=10485760, backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # File handler - error log
    error_handler = RotatingFileHandler(
        ERROR_LOG_FILE, maxBytes=10485760, backupCount=5
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(error_handler)
    
    return logger

def get_logger(name):
    """Get a logger for a specific module"""
    return logging.getLogger(name)
