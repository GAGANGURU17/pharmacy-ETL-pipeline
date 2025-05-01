"""
Helper Functions
Contains utility functions used across the ETL pipeline
"""

import os
from datetime import datetime
from typing import Dict, Any

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate configuration parameters
    
    Args:
        config: Configuration dictionary to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_keys = [
        "app_name",
        "master",
        "executor_memory",
        "driver_memory"
    ]
    
    return all(key in config for key in required_keys)

def create_directory_if_not_exists(directory: str) -> None:
    """
    Create directory if it doesn't exist
    
    Args:
        directory: Directory path to create
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_current_timestamp() -> str:
    """
    Get current timestamp in standard format
    
    Returns:
        str: Formatted timestamp
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def format_file_size(size_in_bytes: int) -> str:
    """
    Format file size in human-readable format
    
    Args:
        size_in_bytes: File size in bytes
        
    Returns:
        str: Formatted file size
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024
    return f"{size_in_bytes:.2f} TB"
