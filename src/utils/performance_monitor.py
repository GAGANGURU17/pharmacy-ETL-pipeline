"""
Performance Monitoring Utility
Provides tools for monitoring and logging performance metrics
"""

import time
import contextlib
from config.logging_config import get_logger

logger = get_logger(__name__)

class PerformanceMonitor:
    """Class for monitoring and logging performance metrics"""
    
    def __init__(self):
        """Initialize the performance monitor"""
        self.metrics = {}
        logger.info("Initialized PerformanceMonitor")
    
    @contextlib.contextmanager
    def measure_time(self, operation_name):
        """
        Context manager to measure execution time of an operation
        
        Args:
            operation_name: Name of the operation being measured
        """
        start_time = time.time()
        try:
            yield
        finally:
            end_time = time.time()
            execution_time = end_time - start_time
            
            if operation_name in self.metrics:
                self.metrics[operation_name].append(execution_time)
            else:
                self.metrics[operation_name] = [execution_time]
            
            logger.info(f"Operation '{operation_name}' completed in {execution_time:.2f} seconds")
    
    def log_summary(self):
        """Log a summary of all performance metrics"""
        logger.info("Performance Summary:")
        
        for operation, times in self.metrics.items():
            total_time = sum(times)
            avg_time = total_time / len(times)
            min_time = min(times)
            max_time = max(times)
            
            logger.info(f"  {operation}:")
            logger.info(f"    Executions: {len(times)}")
            logger.info(f"    Total time: {total_time:.2f} seconds")
            logger.info(f"    Average time: {avg_time:.2f} seconds")
            logger.info(f"    Min time: {min_time:.2f} seconds")
            logger.info(f"    Max time: {max_time:.2f} seconds")
