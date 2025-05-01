"""
Error Handler Utility
Provides tools for handling and reporting errors
"""

import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config.logging_config import get_logger
from config.config import EMAIL_CONFIG

logger = get_logger(__name__)

class ErrorHandler:
    """Class for handling and reporting errors"""
    
    @staticmethod
    def format_error(error, include_traceback=True):
        """
        Format an error for logging and reporting
        
        Args:
            error: The exception object
            include_traceback: Whether to include the traceback
            
        Returns:
            Formatted error message
        """
        error_type = type(error).__name__
        error_message = str(error)
        
        formatted_error = f"Error Type: {error_type}\nError Message: {error_message}"
        
        if include_traceback:
            error_traceback = traceback.format_exc()
            formatted_error += f"\n\nTraceback:\n{error_traceback}"
        
        return formatted_error
    
    @staticmethod
    def log_error(error, context=None):
        """
        Log an error with context
        
        Args:
            error: The exception object
            context: Additional context about where the error occurred
        """
        error_message = ErrorHandler.format_error(error)
        
        if context:
            logger.error(f"Error in {context}: {error_message}")
        else:
            logger.error(error_message)
    
    @staticmethod
    def send_error_email(error, context=None, recipients=None):
        """
        Send an email notification about an error
        
        Args:
            error: The exception object
            context: Additional context about where the error occurred
            recipients: List of email recipients
        """
        if not EMAIL_CONFIG.get("enabled", False):
            logger.info("Email notifications are disabled")
            return
        
        if recipients is None:
            recipients = EMAIL_CONFIG.get("default_recipients", [])
        
        if not recipients:
            logger.warning("No recipients specified for error email")
            return
        
        try:
            # Format the error message
            error_message = ErrorHandler.format_error(error)
            
            # Create the email
            msg = MIMEMultipart()
            msg["From"] = EMAIL_CONFIG.get("sender", "pharmacy-etl@example.com")
            msg["To"] = ", ".join(recipients)
            
            if context:
                msg["Subject"] = f"Pharmacy ETL Error: {context}"
                body = f"An error occurred in {context}:\n\n{error_message}"
            else:
                msg["Subject"] = "Pharmacy ETL Error"
                body = f"An error occurred:\n\n{error_message}"
            
            msg.attach(MIMEText(body, "plain"))
            
            # Send the email
            with smtplib.SMTP(EMAIL_CONFIG.get("smtp_server"), EMAIL_CONFIG.get("smtp_port", 587)) as server:
                if EMAIL_CONFIG.get("use_tls", True):
                    server.starttls()
                
                if EMAIL_CONFIG.get("username") and EMAIL_CONFIG.get("password"):
                    server.login(EMAIL_CONFIG.get("username"), EMAIL_CONFIG.get("password"))
                
                server.send_message(msg)
            
            logger.info(f"Sent error email to {', '.join(recipients)}")
            
        except Exception as e:
            logger.error(f"Failed to send error email: {str(e)}")
    
    @staticmethod
    def handle_error(error, context=None, send_email=True, recipients=None, raise_error=True):
        """
        Handle an error: log it, send email notification, and optionally re-raise
        
        Args:
            error: The exception object
            context: Additional context about where the error occurred
            send_email: Whether to send an email notification
            recipients: List of email recipients
            raise_error: Whether to re-raise the error after handling
        """
        # Log the error
        ErrorHandler.log_error(error, context)
        
        # Send email notification if requested
        if send_email:
            ErrorHandler.send_error_email(error, context, recipients)
        
        # Re-raise the error if requested
        if raise_error:
            raise error
