import logging
import sys
import os
from datetime import datetime

class CompanyFormatter(logging.Formatter):
    """
    Custom formatter to match the company's log format:
    [ Tue Jan 06 05:32:41 AM UTC 2026 ] : INFO : root : Message
    """
    def format(self, record):
        # Format timestamp to match requirements
        # Example: [ Tue Jan 06 05:32:41 AM UTC 2026 ]
        dt = datetime.fromtimestamp(record.created)
        timestamp = dt.strftime("%a %b %d %I:%M:%S %p UTC %Y")
        
        # Get context (either 'root' or worker name)
        context = getattr(record, 'context', 'root')
        
        # Construct the final message
        return f"[ {timestamp} ] : {record.levelname} : {context} : {record.getMessage()}"

def setup_logger(name="crawler", log_file=None, level=logging.INFO):
    """Sets up a logger with the company standard format."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Avoid duplicate handlers if setup_logger is called multiple times
    if logger.handlers:
        return logger

    # Child loggers (job, worker) should propagate to the root 'crawler' logger
    if name != "crawler":
        logger.propagate = True
        # Ensure 'crawler' is setup if it hasn't been
        setup_logger("crawler", log_file=log_file, level=level)
        return logger

    formatter = CompanyFormatter()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (optional)
    # Only the root 'crawler' logger gets a FileHandler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# Global logger instance
logger = setup_logger()
