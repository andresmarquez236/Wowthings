
import logging
import sys
import os
from datetime import datetime

# Define custom levels if needed, or use standard
# We want clean, professional output.

class ContextFilter(logging.Filter):
    """
    This is a filter which injects contextual information into the log.
    """
    def __init__(self):
        super().__init__()
        self.step = "General"
        self.module_name = "System"

    def filter(self, record):
        record.step = self.step
        record.module_name = self.module_name
        return True

_context_filter = ContextFilter()

def setup_logger(module_name: str, log_file: str = None) -> logging.Logger:
    """
    Sets up a logger with a professional "PhD-level" format.
    Removes emojis and uses strict structure.
    
    Format: [YYYY-MM-DD HH:MM:SS] [LEVEL] [MODULE] [STEP] Message
    """
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formatter
    # We include 'step' which is a dynamic field managed by update_context
    formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [%(module_name)s] [%(step)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Console Handler (Stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler (Optional)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Attach context filter
    _context_filter.module_name = module_name
    logger.addFilter(_context_filter)
    
    return logger

def update_context(step: str = None, module_name: str = None):
    """
    Updates the context for the logger.
    Call this when moving to a new significant step in the process.
    """
    if step:
        _context_filter.step = step
    if module_name:
        _context_filter.module_name = module_name

def log_section(logger: logging.Logger, title: str):
    """
    Logs a section divider professionally.
    """
    logger.info("-" * 60)
    logger.info(f"STARTING PHASE: {title.upper()}")
    logger.info("-" * 60)
