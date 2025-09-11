import logging
import logging.handlers
from logging import StreamHandler
import sys
from typing import Optional, Dict

try:
    import colorlog
    COLOR_AVAILABLE = True
except ImportError:
    COLOR_AVAILABLE = False


def configure_logging(
    name: str, 
    use_colors: bool = True,
    console_level: int = logging.INFO,
    file_level: int = logging.INFO,
    custom_colors: Optional[Dict[str, str]] = None
):
    """
    Configure logging with optional colored output.
    
    Args:
        name: Logger name
        use_colors: Whether to use colored output in console
        console_level: Log level for console output
        file_level: Log level for file output
        custom_colors: Custom color mapping for log levels
    """
    logger = logging.getLogger(name)
    logger.setLevel(min(console_level, file_level))
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    # Default color scheme
    default_colors = {
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'red,bg_white',
    }
    
    # Use custom colors if provided
    log_colors = custom_colors if custom_colors else default_colors
    
    # File formatter (no colors for log files)
    file_formatter = logging.Formatter(
        "%(asctime)s|%(name)s|%(levelname)s|%(funcName)s:%(lineno)d > %(message)s"
    )
    
    # Console formatter (with colors if available)
    if COLOR_AVAILABLE and use_colors and sys.stdout.isatty():
        console_formatter = colorlog.ColoredFormatter(
            "%(log_color)s%(asctime)s|%(name)s|%(levelname)s|%(funcName)s:%(lineno)d > %(message)s",
            datefmt=None,
            reset=True,
            log_colors=log_colors,
            secondary_log_colors={},
            style='%'
        )
    else:
        console_formatter = file_formatter
    
    # Rotating file handler (no colors)
    rotating_handler = logging.handlers.RotatingFileHandler(
        "logs/mkts-backend.log", maxBytes=1048576, backupCount=5
    )
    rotating_handler.setFormatter(file_formatter)
    rotating_handler.setLevel(file_level)
    logger.addHandler(rotating_handler)
    
    # Stream handler (with colors for console)
    stream_handler = StreamHandler()
    stream_handler.setFormatter(console_formatter)
    stream_handler.setLevel(console_level)
    logger.addHandler(stream_handler)

    return logger
