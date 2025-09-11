# Colored Logging Configuration

This document explains how to configure and use colored logging in the mkts-backend project.

## Overview

The logging system has been enhanced to support colored output in the console while maintaining clean, uncolored output in log files. This makes it easier to quickly identify different log levels when running the application in a terminal.

## Features

- **Colored console output**: Different log levels are displayed in different colors
- **Clean file output**: Log files contain no color codes for better readability
- **Customizable colors**: You can define your own color scheme
- **Configurable log levels**: Different levels for console and file output
- **Automatic fallback**: Falls back to plain text if colors are not available

## Basic Usage

### Default Configuration

```python
from config.logging_config import configure_logging

# Basic usage with default colors
logger = configure_logging("my_module")
logger.info("This will appear in green")
logger.warning("This will appear in yellow")
logger.error("This will appear in red")
```

### Advanced Configuration

```python
import logging
from config.logging_config import configure_logging

# Custom configuration
logger = configure_logging(
    name="my_module",
    use_colors=True,                    # Enable/disable colors
    console_level=logging.INFO,         # Console log level
    file_level=logging.DEBUG,           # File log level
    custom_colors={                     # Custom color scheme
        'DEBUG': 'blue',
        'INFO': 'white',
        'WARNING': 'purple',
        'ERROR': 'red,bg_yellow',
        'CRITICAL': 'white,bg_red',
    }
)
```

## Available Colors

The following colors are supported by `colorlog`:

### Foreground Colors
- `black`
- `red`
- `green`
- `yellow`
- `blue`
- `purple`
- `cyan`
- `white`

### Background Colors
- `bg_black`
- `bg_red`
- `bg_green`
- `bg_yellow`
- `bg_blue`
- `bg_purple`
- `bg_cyan`
- `bg_white`

### Combining Colors
You can combine foreground and background colors using commas:
```python
'ERROR': 'red,bg_yellow'  # Red text on yellow background
'CRITICAL': 'white,bg_red'  # White text on red background
```

## Default Color Scheme

The default color scheme is:
- **DEBUG**: `cyan`
- **INFO**: `green`
- **WARNING**: `yellow`
- **ERROR**: `red`
- **CRITICAL**: `red,bg_white`

## Configuration Options

### `configure_logging()` Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | Required | Logger name |
| `use_colors` | bool | `True` | Enable colored output |
| `console_level` | int | `logging.INFO` | Minimum level for console output |
| `file_level` | int | `logging.INFO` | Minimum level for file output |
| `custom_colors` | dict | `None` | Custom color mapping |

### Log Levels

Available log levels (from lowest to highest priority):
- `logging.DEBUG` (10)
- `logging.INFO` (20)
- `logging.WARNING` (30)
- `logging.ERROR` (40)
- `logging.CRITICAL` (50)

## Examples

### Example 1: Basic Usage
```python
from config.logging_config import configure_logging

logger = configure_logging("market_data")
logger.info("Starting market data collection")
logger.warning("API rate limit approaching")
logger.error("Failed to fetch market data")
```

### Example 2: Custom Colors
```python
import logging
from config.logging_config import configure_logging

# Custom color scheme for better visibility
custom_colors = {
    'DEBUG': 'blue',
    'INFO': 'green',
    'WARNING': 'yellow',
    'ERROR': 'red,bg_white',
    'CRITICAL': 'white,bg_red',
}

logger = configure_logging(
    "critical_system",
    custom_colors=custom_colors,
    console_level=logging.WARNING  # Only show warnings and above
)
```

### Example 3: Different Console and File Levels
```python
import logging
from config.logging_config import configure_logging

# Verbose console output, detailed file logging
logger = configure_logging(
    "debug_module",
    console_level=logging.INFO,    # Show INFO+ in console
    file_level=logging.DEBUG       # Log everything to file
)
```

## File Output

Log files are written to `logs/mkts-backend.log` with the following characteristics:
- **No color codes**: Clean, readable text
- **Rotating**: Automatically rotates when file size exceeds 1MB
- **Backup**: Keeps 5 backup files
- **Format**: `timestamp|logger_name|level|function:line > message`

## Environment Considerations

### Terminal Support
Colors are automatically disabled if:
- The terminal doesn't support colors
- Output is redirected to a file
- `use_colors=False` is specified

### CI/CD Environments
In automated environments (like GitHub Actions), colors are automatically disabled to prevent issues with log parsing.

## Troubleshooting

### Colors Not Appearing
1. Check if you're running in a terminal that supports colors
2. Verify that `colorlog` is installed: `pip install colorlog`
3. Ensure `use_colors=True` (default)

### Invalid Color Errors
If you see `KeyError` for colors, check that you're using valid color names from the list above.

### Performance Impact
Colored logging has minimal performance impact. The color codes are only applied to console output, not file output.

## Migration from Old Configuration

The new configuration is backward compatible. Existing code will continue to work, but you can now take advantage of the new features:

```python
# Old way (still works)
logger = configure_logging("my_module")

# New way with colors
logger = configure_logging("my_module", use_colors=True)
```