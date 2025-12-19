"""
Centralized logging configuration for Kalshi Market Maker.

Provides setup_logging() function that configures all loggers with file handlers,
log rotation, and configurable log levels from config.yaml.
"""
import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional


def setup_logging(config: Optional[Dict] = None) -> Dict[str, logging.Logger]:
    """
    Set up centralized logging configuration.
    
    Args:
        config: Configuration dictionary (from config.yaml). If None, uses defaults.
    
    Returns:
        Dictionary of configured loggers by component name.
    """
    # Get logging config with defaults
    log_config = (config or {}).get('logging', {})
    log_dir = log_config.get('log_dir', 'logs')
    main_log_config = log_config.get('main_log', {})
    websocket_log_config = log_config.get('websocket_log', {})
    console_config = log_config.get('console', {})
    component_levels = log_config.get('levels', {})
    
    # Create logs directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Standard log format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Get today's date for log file names
    today = datetime.now().strftime('%Y%m%d')
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # Set to lowest level, handlers will filter
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Main application log file handler
    if main_log_config.get('enabled', True):
        main_log_level = getattr(logging, main_log_config.get('level', 'INFO').upper())
        main_log_file = log_path / f"kalshi_mm_{today}.log"
        
        rotation_config = main_log_config.get('rotation', {})
        max_bytes = rotation_config.get('max_bytes', 10485760)  # 10MB default
        backup_count = rotation_config.get('backup_count', 5)
        
        main_file_handler = logging.handlers.RotatingFileHandler(
            main_log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        main_file_handler.setLevel(main_log_level)
        main_file_handler.setFormatter(formatter)
        root_logger.addHandler(main_file_handler)
    
    # WebSocket detailed log file handler (optional, for debugging)
    if websocket_log_config.get('enabled', False):
        ws_log_level = getattr(logging, websocket_log_config.get('level', 'DEBUG').upper())
        ws_log_file = log_path / f"websocket_{today}.log"
        
        rotation_config = websocket_log_config.get('rotation', {})
        max_bytes = rotation_config.get('max_bytes', 10485760)  # 10MB default
        backup_count = rotation_config.get('backup_count', 3)
        
        ws_file_handler = logging.handlers.RotatingFileHandler(
            ws_log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        ws_file_handler.setLevel(ws_log_level)
        ws_file_handler.setFormatter(formatter)
        
        # Create separate logger for WebSocket that only logs to this file
        ws_logger = logging.getLogger('KalshiWebsocketClient')
        ws_logger.addHandler(ws_file_handler)
        ws_logger.setLevel(ws_log_level)
        ws_logger.propagate = False  # Don't propagate to root logger
    
    # Console handler
    if console_config.get('enabled', True):
        console_level = getattr(logging, console_config.get('level', 'INFO').upper())
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    # Configure component-specific log levels
    loggers = {}
    component_names = [
        'Main',
        'VolatilityScanner',
        'VolatilityMMManager',
        'MarketDiscovery',
        'KalshiWebsocketClient',
        'MarketStateStore',
    ]
    
    for component_name in component_names:
        logger = logging.getLogger(component_name)
        # Set level from config, or default to INFO
        level_name = component_levels.get(component_name, 'INFO')
        logger.setLevel(getattr(logging, level_name.upper()))
        loggers[component_name] = logger
    
    # Also create Main logger (used by runner.py)
    if 'Main' not in loggers:
        loggers['Main'] = logging.getLogger('Main')
    
    return loggers


def get_log_file_paths(log_dir: str = 'logs') -> Dict[str, str]:
    """
    Get paths to current log files for monitoring.
    
    Args:
        log_dir: Log directory path
    
    Returns:
        Dictionary mapping log type to file path
    """
    log_path = Path(log_dir)
    today = datetime.now().strftime('%Y%m%d')
    
    return {
        'main': str(log_path / f"kalshi_mm_{today}.log"),
        'websocket': str(log_path / f"websocket_{today}.log"),
        'sweep_details': str(log_path / f"volatility_sweep_details_{today}.log"),
        'pipeline': str(log_path / f"filtering_pipeline_{today}.log"),
    }

