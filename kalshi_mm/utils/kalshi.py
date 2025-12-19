"""
Shared utilities for Kalshi API operations
Moved from mm.py to prevent circular imports
"""
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from functools import lru_cache


def parse_kalshi_timestamp(ts_raw) -> Optional[datetime]:
    """Parse Kalshi timestamp (ISO string or Unix seconds/ms)"""
    if ts_raw is None:
        return None
    if isinstance(ts_raw, str):
        # Check if it's a numeric string (Unix timestamp)
        if ts_raw.isdigit() or (ts_raw.startswith('-') and ts_raw[1:].isdigit()):
            # Numeric string - treat as Unix timestamp
            ts_raw_num = float(ts_raw)
            if ts_raw_num > 1e10:  # Milliseconds
                ts_raw_num = ts_raw_num / 1000
            return datetime.utcfromtimestamp(ts_raw_num).replace(tzinfo=timezone.utc)
        # Handle ISO8601: "2023-11-06T01:44:08.930114Z"
        ts_str = ts_raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(ts_str)
        except ValueError:
            return None
    elif isinstance(ts_raw, (int, float)):
        # Handle Unix timestamp (seconds or milliseconds)
        if ts_raw > 1e10:  # Milliseconds
            ts_raw = ts_raw / 1000
        return datetime.utcfromtimestamp(ts_raw).replace(tzinfo=timezone.utc)
    return None


@lru_cache(maxsize=2048)
def parse_kalshi_timestamp_cached(ts_raw_str: str):
    """Cached version for high-frequency parsing (fills, scanner)
    Cache stringified timestamps for consistent hashing"""
    return parse_kalshi_timestamp(ts_raw_str)


def get_price_field(market_dict: Dict[str, Any], key: str) -> Optional[float]:
    """Get price field handling all Kalshi variations (*_dollars, *_dollars_str, cents)
    
    CRITICAL FIX 2.2: Treat ALL numeric fields as strings - Kalshi WS sends strings
    """
    # Try *_dollars first
    if key + "_dollars" in market_dict:
        val = market_dict[key + "_dollars"]
        return float(val) if val is not None else None
    # Try *_dollars_str (some endpoints use string)
    if key + "_dollars_str" in market_dict:
        val = market_dict[key + "_dollars_str"]
        return float(val) if val is not None else None
    # Fallback to cents conversion (handle both int and string)
    if key in market_dict:
        val = market_dict[key]
        if val is None:
            return None
        # CRITICAL FIX 2.2: Handle string or numeric
        if isinstance(val, str):
            return float(val) / 100.0
        return float(val) / 100.0
    return None

