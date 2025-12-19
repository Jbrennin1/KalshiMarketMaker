"""
Data models for volatility-driven market making system
"""
from dataclasses import dataclass, field
from typing import Optional, List, Deque
from datetime import datetime
from collections import deque


@dataclass
class MarketState:
    """Rolling state for a single market"""
    ticker: str
    mid_prices: List[float]  # Rolling window of mid prices
    volume_deltas: List[float]  # Rolling window of volume_24h deltas
    spreads: List[float]  # Rolling window of spreads
    liquidity_values: List[float]  # Rolling window of liquidity
    timestamps: List[datetime]  # Timestamps for each data point
    time_to_expiry: Optional[float]  # Days to expiry
    close_time: Optional[datetime]  # Market close time
    last_activity_time: datetime  # Last time we fetched this market
    
    # Regime detection fields (using fixed-length ring buffers to prevent unbounded growth)
    returns: Deque[float] = field(default_factory=lambda: deque(maxlen=100))  # Fixed-length ring buffer
    jump_count: int = 0  # Number of jumps > threshold in window
    mean_reversion_score: float = 0.0  # Fraction of moves that reverted (0-1)
    order_flow_imbalance: float = 0.0  # Approximate buy/sell imbalance
    avg_spread: float = 0.0  # Average spread over window
    max_spread: float = 0.0  # Maximum spread over window
    avg_depth: float = 0.0  # Average book depth/liquidity
    regime: Optional[str] = None  # Current regime classification
    drift_score: float = 0.0  # Cumulative return over window (normalized by volatility)
    regime_history: Deque[str] = field(default_factory=lambda: deque(maxlen=10))  # Last N regime classifications for stability check
    regime_timestamp: Optional[datetime] = None  # When regime was last updated
    
    def __post_init__(self):
        """Initialize empty lists if None"""
        if self.mid_prices is None:
            self.mid_prices = []
        if self.volume_deltas is None:
            self.volume_deltas = []
        if self.spreads is None:
            self.spreads = []
        if self.liquidity_values is None:
            self.liquidity_values = []
        if self.timestamps is None:
            self.timestamps = []
    
    def add_snapshot(self, mid_price: float, spread: float, 
                     liquidity: float, timestamp: datetime):
        """Add a new snapshot to the rolling state"""
        self.mid_prices.append(mid_price)
        # volume_deltas list kept for backward compatibility but not populated
        # (volume_24h is stale, using absolute threshold instead)
        self.spreads.append(spread)
        self.liquidity_values.append(liquidity)
        self.timestamps.append(timestamp)
        self.last_activity_time = timestamp
    
    def trim_to_window(self, window_minutes: int):
        """Trim data to only keep last N minutes"""
        cutoff_time = datetime.now()
        cutoff_time = cutoff_time.replace(microsecond=0)
        cutoff_time = cutoff_time.timestamp() - (window_minutes * 60)
        
        # Keep only data within window
        valid_indices = [
            i for i, ts in enumerate(self.timestamps)
            if ts.timestamp() >= cutoff_time
        ]
        
        if valid_indices:
            self.mid_prices = [self.mid_prices[i] for i in valid_indices]
            # volume_deltas is kept for backward compatibility but not populated
            # Only trim if it has data (should be empty in normal operation)
            if len(self.volume_deltas) > 0:
                self.volume_deltas = [self.volume_deltas[i] for i in valid_indices if i < len(self.volume_deltas)]
            self.spreads = [self.spreads[i] for i in valid_indices]
            self.liquidity_values = [self.liquidity_values[i] for i in valid_indices]
            self.timestamps = [self.timestamps[i] for i in valid_indices]
        else:
            # If no data in window, keep at least the most recent
            if self.mid_prices:
                self.mid_prices = [self.mid_prices[-1]]
                # volume_deltas is kept for backward compatibility but not populated
                if len(self.volume_deltas) > 0:
                    self.volume_deltas = [self.volume_deltas[-1]]
                self.spreads = [self.spreads[-1]]
                self.liquidity_values = [self.liquidity_values[-1]]
                self.timestamps = [self.timestamps[-1]]


@dataclass
class VolatilityEvent:
    """Event emitted when volatility is detected"""
    ticker: str
    timestamp: datetime
    jump_magnitude: Optional[float]  # Price jump in cents
    sigma: float  # Realized volatility
    volume_multiplier: float  # DEPRECATED: volume_24h is stale (updated next day), always 0.0
    volume_delta: float  # DEPRECATED: volume_24h is stale (updated next day), always 0.0
    estimated_trades: float  # DEPRECATED: derived from volume_delta, always 0.0
    volume_velocity: Optional[float]  # DEPRECATED: volume_24h is stale, disabled
    close_time: Optional[datetime]  # Market close time
    direction: Optional[str]  # Optional: 'up' or 'down' based on price trend
    signal_strength: float  # Combined signal strength (0-1)
    regime: str  # Current market regime (UNKNOWN, QUIET, MEAN_REVERTING, TRENDING, CHAOTIC, NOISE)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for logging"""
        return {
            'ticker': self.ticker,
            'timestamp': self.timestamp.isoformat(),
            'jump_magnitude': self.jump_magnitude,
            'sigma': self.sigma,
            'volume_multiplier': self.volume_multiplier,
            'volume_delta': self.volume_delta,
            'estimated_trades': self.estimated_trades,
            'volume_velocity': self.volume_velocity,
            'close_time': self.close_time.isoformat() if self.close_time else None,
            'direction': self.direction,
            'signal_strength': self.signal_strength,
            'regime': self.regime
        }


@dataclass
class VolatilityEndedEvent:
    """Event emitted when volatility drops below threshold"""
    ticker: str
    timestamp: datetime
    reason: str  # Why volatility ended (e.g., "volatility_collapse", "expiration", etc.)

