-- Trading Bot Analytics Database Schema
-- SQLite database schema for tracking market making strategy performance

-- Strategy runs table: Track each strategy execution session
CREATE TABLE IF NOT EXISTS strategy_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name TEXT NOT NULL,
    market_ticker TEXT NOT NULL,
    trade_side TEXT NOT NULL CHECK(trade_side IN ('yes', 'no')),
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    config_params TEXT,  -- JSON string storing gamma, k, sigma, T, max_position, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Market snapshots: Periodic market state snapshots
CREATE TABLE IF NOT EXISTS market_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    yes_bid REAL,
    yes_ask REAL,
    yes_mid REAL,
    no_bid REAL,
    no_ask REAL,
    no_mid REAL,
    current_position INTEGER NOT NULL,
    reservation_price REAL,
    computed_bid REAL,
    computed_ask REAL,
    spread REAL,  -- computed_ask - computed_bid
    regime TEXT,  -- Current market regime (UNKNOWN, QUIET, MEAN_REVERTING, TRENDING, CHAOTIC, NOISE)
    regime_duration_seconds REAL,  -- Time spent in current regime
    prev_regime TEXT,  -- Previous regime before transition
    regime_transition BOOLEAN DEFAULT 0,  -- Whether this snapshot represents a regime transition
    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
);

-- Orders: All order events (placed, filled, cancelled)
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,  -- UUID from Kalshi
    run_id INTEGER NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('buy', 'sell')),
    side TEXT NOT NULL CHECK(side IN ('yes', 'no')),
    placed_price REAL NOT NULL,
    placed_quantity INTEGER NOT NULL,
    placed_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'placed' CHECK(status IN ('placed', 'filled', 'partially_filled', 'cancelled', 'expired')),
    filled_price REAL,
    filled_quantity INTEGER,
    filled_timestamp TIMESTAMP,
    cancelled_timestamp TIMESTAMP,
    expiration_ts TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
);

-- Trades: Completed trades (derived from order fills)
CREATE TABLE IF NOT EXISTS trades (
    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('buy', 'sell')),
    side TEXT NOT NULL CHECK(side IN ('yes', 'no')),
    price REAL NOT NULL,
    quantity INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    realized_pnl REAL,  -- calculated when position closed
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
);

-- Position history: Position changes over time
CREATE TABLE IF NOT EXISTS position_history (
    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    position INTEGER NOT NULL,
    unrealized_pnl REAL,  -- based on current market price
    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
);

-- Discovery sessions: Track each market discovery run
CREATE TABLE IF NOT EXISTS discovery_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    total_markets INTEGER NOT NULL,
    binary_markets INTEGER NOT NULL,
    pre_filtered INTEGER NOT NULL,
    scored INTEGER NOT NULL,
    top_markets_count INTEGER NOT NULL,
    configs_generated INTEGER NOT NULL,
    model_version TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Discovered markets: Track individual market discoveries
CREATE TABLE IF NOT EXISTS discovered_markets (
    discovery_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    ticker TEXT NOT NULL,
    title TEXT,
    score REAL,
    volume_24h REAL,
    spread REAL,
    mid_price REAL,
    liquidity REAL,
    open_interest INTEGER,
    reasons TEXT,  -- JSON array of scoring reasons
    generated_config TEXT,  -- JSON of generated config
    selected_for_trading BOOLEAN NOT NULL DEFAULT 0,
    trade_side TEXT,  -- 'yes', 'no', or NULL if not selected
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES discovery_sessions(session_id) ON DELETE CASCADE
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_market_snapshots_run_id ON market_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_timestamp ON market_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_orders_run_id ON orders(run_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_placed_timestamp ON orders(placed_timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_run_id ON trades(run_id);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_position_history_run_id ON position_history(run_id);
CREATE INDEX IF NOT EXISTS idx_position_history_timestamp ON position_history(timestamp);
CREATE INDEX IF NOT EXISTS idx_strategy_runs_strategy_name ON strategy_runs(strategy_name);
CREATE INDEX IF NOT EXISTS idx_strategy_runs_start_time ON strategy_runs(start_time);
CREATE INDEX IF NOT EXISTS idx_discovery_sessions_timestamp ON discovery_sessions(timestamp);
CREATE INDEX IF NOT EXISTS idx_discovered_markets_session_id ON discovered_markets(session_id);
CREATE INDEX IF NOT EXISTS idx_discovered_markets_ticker ON discovered_markets(ticker);
CREATE INDEX IF NOT EXISTS idx_discovered_markets_selected ON discovered_markets(selected_for_trading);

-- Volatility events: Store detected volatility events
CREATE TABLE IF NOT EXISTS volatility_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_ticker TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    signal_strength REAL,
    sigma REAL,
    volume_multiplier REAL,
    volume_delta REAL,
    estimated_trades REAL,
    volume_velocity REAL,
    price_change REAL,
    jump_magnitude REAL,
    direction TEXT,
    close_time TIMESTAMP,
    regime TEXT,  -- Market regime when event was detected
    run_id INTEGER,  -- Link to strategy run if session was spawned
    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE SET NULL
);

-- Volatility sessions: Track ephemeral MM sessions
CREATE TABLE IF NOT EXISTS volatility_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id INTEGER,
    run_id INTEGER NOT NULL,
    start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    kill_reason TEXT,  -- expiration, illiquidity, volatility_collapse, ttl, etc.
    final_pnl REAL,
    FOREIGN KEY (event_id) REFERENCES volatility_events(event_id) ON DELETE SET NULL,
    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
);

-- Order fills: Individual fill events (supports partial fills)
CREATE TABLE IF NOT EXISTS order_fills (
    fill_id TEXT PRIMARY KEY,  -- Unique fill ID from Kalshi
    order_id TEXT NOT NULL,
    run_id INTEGER NOT NULL,
    ticker TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('yes', 'no')),
    action TEXT NOT NULL CHECK(action IN ('buy', 'sell')),
    count INTEGER NOT NULL,  -- Quantity filled in this event
    price_cents INTEGER NOT NULL,  -- Price in cents
    ts INTEGER NOT NULL,  -- Unix timestamp in seconds
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES strategy_runs(run_id) ON DELETE CASCADE
);

-- Indexes for volatility tables
CREATE INDEX IF NOT EXISTS idx_volatility_events_ticker ON volatility_events(market_ticker);
CREATE INDEX IF NOT EXISTS idx_volatility_events_timestamp ON volatility_events(timestamp);
CREATE INDEX IF NOT EXISTS idx_volatility_events_run_id ON volatility_events(run_id);
CREATE INDEX IF NOT EXISTS idx_volatility_sessions_event_id ON volatility_sessions(event_id);
CREATE INDEX IF NOT EXISTS idx_volatility_sessions_run_id ON volatility_sessions(run_id);
CREATE INDEX IF NOT EXISTS idx_volatility_sessions_start_time ON volatility_sessions(start_time);

-- Indexes for order_fills table
CREATE INDEX IF NOT EXISTS idx_order_fills_order_id ON order_fills(order_id);
CREATE INDEX IF NOT EXISTS idx_order_fills_run_id ON order_fills(run_id);
CREATE INDEX IF NOT EXISTS idx_order_fills_ts ON order_fills(ts);

