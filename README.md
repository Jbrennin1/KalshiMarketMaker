# Kalshi Market Making Bot

A sophisticated algorithmic trading system for market making on Kalshi, implementing the Avellaneda-Stoikov optimal market making model with advanced volatility detection, dynamic risk management, and comprehensive analytics.

## Table of Contents

1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
4. [Operational Modes](#operational-modes)
5. [Risk Management](#risk-management)
6. [Advanced Features](#advanced-features)
7. [Configuration](#configuration)
8. [Database Schema](#database-schema)
9. [Setup & Installation](#setup--installation)
10. [Usage](#usage)
11. [Deployment](#deployment)
12. [Monitoring & Analytics](#monitoring--analytics)

## Introduction

This bot implements a professional-grade market making system for Kalshi prediction markets. It operates in two distinct modes:

1. **Discovery-Based Mode**: Discovers and scores markets, then runs market making strategies on the top candidates
2. **Volatility-Driven Mode**: Continuously scans markets for volatility spikes and spawns short-lived market making sessions

### Key Features

- **Avellaneda-Stoikov Algorithm**: Optimal market making with inventory skewing and dynamic spread adjustment
- **Intelligent Market Discovery**: Multi-stage filtering and scoring system to identify profitable opportunities
- **Volatility Detection**: Real-time monitoring for price jumps and volatility spikes
- **Dynamic Risk Management**: Position sizing based on liquidity, category caps, and extreme price handling
- **Comprehensive Analytics**: SQLite database tracking all orders, fills, positions, and market snapshots
- **Advanced Throttling**: Spread widening, book health monitoring, and adverse selection detection
- **RSA-PSS Authentication**: Secure API authentication using Kalshi's API key system
- **Real-Time WebSocket Updates**: Low-latency market data via WebSocket with automatic fallback to REST

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      Runner (Entry Point)                    │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                                 │
┌───────▼────────┐            ┌──────────▼──────────┐
│  Discovery     │            │  Volatility Scanner  │
│  System        │            │  (Continuous Loop)   │
└───────┬────────┘            └──────────┬───────────┘
        │                                 │
        │                                 │
┌───────▼────────┐            ┌──────────▼───────────┐
│ Market Scorer  │            │ Volatility MM Manager │
│ (Normalized    │            │ (Session Spawner)     │
│  Features)     │            └──────────┬───────────┘
└───────┬────────┘                        │
        │                                 │
        └───────────────┬─────────────────┘
                        │
            ┌───────────▼───────────┐
            │  Dynamic Config       │
            │  Generator            │
            └───────────┬───────────┘
                        │
            ┌───────────▼───────────┐
            │  AvellanedaMarketMaker │
            │  (AS Algorithm)        │
            └───────────┬───────────┘
                        │
            ┌───────────▼───────────┐
            │  KalshiTradingAPI     │
            │  (RSA-PSS Auth)       │
            └───────────┬───────────┘
                        │
        ┌───────────────┴───────────────┐
        │                                 │
┌───────▼────────┐            ┌──────────▼──────────┐
│ WebSocket      │            │  AnalyticsDB        │
│ Client         │            │  (SQLite)          │
│ (Real-time)    │            └────────────────────┘
└───────┬────────┘
        │
┌───────▼────────┐
│ Market State   │
│ Store          │
│ (In-memory)    │
└────────────────┘
```

### Core Components

1. **Trading API Layer** (`kalshi_mm/api/trading.py` - `KalshiTradingAPI`): Handles all Kalshi API interactions
2. **Market Making Engine** (`kalshi_mm/api/trading.py` - `AvellanedaMarketMaker`): Implements the AS algorithm
3. **WebSocket Client** (`kalshi_mm/api/websocket.py` - `KalshiWebsocketClient`): Real-time market data via WebSocket
4. **Market State Store** (`kalshi_mm/state/store.py` - `MarketStateStore`): Shared in-memory store for WebSocket data
5. **Market Discovery** (`kalshi_mm/discovery/discovery.py`): Fetches and filters markets
6. **Market Scoring** (`kalshi_mm/discovery/scorer.py`): Scores markets using normalized features
7. **Volatility Scanner** (`kalshi_mm/volatility/scanner.py`): Monitors markets for volatility events
8. **Volatility MM Manager** (`kalshi_mm/volatility/manager.py`): Manages ephemeral MM sessions
9. **Dynamic Configuration** (`kalshi_mm/config/dynamic.py`): Generates market-specific configs
10. **Analytics System** (`kalshi_mm/analytics/db.py`): Database operations and logging

## Core Components

### 3.1 Trading API (`kalshi_mm/api/trading.py`)

The `KalshiTradingAPI` class provides a complete interface to the Kalshi trading API.

#### Authentication

Uses RSA-PSS with SHA256 signing for API key authentication:

```python
# Headers include:
# - KALSHI-ACCESS-KEY: Your API access key
# - KALSHI-ACCESS-TIMESTAMP: Unix timestamp in milliseconds
# - KALSHI-ACCESS-SIGNATURE: Base64-encoded RSA-PSS signature
```

The signature is computed over: `timestamp + method + path` (body is NOT included per Kalshi API spec).

#### Key Methods

- `get_price()`: Retrieves current market prices (bid/ask/mid for both yes/no sides)
- `place_order(action, side, price, quantity, expiration_ts)`: Places limit orders
- `cancel_order(order_id)`: Cancels existing orders
- `get_position()`: Gets current position for the market
- `get_orders()`: Retrieves all resting orders
- `get_fills()`: Fetches fill history with pagination support
- `iter_fills_since(min_ts)`: Iterator for all fills since a timestamp

#### Fill Reconciliation

The API provides `iter_fills_since()` which is used by the market maker to reconcile fills from Kalshi's API (single source of truth). This ensures no fills are missed even if order status updates are delayed.

### 3.2 Market Making Algorithm (`kalshi_mm/api/trading.py`)

The `AvellanedaMarketMaker` class implements the Avellaneda-Stoikov optimal market making model.

#### Avellaneda-Stoikov Model

The AS model provides optimal bid/ask quotes that balance:
- **Profit from spread**: Wider spreads = more profit per trade
- **Inventory risk**: Holding inventory exposes you to price movements
- **Order flow intensity**: Higher k = more orders expected

#### Key Calculations

**Reservation Price** (target price adjusted for inventory):
```
r = S - q * γ * σ² * (T - t)
```
Where:
- `S`: Mid market price
- `q`: Current inventory
- `γ`: Risk aversion parameter (dynamic, decreases with inventory)
- `σ`: Volatility estimate
- `T - t`: Time to expiry

**Optimal Spread**:
```
δ* ≈ (γσ²(T-t))/2 + (1/γ)*ln(1+γ/k)
```
The model uses a shorter risk horizon (default 1 hour) instead of full time-to-expiry for more responsive spreads.

**Asymmetric Quotes**:
- Bid/ask are placed around the reservation price
- Spread is widened on the side where inventory is building
- Minimum spread enforced to ensure profitability
- Maximum spread cap prevents quoting too wide

#### Dynamic Features

1. **Dynamic Gamma**: Risk aversion decreases exponentially as inventory approaches limits
   ```python
   γ(q) = γ_base * exp(-|q|/max_position)
   ```

2. **Inventory Skewing**: Quotes are skewed away from inventory to encourage mean reversion
   - Long inventory → lower bid, higher ask (discourage buys, encourage sells)
   - Short inventory → higher bid, lower ask (encourage buys, discourage sells)

3. **Order Sizing**: Order sizes adjust based on remaining capacity
   - When long: smaller buy orders, full-size sell orders
   - When short: full-size buy orders, smaller sell orders

#### Order Management

The market maker maintains one bid and one ask order at all times:

1. **Price Clamping**: Quotes are clamped to stay within NBBO (National Best Bid/Offer)
   - Never cross the book
   - Attempts to improve by at least one tick when possible

2. **Order Replacement**: 
   - Keeps existing orders if price/size match desired values
   - Cancels and replaces if price drifts > 1 cent or size changes

3. **One-Sided Quoting**: For hard extreme prices (< 5¢ or > 95¢):
   - Only quotes on the safe side (bids for cheap, asks for expensive)
   - Prevents getting trapped in extreme positions

#### Main Loop

```python
while time_elapsed < T:
    1. Get current market prices
    2. Get current inventory
    3. Calculate reservation price
    4. Calculate optimal bid/ask quotes
    5. Calculate order sizes
    6. Manage orders (place/cancel/replace)
    7. Reconcile fills from Kalshi API
    8. Log market snapshot to analytics
    9. Sleep for dt seconds
```

### 3.3 Market Discovery (`kalshi_mm/discovery/discovery.py`)

The `MarketDiscovery` class handles fetching and filtering markets from Kalshi.

#### Market Fetching

- `get_all_open_markets(limit=1000)`: Fetches all open markets with pagination
- Filters to binary markets only (excludes scalar/combo markets)

#### Pre-Filtering

Markets are filtered through multiple criteria:

1. **Status**: Must be 'active' or 'open'
2. **Market Age**: Must be at least 60 seconds old (avoids newborn markets with garbage books)
3. **Time-to-Expiry**: Category-specific windows:
   - Default: 1-60 days
   - Sports: 1 hour - 7 days
   - Rates: 1-90 days
   - Crypto: 1-90 days
4. **Category Exclusion**: Skips chronically illiquid categories (e.g., "long-tail-politics")
5. **Recent Activity**: 
   - Scoring mode: Allows if 24h volume > $10k OR recent trades
   - Execution mode: Requires recent trades (configurable threshold)
6. **Extreme Prices**: Skips soft/hard extreme markets in discovery (handled separately in volatility mode)

#### Market Metrics

Calculates various metrics for scoring:

- **Spread**: `min(yes_spread, no_spread)` in dollars
- **Mid Price**: `(bid + ask) / 2` for yes/no sides
- **Book Depth**: Sum of liquidity at best bid/ask (proxy from `liquidity` field)
- **Time-to-Expiry**: Days until market closes
- **Volume**: 24h volume in dollars

#### Sigma Estimation

Estimates volatility (σ) using a simple proxy:
```
σ = σ_baseline * (1 + 2 * p * (1 - p))
```
Where `p` is the mid price. This gives higher volatility around 0.5 (maximum uncertainty) and lower at extremes.

### 3.4 Market Scoring (`kalshi_mm/discovery/scorer.py`)

The `MarketScorer` class scores markets using normalized features to identify the most profitable opportunities.

#### Scoring Formula

Markets are scored using a weighted combination of normalized features:

```
score = w1 * norm_volume_24h 
      + w2 * norm_recent_volume_1h
      + w3 * norm_book_liquidity
      - w4 * norm_spread          (negative: tighter is better)
      - w5 * time_penalty         (negative: closer expiry is riskier)
```

Default weights (from `config.yaml`):
- `volume_24h`: 0.30
- `recent_volume_1h`: 0.25
- `book_liquidity`: 0.20
- `spread`: -0.15
- `time_penalty`: -0.10

#### Normalization

Features are normalized to [0, 1] using min-max scaling over all candidate markets:
```python
norm_value = (value - min(all_values)) / (max(all_values) - min(all_values))
```

This ensures scores are comparable across markets with different absolute values.

#### Time Penalty

Markets closer to expiry get penalized:
- < 1 day to expiry: penalty = 1.0 (maximum)
- > 60 days to expiry: penalty = 0.0 (minimum)
- Linear interpolation between 1-60 days

#### Volume Bands

Markets are labeled by volume level:
- Very low: normalized_volume < 0.25
- Low: 0.25 ≤ normalized_volume < 0.5
- Medium: 0.5 ≤ normalized_volume < 0.75
- High: normalized_volume ≥ 0.75

#### Selection

After scoring, markets are:
1. Filtered by minimum 24h volume threshold (default: $5,000)
2. Sorted by score (highest first)
3. Top N markets selected (default: 20)

### 3.5 Dynamic Configuration (`kalshi_mm/config/dynamic.py`)

The `generate_config_for_market()` function creates market-specific configurations based on market characteristics.

#### Risk Budget Calculation

Position sizing uses a sublinear relationship with liquidity:

```python
raw_exposure = alpha * sqrt(liquidity)
max_exposure = min(raw_exposure, per_market_cap, global_budget / n_active_markets, category_cap)
```

This ensures:
- More liquid markets → larger positions (but sublinear, not linear)
- Global risk budget is divided across active markets
- Category-level caps prevent over-concentration
- Per-market cap provides hard limit

#### Extreme Price Handling

Markets are classified into three bands:

1. **Normal**: 15¢ - 85¢
   - Full position sizing
   - Two-sided quoting

2. **Soft Extreme**: 5¢ - 15¢ or 85¢ - 95¢
   - Position size reduced by factor (default: 0.3x)
   - Two-sided quoting still enabled

3. **Hard Extreme**: < 5¢ or > 95¢
   - Position size heavily reduced (default: 0.1x)
   - One-sided quoting only (bids for cheap, asks for expensive)
   - OR: Skipped entirely in discovery mode

#### Sigma Blending (Dynamic)

For volatility-driven sessions, sigma is dynamically blended based on event type and category:

**Jump-Aware Logic**:
- If `jump_magnitude >= 10 cents`: `blend_weight = 0.0` (ignore event sigma)
  - Jump-driven sigma explosions should not shrink spreads
  - Only use event sigma for true volatility (sigma-based events)

**Category-Based Blending**:
- **Mean-reverting** (rates, economic): `blend_weight = 0.5`
- **Medium-trending** (crypto): `blend_weight = 0.25`
- **Low-liquidity** (entertainment, tech): `blend_weight = 0.1`

```python
if jump_magnitude >= 10 cents:
    blend_weight = 0.0  # Ignore event sigma on jumps
elif category_profile:
    blend_weight = category_profile.sigma_blend
else:
    blend_weight = 0.5  # Default fallback

blended_sigma = (1 - blend_weight) * estimated_sigma + blend_weight * event_sigma
final_sigma = clamp(blended_sigma, sigma_floor, sigma_cap)
```

Default: `sigma_cap = 0.08`, `sigma_floor = 0.001`

#### Category-Specific Parameters

Different market categories use different `k` values (order intensity):
- Default: 1.5
- Sports: 2.0 (higher order flow expected)
- Rates: 1.2 (lower order flow)
- Crypto: 1.8

#### Long-Dated Penalty

Markets expiring > 30 days get a position size haircut (default: 0.7x) to account for increased risk.

### 3.6 Volatility Scanner (`kalshi_mm/volatility/scanner.py`)

The `VolatilityScanner` continuously monitors markets for volatility spikes that indicate profitable market making opportunities.

#### Scanning Strategy

1. **Initial Full Scan**: 
   - Fetches all open markets
   - Filters to binary, active markets
   - Scores by activity (volume + liquidity + recency - spread)
   - Tracks top 200 markets

2. **Incremental Sweeps**:
   - Round-robin through tracked markets
   - Processes in chunks to target ~30s cycle time
   - Rate-limited to ~20 requests/second

3. **Periodic Rediscovery**:
   - Every 10 minutes, re-runs full discovery
   - Updates tracked set (adds new markets, removes inactive)
   - Maintains top 200 by activity score

#### Market State Tracking

Each tracked market maintains a `MarketState` with rolling windows:
- **Mid prices**: Last 15 minutes (rolling window)
- **Spreads**: Last 15 minutes
- **Liquidity**: Last 15 minutes
- **Timestamps**: For each data point

State is trimmed to the rolling window to keep memory bounded.

#### Volatility Event Detection

A volatility event is triggered when ALL of the following are true:

1. **Warmup**: At least 3 price points collected
2. **Category Filter**: Market category not in exclusion list (sports excluded by default)
3. **Spread Filter**: Current spread ≤ 10¢ (configurable)
4. **Volatility**: At least ONE of:
   - **Price Jump**: ≥ 12¢ move in last 5 minutes (configurable) **AND mean-reversion detected**
   - **Sigma**: Realized volatility ≥ 0.02 AND mean-reversion detected
5. **Liquidity**: 24h volume ≥ minimum threshold (default: $5,000)
6. **Not Already Active**: No existing session for this ticker

**Important**: Both price jumps AND sigma triggers now require mean-reversion. This prevents triggering on pure directional moves (injuries, touchdowns, etc.) which are toxic to market making.

#### Category Exclusion

Sports markets are excluded from volatility scanning by default because:
- Sports volatility is directional (news-driven), not mean-reverting
- Price jumps in sports are typically one-way moves (injuries, scoring events)
- Market making is fundamentally incompatible with directional sports volatility

To exclude additional categories, add them to `volatility_scanner.exclude_categories` in config.

#### Mean-Reversion Check

**All volatility triggers** (both jump and sigma) require mean-reversion:
- Calculates mean price over last 3 minutes
- Current price must be within tolerance (default: 6¢) of mean
- Filters out directional drift (bad for MM) vs. oscillating prices (good for MM)
- Prevents triggering on pure directional jumps which would cause losses

#### Signal Strength

Combined signal strength (0-1) is calculated as:
```python
jump_score = min(1.0, price_jump / (threshold * 2))
sigma_score = min(1.0, sigma / (threshold * 2))
signal_strength = (jump_score * 0.5 + sigma_score * 0.5)
```

#### Event Emission

When an event is detected, `VolatilityEvent` is emitted with:
- Ticker, timestamp
- Jump magnitude (cents) or None
- Sigma (realized volatility)
- Signal strength
- Direction (up/down) if price moved > 5¢
- Close time (for TTL calculation)

#### Volatility Ended Detection

The scanner monitors active events and emits `VolatilityEndedEvent` when:
- Volatility drops below threshold (sigma < threshold AND no price jump)
- Market becomes inactive
- Market is removed from tracking

### 3.7 Volatility MM Manager (`kalshi_mm/volatility/manager.py`)

The `VolatilityMMManager` spawns and manages short-lived market making sessions triggered by volatility events.

#### Session Lifecycle

1. **Event Received**: Volatility event detected by scanner
2. **Spawn Check**: 
   - Not already active for this ticker
   - Under max concurrent sessions limit (default: 3)
3. **Config Generation**: Creates volatility-specific config (see Dynamic Config)
4. **Session Start**: Submits to thread pool executor
5. **Session Run**: Market maker runs for TTL duration
6. **Session End**: Terminated by:
   - TTL expiration (default: 5 minutes)
   - Market close (stops 3 minutes before)
   - Volatility collapse (event ended)
   - Manager shutdown

#### Config Overrides

Volatility sessions use category-specific parameters:

- **Max Position**: Capped at 3 contracts (very conservative)
- **T (TTL)**: Calculated from market close time with buffer (default: 180s buffer)
- **dt**: Faster quote refresh (default: 3 seconds)
- **Spread Caps**: Category-specific from profiles:
  - Mean-reverting (rates, economic): tight=2¢, wide=4¢
  - Medium-trending (crypto): tight=3¢, wide=8¢
  - Low-liquidity (entertainment, tech): tight=4¢, wide=12¢
  - Hard-trending (sports): excluded by default
- **Sigma**: Dynamically blended with event sigma (see Dynamic Config)
  - Jump-driven events (≥10¢): ignore event sigma (blend_weight=0.0)
  - Category-based blend weights from profiles

#### Guardrails

Minimal guardrails for volatility sessions:

1. **Expiration Danger Window**: Stop quoting 3 minutes before market close
2. **Illiquidity Timeout**: If no trades for 15 seconds, freeze quoting
3. **Min Volatility Threshold**: If volatility drops below 0.01, kill session

#### Passive Exit Strategy

When TTL expires in passive exit mode (default for volatility sessions):

1. **Stop Quoting**: Immediately freeze all quotes
2. **Position Handling**:
   - If position is small (< 2 contracts): Close immediately via limit order
   - If position is large: Keep open, wait 15 seconds for mean reversion, then place passive limit orders at better prices
3. **Rationale**: Prevents crystallizing losses at worst prices. TTL exits were causing consistent losses by forcing exits at the moment volatility disappeared, dead-center in the direction of the move.

#### Concurrent Session Management

- Thread pool executor with max workers = max_concurrent_sessions
- Thread-safe session tracking with locks
- Automatic cleanup on session completion or termination

### 3.8 WebSocket Client (`websocket_client.py`)

The `KalshiWebsocketClient` provides real-time market data via WebSocket connections to the Kalshi API.

#### Architecture

- **Async/Sync Bridge**: Uses asyncio event loop in background thread with sync wrappers
- **Thread-Safe**: All operations protected by locks for concurrent access
- **State Store Integration**: Updates `MarketStateStore` with real-time data
- **Automatic Reconnection**: Exponential backoff with subscription re-establishment

#### Authentication

Uses the same RSA-PSS authentication as REST API:
- Signature computed over: `timestamp + "GET" + "/trade-api/ws/v2"`
- Headers: `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE`, `KALSHI-ACCESS-TIMESTAMP`
- Authenticated during WebSocket handshake

#### Supported Channels

The client supports all Kalshi WebSocket channels:

1. **Ticker** (`"ticker"`): Real-time price updates
   - Can subscribe globally or to specific markets
   - Format: `{"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_ticker": "..."}}`

2. **Orderbook** (`"orderbook_delta"`): Order book updates
   - Market-specific subscription required
   - Format: `{"id": 1, "cmd": "subscribe", "params": {"channels": ["orderbook_delta"], "market_ticker": "..."}}`

3. **Public Trades** (`"trade"`): Trade notifications
   - Market-specific subscription
   - Format: `{"id": 1, "cmd": "subscribe", "params": {"channels": ["trade"], "market_ticker": "..."}}`

4. **User Fills** (`"fill"`): Your order fill notifications (authenticated)
   - Global subscription (all your fills)
   - Format: `{"id": 1, "cmd": "subscribe", "params": {"channels": ["fill"]}}`

5. **Market Positions** (`"market_positions"`): Real-time position updates (authenticated)
   - Global subscription (all positions)
   - Format: `{"id": 1, "cmd": "subscribe", "params": {"channels": ["market_positions"]}}`

6. **Market Lifecycle** (`"market_lifecycle_v2"`): Market state changes and event creation
   - Global subscription (all lifecycle events)
   - Format: `{"id": 1, "cmd": "subscribe", "params": {"channels": ["market_lifecycle_v2"]}}`

**Important**: Channel names in subscription messages must match Kalshi API exactly:
- Use `"trade"` not `"trades"`
- Use `"fill"` not `"fills"`
- Use `"market_positions"` not `"positions"`
- Use `"market_lifecycle_v2"` not `"lifecycle"`

#### Message Processing

1. **Subscription Confirmation**: Server sends `{"type": "subscribed", "id": <msg_id>}`
2. **Data Messages**: Server sends `{"type": "<channel>", "data": {...}}` or `{"type": "<channel>", "msg": {...}}`
3. **Error Messages**: Server sends `{"type": "error", "id": <msg_id>, "msg": {"code": <code>, "msg": "..."}}`

The client maps API message types to internal channel names:
- `"trade"` → `"trades"` (internal)
- `"fill"` → `"fills"` (internal)
- `"market_positions"` → `"positions"` (internal)
- `"market_lifecycle_v2"` → `"lifecycle"` (internal)

#### Message Normalization

All messages are normalized to a consistent format:
- **Ticker**: Converts `bid`/`ask` (dollars) to `yes_bid`/`yes_ask` (cents), calculates mid price
- **Orderbook**: Converts Kalshi's `yes`/`no` arrays to standard `bids`/`asks` format
- **Trades**: Normalizes timestamp fields
- **Fills/Positions/Lifecycle**: Ensures `market_ticker` field is present

#### Reconnection

On connection loss:
1. Exponential backoff (configurable: `reconnect_base_delay_seconds`, `reconnect_max_delay_seconds`)
2. Re-establishes connection
3. Re-subscribes all active subscriptions automatically
4. Updates state store health flags

#### Integration Points

- **Volatility Scanner**: Subscribes to ticker, orderbook, and trades for tracked markets
- **Market Maker**: Uses WebSocket data via state store when available, falls back to REST
- **State Store**: Centralized in-memory cache with staleness detection

### 3.9 Market State Store (`kalshi_mm/state/store.py`)

The `MarketStateStore` provides a thread-safe, in-memory cache for WebSocket data.

#### Data Structures

- **Tickers**: Latest price data per market with staleness tracking
- **Orderbooks**: Normalized order book data per market
- **Trades**: Ring buffer of recent trades per market (configurable size)
- **User Fills**: Queue of user fill notifications (max 10,000)
- **Positions**: Latest position data per market
- **Lifecycle Events**: Latest lifecycle event per market
- **Price History**: Rolling window of mid prices and spreads for volatility detection

#### Staleness Detection

Each data type has configurable staleness thresholds:
- **Ticker**: Default 10 seconds
- **Orderbook**: Default 10 seconds
- **Positions**: Default 30 seconds

Stale data returns `None` to trigger REST fallback.

#### Thread Safety

All operations are protected by locks to ensure:
- No data corruption under concurrent access
- Consistent state during updates
- Safe access from multiple threads (scanner, MM, WebSocket client)

#### Health Monitoring

- `ws_healthy`: Boolean flag indicating WebSocket connection health
- `last_message_time`: Timestamp of last received message
- Used by components to decide REST fallback

### 3.10 Analytics System (`analytics.py`)

The `AnalyticsDB` class provides comprehensive logging and analytics via SQLite database.

#### Database Schema

See [Database Schema](#database-schema) section for full details.

#### Key Operations

1. **Strategy Run Tracking**:
   - `create_strategy_run()`: Creates new run with config params
   - `end_strategy_run()`: Marks run as completed

2. **Order & Fill Logging**:
   - `log_order_placed()`: Logs order placement
   - `log_order_cancelled()`: Marks order as cancelled
   - `log_fill_event()`: Logs individual fill (supports partial fills)
   - `log_order_filled()`: Updates order status and creates trade record

3. **Market Snapshots**:
   - `log_market_snapshot()`: Periodic state snapshots (prices, position, quotes)

4. **Position Tracking**:
   - `log_position()`: Logs position changes

5. **Discovery Logging**:
   - `create_discovery_session()`: Tracks discovery runs
   - `log_discovered_market()`: Logs each discovered market with score

6. **Volatility Logging**:
   - `log_volatility_event()`: Logs detected volatility events
   - `log_volatility_session_end()`: Tracks session completion

#### Fill Reconciliation

The system uses Kalshi's `/portfolio/fills` endpoint as the single source of truth:
- `reconcile_new_fills()` in market maker pulls fills since last check
- Deduplicates by `fill_id` to prevent double-counting
- Handles partial fills correctly
- Tracks fills for adverse selection detection

## Operational Modes

### 4.1 Discovery-Based Mode

**Status**: Currently not the default mode (volatility-driven is active)

The discovery-based mode would work as follows:

1. **Discovery Phase**:
   - Fetch all open markets
   - Filter to binary markets
   - Pre-filter by time-to-expiry, category, activity
   - Score markets using normalized features
   - Select top N markets (default: 20)

2. **Config Generation**:
   - For each selected market:
     - Calculate risk budget
     - Determine extreme price band
     - Estimate sigma
     - Generate market-specific config

3. **Parallel Execution**:
   - Spawn one market maker per market/side combination
   - All run in parallel using ThreadPoolExecutor
   - Each logs to separate log file

4. **Monitoring**:
   - Each strategy logs to `{strategy_name}.log`
   - All activity logged to analytics database

### 4.2 Volatility-Driven Mode (Current Default)

The bot currently runs in volatility-driven mode by default.

1. **Scanner Startup**:
   - Initial full scan: discovers top 200 markets
   - Starts incremental sweep loop (every 3 seconds)
   - Starts rediscovery thread (every 10 minutes)

2. **Continuous Monitoring**:
   - Scanner processes markets in chunks
   - Updates market state for each
   - Checks for volatility events

3. **Event Handling**:
   - When event detected → manager spawns session
   - Session runs for TTL (typically 5 minutes)
   - Session terminates when volatility ends or TTL expires

4. **Session Management**:
   - Max 3 concurrent sessions (configurable)
   - Each session is independent market maker
   - All activity logged to analytics database

## Risk Management

### Global Risk Budget

Total dollar exposure across all markets is capped:
- Default: $60 (leaves $40 buffer for $100 test account)
- Divided among active markets
- Enforced per-market: `max_exposure = min(calculated, global_budget / n_active)`

### Per-Market Caps

Maximum exposure per market:
- Default: $8 per market
- Very conservative for sports markets (can drift hard)
- Can be scaled up for more liquid markets

### Category-Level Caps

Prevents over-concentration in correlated markets:
- Crypto: $8
- Rates: $8
- Sports: $8
- Default: $8

All set to $8 for conservative testing.

### Category Profiles System

The bot uses a category profile system to apply different parameters based on market behavior:

**Profile Types**:
1. **Mean-Reverting** (rates, economic indicators):
   - Spread caps: tight=2¢, wide=4¢
   - Sigma blend: 0.5 (trust event sigma)
   - MM-friendly, oscillating prices

2. **Medium-Trending** (crypto):
   - Spread caps: tight=3¢, wide=8¢
   - Sigma blend: 0.25 (less trust in event sigma)
   - Sometimes directional, sometimes mean-reverting

3. **Hard-Trending** (sports):
   - Spread caps: tight=10¢, wide=20¢ (if re-enabled)
   - Sigma blend: 0.2
   - **Excluded by default** - directional, toxic to MM

4. **Low-Liquidity** (entertainment, tech events):
   - Spread caps: tight=4¢, wide=12¢
   - Sigma blend: 0.1 (very conservative)
   - Requires wider spreads due to low liquidity

Categories are mapped to profiles via `category_mapping` in config. This system ensures appropriate spread caps and sigma blending for each market type.

### Extreme Price Handling

Three price bands with different risk profiles:

1. **Normal (15¢ - 85¢)**:
   - Full position sizing
   - Two-sided quoting

2. **Soft Extreme (5¢ - 15¢ or 85¢ - 95¢)**:
   - Position size: 30% of normal
   - Two-sided quoting

3. **Hard Extreme (< 5¢ or > 95¢)**:
   - Position size: 10% of normal
   - One-sided quoting only
   - OR: Skipped in discovery mode

### Long-Dated Penalty

Markets expiring > 30 days get 30% position size haircut to account for:
- Increased uncertainty
- Longer exposure time
- Higher risk of adverse moves

### Position Limit Buffers

Order sizes are calculated with buffer to prevent hitting limits:
- When long: buy orders use buffer size, sell orders use full size
- When short: buy orders use full size, sell orders use buffer size
- Buffer: 5-10% of max_position (configurable)

## Advanced Features

### 6.1 Throttling System

The market maker includes a sophisticated throttling system to protect against adverse conditions.

#### Spread Widening Detection

- **Trigger**: Current spread > threshold (default: 20¢)
- **Action**: Widen quotes by N ticks (default: 5 ticks = 5¢)
- **Recovery**: Requires N consecutive good checks (default: 5)

#### Book Health Monitoring

- **Trigger**: Book depth < threshold OR spread very wide
- **Action**: Widen quotes by N ticks (default: 3 ticks)
- **Recovery**: Requires N consecutive good checks

#### Adverse Selection Detection

Tracks fills and price movements to detect if we're being picked off:

- **Window**: Last 5 minutes of fills
- **Detection**: If ≥ 70% of fills are on wrong side of price move
  - Bought and price went down
  - Sold and price went up
- **Action**: Widen quotes
- **Recovery**: Requires N consecutive good checks

#### Hysteresis

Uses hysteresis to prevent oscillation:
- **Trigger Threshold**: N consecutive bad checks (default: 3)
- **Recovery Threshold**: N consecutive good checks (default: 5)
- Prevents rapid on/off switching

### 6.2 Order Management

#### Price Clamping

Quotes are clamped to stay competitive within NBBO:

- **Buy Orders**: 
  - Must be ≤ best ask - 1 tick
  - Prefer ≥ best bid + 1 tick (improve if possible)
  - Skip if would cross book

- **Sell Orders**:
  - Must be ≥ best bid + 1 tick
  - Prefer ≤ best ask - 1 tick (improve if possible)
  - Skip if would cross book

#### Order Replacement Logic

- Keeps existing order if: price within 1¢ AND size matches
- Cancels and replaces if: price drifts > 1¢ OR size changes
- Prevents excessive order churn

#### Fill Reconciliation

- Pulls fills from Kalshi API every loop iteration
- Uses `iter_fills_since(last_fill_ts)` for pagination
- Deduplicates by `fill_id`
- Handles partial fills correctly
- Single source of truth (Kalshi API, not order status)

#### One-Sided Quoting

For hard extreme prices:
- **< 5¢**: Only post bids (we like being long cheap)
- **> 95¢**: Only post asks (we don't want to be long expensive)
- Prevents getting trapped in extreme positions

### 6.3 Position Management

#### Inventory Tracking

- Position fetched every loop iteration
- Logged to analytics on change
- Used for reservation price and quote skewing

#### Position Closing on Exit

When market maker session ends:
- Optionally closes all positions (configurable)
- Places limit order at current market price
- Logs close order to analytics

#### Dynamic Position Sizing

Position sizes adjust based on:
- Remaining capacity: `max_position - |inventory|`
- Buffer size: `max_position * position_limit_buffer`
- Inventory direction: smaller orders on side where we're building inventory

## Configuration

The bot is configured via `config.yaml`. This section documents all available configuration options with their default values and descriptions.

### Discovery Settings

```yaml
discovery:
  model_version: "AS-v1.0"  # Model version identifier
  
  # Number of top markets to generate configs for and trade
  max_markets_to_trade: 20
  
  # Which sides to trade: ['yes'], ['no'], or ['yes', 'no']
  sides_to_trade:
    - 'yes'
    # - 'no'  # Uncomment to also trade NO side
  
  # Time-to-expiry windows per category (in hours/days)
  time_windows:
    default:
      min_days: 1
      max_days: 60
    sports:
      min_hours: 1
      max_days: 7
    rates:
      min_days: 1
      max_days: 90
    crypto:
      min_days: 1
      max_days: 90
  
  # Categories to exclude (chronically illiquid)
  excluded_categories:
    - "long-tail-politics"
    - "niche-rankings"
  
  # Recent activity filter (softer for scoring, stricter for execution)
  recent_activity:
    scoring:
      min_trades_last_minutes: 0  # Allow if 24h volume is high
      min_volume_24h_for_bypass: 10000  # If 24h volume > this, allow even with no recent trades
    execution:
      min_trades_last_minutes: 10  # Stricter for MM layer
      throttle_if_no_trades_minutes: 30
      min_recent_trades_for_event_markets: 3  # Block thin markets from volatility events
```

### Scoring Weights

```yaml
scoring:
  weights:
    volume_24h: 0.30        # 24-hour volume weight
    recent_volume_1h: 0.25  # Recent 1-hour volume weight
    book_liquidity: 0.20    # Order book depth weight
    spread: -0.15           # Negative because tighter is better
    time_penalty: -0.10      # Negative because closer expiry is riskier
  
  # Volume label bands (based on normalized value)
  volume_bands:
    very_low: 0.25
    low: 0.5
    medium: 0.75
    high: 1.0
  
  # Minimum volume threshold (dollars)
  min_volume_24h: 5000.0
```

### Risk Parameters

```yaml
risk:
  # Global risk budget - total dollar exposure across all markets
  global_risk_budget: 60  # Leaves buffer for safety
  
  # Maximum exposure per market (dollars)
  per_market_cap: 8
  
  # Scaling factor for sqrt(liquidity) calculation
  alpha: 0.1
  
  # Category-level caps (for correlated markets)
  category_caps:
    crypto: 8
    rates: 8
    sports: 8
    default: 8
  
  # Extra haircuts for long-dated markets
  long_dated_penalty_days: 30  # Markets > this get haircut
  long_dated_penalty_factor: 0.7
```

### AS Model Parameters

```yaml
as_model:
  # Risk horizon (shorter than expiry for event markets)
  risk_horizon_seconds: 3600  # 1 hour default
  
  # Order intensity parameter k (per category)
  k_by_category:
    default: 1.5
    sports: 2.0
    rates: 1.2
    crypto: 1.8
  
  # Gamma (risk aversion) - can be per-market based on characteristics
  base_gamma: 0.3  # Increased for better inventory skewing
  
  # Spread limits (total spread in dollars)
  min_total_spread: 0.02  # Minimum total spread to quote
  max_total_spread: 0.02  # Maximum total spread cap
  
  # Sigma estimation
  sigma:
    baseline: 0.05
    use_proxy: true  # Use simple proxy until historical data available
    cache_ttl_hours: 1
```

### Extreme Price Handling

```yaml
extreme_prices:
  normal_band: [0.15, 0.85]
  soft_extreme_band: [[0.05, 0.15], [0.85, 0.95]]
  hard_extreme_band: [[0.0, 0.05], [0.95, 1.0]]
  
  # Multiplicative haircuts on base max_position
  soft_extreme_factor: 0.3
  hard_extreme_factor: 0.1
  
  # One-sided quoting for hard extremes
  hard_extreme_one_sided: true
  
  # Spread shock protection (Kalshi sometimes jumps spread instantly)
  spread_shock_threshold: 0.15  # Freeze quoting if spread > this threshold
```

### Dynamic Throttling

```yaml
throttling:
  # Hysteresis: require N consecutive checks before triggering
  trigger_threshold: 3  # Bad checks needed to trigger
  recovery_threshold: 5  # Good checks needed to recover
  
  # Spread widening
  max_spread_threshold: 0.20  # If spread > 20¢, throttle
  spread_widen_ticks: 5  # Widen by 5 ticks when throttled
  max_total_widened_spread: 0.15  # Maximum spread after widening
  
  # Book health
  min_book_depth: 100  # Minimum book depth in contracts
  book_thin_widen_ticks: 3
  
  # Adverse selection detection
  adverse_selection_window_seconds: 300  # Look back 5 minutes
  adverse_selection_threshold: 0.7  # If 70% of fills are on wrong side of move
```

### Category Profiles

Category profiles define behavioral classes for different market types, with regime-specific overrides:

```yaml
category_profiles:
  mean_reverting:
    # Rates, economic indicators, weather - MM-friendly, mean-reverting
    sigma_blend: 0.5
    spread_tight: 0.02
    spread_wide: 0.04
    exclude: false
    by_regime:
      QUIET:
        spread_tight: 0.015
      MEAN_REVERTING:
        spread_tight: 0.02
        spread_wide: 0.04
      TRENDING:
        exclude: true
        max_position_factor: 0.3
      CHAOTIC:
        exclude: true
      NOISE:
        exclude: true
        max_position_factor: 0.0
      UNKNOWN:
        exclude: true
        max_position_factor: 0.0
  
  medium_trending:
    # Crypto - sometimes directional, sometimes mean-reverting
    sigma_blend: 0.25
    spread_tight: 0.03
    spread_wide: 0.08
    exclude: false
    by_regime:
      MEAN_REVERTING:
        spread_tight: 0.03
        sigma_blend: 0.3
      TRENDING:
        spread_tight: 0.06
        spread_wide: 0.12
        max_position_factor: 0.5
      CHAOTIC:
        exclude: true
      NOISE:
        exclude: true
        max_position_factor: 0.0
      UNKNOWN:
        exclude: true
        max_position_factor: 0.0
  
  hard_trending:
    # Sports - directional, toxic to MM (excluded by default)
    sigma_blend: 0.2
    spread_tight: 0.10
    spread_wide: 0.20
    exclude: true
    by_regime:
      TRENDING:
        exclude: true
      CHAOTIC:
        exclude: true
      NOISE:
        exclude: true
        max_position_factor: 0.0
      UNKNOWN:
        exclude: true
        max_position_factor: 0.0
  
  low_liquidity:
    # Entertainment, tech events - low liquidity, wider spreads needed
    sigma_blend: 0.1
    spread_tight: 0.04
    spread_wide: 0.12
    exclude: false
    by_regime:
      TRENDING:
        exclude: true
      CHAOTIC:
        exclude: true
      NOISE:
        exclude: true
        max_position_factor: 0.0
      UNKNOWN:
        exclude: true
        max_position_factor: 0.0

# Category to profile mapping
category_mapping:
  rates: mean_reverting
  economic: mean_reverting
  crypto: medium_trending
  sports: hard_trending
  entertainment: low_liquidity
  tech: low_liquidity
  default: mean_reverting
  fallback: mean_reverting  # Use when category unknown or invalid
```

### Regime Detection Configuration

```yaml
regimes:
  # Minimum observations before classifying regime
  min_observations: 5
  
  # Drift threshold for trending detection (normalized by volatility)
  drift_threshold: 0.08
  drift_normalization_min_sigma: 0.001  # Prevents division by zero
  
  # Minimum sigma to exit QUIET regime
  min_sigma_for_regime: 0.01
  
  # Mean reversion thresholds
  mean_reversion_good: 0.6  # Above this = MEAN_REVERTING
  mean_reversion_bad: 0.4   # Below this = TRENDING (if drift also high)
  
  # Depth threshold for CHAOTIC detection
  depth_threshold: 50.0  # Minimum liquidity to avoid CHAOTIC
  
  # Regime stability requirement (N consecutive windows must match)
  stability_required: 3
  
  # NOISE regime detection thresholds
  noise_spread_min: 0.07  # Minimum spread for NOISE classification
  noise_spread_max: 0.12  # Maximum spread for NOISE classification
  
  # Shared state freshness (for scanner-MM communication)
  max_shared_state_age_seconds: 4  # MM should freeze if regime data is older than this
  
  # Per-profile drift threshold overrides
  by_profile:
    mean_reverting:
      drift_threshold: 0.06
    medium_trending:
      drift_threshold: 0.10
  
  # Gamma multipliers by regime (adjusts risk aversion)
  gamma_multipliers:
    MEAN_REVERTING: 1.0
    QUIET: 0.8
    TRENDING: 1.5
    CHAOTIC: 2.0
    NOISE: 1.2
    UNKNOWN: 1.5
  
  # k multipliers by regime (adjusts order intensity)
  k_multipliers:
    MEAN_REVERTING: 1.0
    QUIET: 0.7
    TRENDING: 0.5
    CHAOTIC: 0.3
    NOISE: 0.6
    UNKNOWN: 0.5
```

### Volatility Scanner Configuration

```yaml
volatility_scanner:
  refresh_interval_seconds: 3  # Round-robin through tracked markets every 3s
  rolling_window_minutes: 15    # Rolling window for state tracking
  volatility_window_minutes: 10 # Window for σ calculation
  
  # Volatility thresholds (at least one must pass)
  price_jump_threshold_cents: 12  # X cents move required
  price_jump_window_minutes: 5    # Within Y minutes
  sigma_threshold: 0.02           # Minimum σ to trigger
  mean_reversion_tolerance_cents: 6  # Max deviation from 3-min mean for sigma-only triggers
  
  # Categories to exclude from volatility scanning
  exclude_categories:
    - sports  # Sports volatility is directional, not mean-reverting - fundamentally incompatible with MM
  
  # Regime filters for event detection
  regime_filters:
    allowed_for_events:
      - MEAN_REVERTING
      - QUIET
    disallowed_for_events:
      - TRENDING
      - CHAOTIC
      - NOISE
      - UNKNOWN
  
  # Orderbook configuration
  orderbook_depth: 10  # Depth levels to fetch for orderbook
  min_orderbook_levels_required: 1  # Minimum levels required to prevent regime flipping
  
  # Liquidity requirements
  max_time_to_expiry_hours: 24    # T_max
  min_liquidity_spread: 0.10      # Max spread to consider
  
  # Regime staleness protection
  regime_staleness_seconds: 4  # MM should freeze if regime timestamp is older than this
```

### Volatility MM Configuration

```yaml
volatility_mm:
  max_concurrent_sessions: 3  # Maximum concurrent market making sessions
  session_ttl_minutes: 5      # Session time-to-live (shortened for sports markets)
  quote_refresh_seconds: 3    # Faster than default
  
  # Exit strategy - passive exit prevents crystallizing losses at worst prices
  passive_exit: true  # Stop quoting on TTL, keep positions open, close passively
  ttl_soft_stop: true  # Stop quoting when TTL expires (don't force close)
  ttl_close_if_small: 2  # Only auto-close if |inventory| < this threshold
  close_positions_on_exit: false  # Deprecated - use passive_exit instead
  
  # Regime-aware entry conditions
  allowed_regimes_for_sessions:
    - MEAN_REVERTING
    - QUIET
  require_regime_stability: 3  # Consecutive windows required
  delay_before_session_start_seconds: 5  # Wait for stable regime before starting
  
  # Regime kill switches
  regime_kill:
    enabled: true
    bad_regimes:
      - TRENDING
      - CHAOTIC
    require_adverse_selection: true  # Only kill if also being picked off
    kill_modes:
      TRENDING: soft  # Freeze quoting, wait for limit exits
      CHAOTIC: hard   # Exit immediately
  
  # Sigma blending configuration (for volatility event sigma)
  sigma_blend_weight: 0.5  # Default fallback (overridden by category profiles, jump logic, and regime)
  sigma_cap: 0.08  # Maximum sigma to use (8%)
  sigma_floor: 0.005  # Minimum sigma to use (0.5%)
  min_sigma_after_blending: 0.005  # Alias for sigma_floor
  
  # Jump-aware sigma blending
  jump_magnitude_threshold_cents: 10  # If jump >= this, ignore event sigma (blend_weight = 0.0)
  
  # Regime staleness protection
  regime_stale_kill_seconds: 4  # Hard kill if regime data is stale beyond this threshold
  
  # Soft exit timeout
  soft_exit_max_wait_seconds: 20  # Maximum time to wait in soft exit mode before forcing hard exit
  
  # Passive exit slippage tolerance
  max_slippage_for_limit_exit_cents: 4  # Maximum acceptable slippage for passive limit orders
  
  # Session management safety
  disallow_new_sessions_if_open_positions_exist: true  # Prevent multiple sessions on same ticker if unwinding
  
  # Balance and margin checks
  min_available_balance: 100  # Minimum USD balance to open new sessions
  max_resting_order_value: 10000  # Maximum USD locked in resting orders
  balance_hysteresis_ratio: 0.85  # Re-enable threshold multiplier
  
  # Minimal guardrails (ONLY THREE)
  guardrails:
    expiration_danger_window_seconds: 180  # Stop quoting 3 min before close
    illiquidity_timeout_seconds: 15        # No trades for 15s = freeze
    min_volatility_threshold: 0.01        # If vol drops below this, kill session
```

### WebSocket Configuration

```yaml
websockets:
  enabled: false  # Set to true to enable WebSocket real-time updates
  url: "wss://api.elections.kalshi.com"  # Base URL - /trade-api/ws/v2 will be appended
  
  # Connection management
  reconnect_max_retries: 0  # 0 = infinite retries
  reconnect_base_delay_seconds: 0.5  # Initial delay before reconnect
  reconnect_max_delay_seconds: 5.0  # Maximum delay (exponential backoff cap)
  ping_interval_seconds: 15  # WebSocket ping interval (handled by library)
  idle_timeout_seconds: 30  # Consider connection dead if no messages for this duration
  
  # Subscriptions (global channels)
  subscribe_ticker: true  # Subscribe to ticker updates
  subscribe_orderbook: true  # Subscribe to orderbook updates
  subscribe_trades: true  # Subscribe to public trades
  subscribe_user_fills: true  # Subscribe to your order fills (authenticated)
  subscribe_positions: true  # Subscribe to position updates (authenticated)
  subscribe_lifecycle: true  # Subscribe to market lifecycle events
  
  # Backpressure / load control
  max_markets_per_connection: 200  # Maximum markets to track per connection
  shard_connections: false  # Future: partition markets across multiple connections
  
  # Staleness thresholds (seconds)
  ticker_staleness_seconds: 10  # Ticker data older than this is considered stale
  orderbook_staleness_seconds: 10  # Orderbook data older than this is considered stale
  position_staleness_seconds: 30  # Position data older than this is considered stale
  
  # Buffer sizes
  max_price_history: 1000  # Maximum price history points per market (rolling window)
  max_trades_per_ticker: 1000  # Maximum trades to keep per market (ring buffer)
  
  # Health and fallback
  ws_fallback_on_unhealthy: true  # Automatically fallback to REST if WebSocket unhealthy
  ws_staleness_freeze_seconds: 5  # Freeze quoting if WebSocket stale >5s
  ws_staleness_stop_seconds: 15  # Hard stop if WebSocket stale >15s
```

**Note**: When `enabled: false`, the system operates in REST-only mode. All components automatically fall back to REST API calls.

## Database Schema

The analytics database (`trading_analytics.db`) tracks all trading activity.

### Core Tables

#### `strategy_runs`
Tracks each market making session:
- `run_id`: Primary key
- `strategy_name`: Name (e.g., "MARKET-TICKER-VOL-1234567890")
- `market_ticker`: Market identifier
- `trade_side`: 'yes' or 'no'
- `start_time`, `end_time`: Session duration
- `config_params`: JSON of all config parameters

#### `market_snapshots`
Periodic market state snapshots:
- `run_id`: Links to strategy run
- `timestamp`: Snapshot time
- `yes_bid`, `yes_ask`, `yes_mid`: Yes side prices
- `no_bid`, `no_ask`, `no_mid`: No side prices
- `current_position`: Inventory at snapshot time
- `reservation_price`: Calculated reservation price
- `computed_bid`, `computed_ask`: Quotes from AS model
- `spread`: `computed_ask - computed_bid`

#### `orders`
All order events:
- `order_id`: UUID from Kalshi
- `run_id`: Links to strategy run
- `action`: 'buy' or 'sell'
- `side`: 'yes' or 'no'
- `placed_price`, `placed_quantity`: Order details
- `status`: 'placed', 'filled', 'partially_filled', 'cancelled', 'expired'
- `filled_price`, `filled_quantity`: Fill details
- `expiration_ts`: Order expiration

#### `order_fills`
Individual fill events (supports partial fills):
- `fill_id`: Unique fill ID from Kalshi
- `order_id`: Links to order
- `count`: Quantity filled in this event
- `price_cents`: Price in cents
- `ts`: Unix timestamp

#### `trades`
Completed trades (derived from fills):
- `order_id`: Links to order
- `action`, `side`, `price`, `quantity`: Trade details
- `realized_pnl`: PnL when position closed

#### `position_history`
Position changes over time:
- `run_id`: Links to strategy run
- `timestamp`: Position change time
- `position`: New position value
- `unrealized_pnl`: PnL based on current market price

### Discovery Tables

#### `discovery_sessions`
Tracks each discovery run:
- `session_id`: Primary key
- `total_markets`, `binary_markets`, `pre_filtered`, `scored`: Pipeline stats
- `top_markets_count`, `configs_generated`: Results
- `model_version`: Model version string

#### `discovered_markets`
Individual market discoveries:
- `session_id`: Links to discovery session
- `ticker`, `title`: Market info
- `score`: Profitability score
- `volume_24h`, `spread`, `mid_price`, `liquidity`: Metrics
- `reasons`: JSON array of scoring reasons
- `generated_config`: JSON of generated config
- `selected_for_trading`: Boolean flag
- `trade_side`: 'yes', 'no', or NULL

### Volatility Tables

#### `volatility_events`
Detected volatility events:
- `event_id`: Primary key
- `market_ticker`: Market identifier
- `timestamp`: Event detection time
- `signal_strength`: Combined signal (0-1)
- `sigma`: Realized volatility
- `jump_magnitude`: Price jump in cents (if any)
- `direction`: 'up', 'down', or NULL
- `close_time`: Market close time
- `run_id`: Links to spawned session (if any)

#### `volatility_sessions`
Ephemeral MM sessions:
- `session_id`: Primary key
- `event_id`: Links to volatility event
- `run_id`: Links to strategy run
- `start_time`, `end_time`: Session duration
- `kill_reason`: Why session ended
- `final_pnl`: Final PnL for session

## Setup & Installation

### Prerequisites

- Python 3.9+
- Kalshi API credentials (access key and private key)
- SQLite3 (included with Python)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd KalshiMarketMaker
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   Create a `.env` file:
   ```env
   KALSHI_ACCESS_KEY=your_access_key
   KALSHI_PRIVATE_KEY=your_private_key
   KALSHI_BASE_URL=https://api.elections.kalshi.com/trade-api/v2
   ```
   
   **Note**: The private key can be:
   - A file path to a PEM file, OR
   - The PEM content directly (use `\n` for newlines if in .env)

4. **Configure the bot**:
   Edit `config.yaml` with your desired settings (see [Configuration](#configuration))

5. **Initialize the database**:
   The database is automatically created on first run. The schema is defined in `schema.sql`.

### Environment Variables

- `KALSHI_ACCESS_KEY`: Your Kalshi API access key
- `KALSHI_PRIVATE_KEY`: Path to PEM file OR PEM content (with `\n` for newlines)
- `KALSHI_BASE_URL`: Kalshi API base URL (default: `https://api.elections.kalshi.com/trade-api/v2`)

## Usage

### Running the Bot

The bot runs in volatility-driven mode by default:

```bash
python runner.py --config config.yaml
```

This will:
1. Initialize the analytics database
2. Start the volatility scanner (monitors top 200 markets)
3. Start the volatility MM manager (spawns sessions on events)
4. Run until interrupted (Ctrl+C)

### Monitoring

#### Log Files

- **Main log**: Console output (stdout)
- **Strategy logs**: `{strategy_name}.log` for each session
- **Volatility sweep details**: `volatility_sweep_details_YYYYMMDD.log`

#### Database Queries

Query the analytics database using SQLite:

```bash
sqlite3 data/trading_analytics.db
```

Example queries:

```sql
-- Recent strategy runs
SELECT * FROM strategy_runs ORDER BY start_time DESC LIMIT 10;

-- Orders for a run
SELECT * FROM orders WHERE run_id = ?;

-- Volatility events
SELECT * FROM volatility_events ORDER BY timestamp DESC LIMIT 10;

-- Discovery sessions
SELECT * FROM discovery_sessions ORDER BY timestamp DESC LIMIT 5;
```

See `analytics_queries.sql` for more example queries.

### Stopping the Bot

Press `Ctrl+C` to gracefully shutdown:
- Scanner stops scanning
- Manager terminates all active sessions
- Sessions close positions (if configured)
- All data logged to database

## Deployment

### Local Development

1. Follow [Setup & Installation](#setup--installation)
2. Run: `python runner.py --config config.yaml`
3. Monitor logs and database

### Fly.io Deployment

1. **Install flyctl**:
   ```bash
   # macOS
   brew install flyctl
   
   # Or download from https://fly.io/docs/hands-on/install-flyctl/
   ```

2. **Login to fly.io**:
   ```bash
   flyctl auth login
   ```

3. **Initialize your app**:
   ```bash
   flyctl launch
   ```
   Follow prompts, but don't deploy yet.

4. **Set secrets** (API credentials):
   ```bash
   flyctl secrets set KALSHI_ACCESS_KEY=your_access_key
   flyctl secrets set KALSHI_PRIVATE_KEY="$(cat path/to/private_key.pem)"
   flyctl secrets set KALSHI_BASE_URL=https://api.elections.kalshi.com/trade-api/v2
   ```

5. **Deploy**:
   ```bash
   flyctl deploy
   ```

6. **Monitor logs**:
   ```bash
   flyctl logs
   ```

### Docker

The `Dockerfile` is configured to:
- Use Python 3.9-slim base image
- Install dependencies from `requirements.txt`
- Run `runner.py` with `config.yaml` on startup

Build and run:
```bash
docker build -t kalshi-mm .
docker run -d --env-file .env kalshi-mm
```

## Monitoring & Analytics

### Log Files

1. **Console Output**: Main system logs (scanner, manager, discovery)
2. **Strategy Logs**: `{ticker}-{side}.log` or `{ticker}-VOL-{timestamp}.log`
3. **Volatility Sweep Details**: `volatility_sweep_details_YYYYMMDD.log` (detailed metrics for each sweep cycle)

### Database Analytics

The SQLite database provides comprehensive analytics:

#### Key Metrics to Track

- **Strategy Performance**: PnL per run, fill rates, average spreads
- **Volatility Events**: Detection rate, signal strength distribution
- **Order Execution**: Fill latency, cancellation rates
- **Market Discovery**: Top markets by score, selection rates

#### Example Queries

See `analytics_queries.sql` for comprehensive query examples.

### Performance Metrics

Monitor:
- **Active Sessions**: Number of concurrent MM sessions
- **Event Detection Rate**: Volatility events per hour
- **Fill Rate**: Percentage of orders filled
- **Position Exposure**: Total dollar exposure across all markets
- **Database Size**: Growth over time (consider archiving old data)

### Troubleshooting

#### Common Issues

1. **No markets discovered**:
   - Check volume thresholds in `config.yaml`
   - Verify markets are open and binary
   - Check category exclusions

2. **No volatility events**:
   - Adjust `sigma_threshold` and `price_jump_threshold_cents` in config
   - Check that markets have sufficient activity
   - Verify spread filter isn't too tight

3. **Orders not filling**:
   - Check if quotes are competitive (within NBBO)
   - Verify minimum spread isn't too wide
   - Check for throttling (spread widening active)

4. **Database errors**:
   - Ensure SQLite has write permissions
   - Check disk space
   - Verify schema is initialized

5. **API authentication errors**:
   - Verify private key format (PEM)
   - Check timestamp synchronization
   - Ensure access key is correct

#### Debug Logging

Set log level to DEBUG in code or config for detailed logging:
```python
logger.setLevel(logging.DEBUG)
```

## WebSocket Implementation

### Overview

The WebSocket implementation provides real-time market data updates, reducing latency and API rate limit pressure compared to polling REST endpoints.

### Key Features

- **Real-Time Updates**: Sub-millisecond latency for price and order book updates
- **Automatic Reconnection**: Handles connection drops gracefully with exponential backoff
- **Subscription Management**: Automatic re-subscription on reconnect
- **Health Monitoring**: Staleness detection with automatic REST fallback
- **Thread-Safe**: Safe concurrent access from multiple components
- **Message Normalization**: Consistent data format regardless of API message structure

### Channel Names

**Critical**: The Kalshi API uses specific channel names that must match exactly:

| Channel Type | API Channel Name | Internal Name | Notes |
|-------------|------------------|---------------|-------|
| Ticker | `"ticker"` | `"ticker"` | Market-specific or global |
| Orderbook | `"orderbook_delta"` | `"orderbook_delta"` | Market-specific |
| Public Trades | `"trade"` | `"trades"` | Market-specific |
| User Fills | `"fill"` | `"fills"` | Global (authenticated) |
| Positions | `"market_positions"` | `"positions"` | Global (authenticated) |
| Lifecycle | `"market_lifecycle_v2"` | `"lifecycle"` | Global |

**Important**: Always use the API channel names in subscription messages. The client maps them to internal names for processing.

### Message Flow

1. **Subscription**: Client sends subscription message with channel name
2. **Confirmation**: Server responds with `{"type": "subscribed", "id": <msg_id>}`
3. **Data Updates**: Server sends `{"type": "<channel>", "data": {...}}` or `{"type": "<channel>", "msg": {...}}`
4. **Normalization**: Client normalizes message format
5. **State Update**: State store is updated with normalized data
6. **Callback**: Registered callbacks are invoked

### Error Handling

- **Connection Errors**: Automatic reconnection with exponential backoff
- **Subscription Errors**: Failed subscriptions are logged and removed from pending list
- **Message Parsing Errors**: Malformed messages are logged but don't crash the client
- **Staleness**: Components check staleness and fall back to REST if data is too old

### Integration with Components

#### Volatility Scanner

- Subscribes to ticker, orderbook, and trades for tracked markets
- Uses WebSocket data when available and fresh
- Falls back to REST if WebSocket is unhealthy or data is stale

#### Market Maker

- Reads ticker and orderbook data from state store
- Uses WebSocket fills when available
- Falls back to REST for all operations if WebSocket unavailable

#### State Store

- Centralized cache for all WebSocket data
- Provides staleness detection
- Thread-safe access for concurrent readers

### Performance Considerations

- **Memory**: State store uses bounded buffers (ring buffers, max history)
- **CPU**: Message normalization is lightweight
- **Network**: Single WebSocket connection handles all subscriptions
- **Latency**: Sub-millisecond message processing

## Testing

The bot includes a comprehensive test suite for WebSocket integration. Tests are organized into several categories:

### Test Structure

- **Unit Tests**: Fast tests using mocks (`tests/unit/test_websocket_client.py`, `tests/unit/test_market_state_store.py`)
- **Integration Tests**: Tests with real WebSocket connections (requires credentials) - `test_websocket_integration.py`
- **E2E Tests**: Full system tests (`test_websocket_e2e.py`)
- **Performance Tests**: Throughput and latency measurements (`test_websocket_performance.py`)
- **Error Handling Tests**: Edge cases and error scenarios (`test_websocket_errors.py`)
- **Reconnection Tests**: Connection drop and recovery scenarios (`test_websocket_reconnection.py`)
- **Fallback Tests**: REST fallback behavior (`test_websocket_fallback.py`)

### Running Tests

**Unit tests only** (fast, no network):
```bash
python -m pytest tests/unit/ -v
```

**Integration tests** (requires Kalshi credentials):
```bash
export KALSHI_ACCESS_KEY=your_key
export KALSHI_PRIVATE_KEY_PATH=path/to/key.pem
python -m pytest tests/integration/ -v
```

**E2E tests**:
```bash
python -m pytest tests/e2e/ -v
```

**Full test suite**:
```bash
python -m pytest tests/ -v
```

**Specific test categories**:
```bash
# Unit tests only
python -m pytest tests/unit/ -v

# Integration tests only
python -m pytest tests/integration/ -v

# E2E tests only
python -m pytest tests/e2e/ -v

# Specific test file
python -m pytest tests/integration/test_websocket_integration.py -v
```

### Test Utilities

The `tests/unit/test_websocket_utils.py` module provides:
- `MockWebSocketApp`: Mock WebSocket client for unit tests
- Message generators: `create_mock_ticker_message()`, `create_mock_orderbook_message()`, etc.
- `wait_for_condition()`: Helper for async test conditions

### Test Coverage

Tests verify:
1. ✅ WebSocket connects and authenticates correctly
2. ✅ All channel subscriptions work (ticker, orderbook, trades, fills, positions, lifecycle)
3. ✅ Messages are parsed and normalized correctly
4. ✅ State store updates correctly and maintains consistency
5. ✅ Scanner uses WS data when available, falls back to REST
6. ✅ MM uses WS data when available, falls back to REST
7. ✅ Reconnection works and re-subscribes correctly
8. ✅ Staleness detection works correctly
9. ✅ Health flags toggle correctly
10. ✅ No data corruption under concurrent access
11. ✅ System gracefully handles errors and edge cases
12. ✅ Channel name mapping (API → internal) works correctly
13. ✅ Message normalization handles all message types

## License

See `LICENSE.md` for license information.

## Contributing

This is a private trading bot. Contributions should be discussed with the maintainer.

---

**Note**: This bot is designed for algorithmic trading on Kalshi. Use at your own risk. Always test with small positions first and monitor closely.
