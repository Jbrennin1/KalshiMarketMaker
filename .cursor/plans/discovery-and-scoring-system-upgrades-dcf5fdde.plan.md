<!-- dcf5fdde-ee2e-4b27-bd81-f888c7e5a834 4e4f1f78-056e-4a26-b8ba-ffaa9e2dc0b3 -->
# Dynamic Config System Integration

## Overview

Refactor the system to use dynamically generated configs from market discovery instead of static config.yaml entries. Discovery runs once at startup, generates configs for top markets, logs everything to the database, then runs the MM bot.

## Implementation Plan

### 1. Add Discovery Session Tracking to Database (`schema.sql`, `analytics.py`)

**Add new table for discovery sessions:**

- `discovery_sessions` table to track each discovery run
- Fields: session_id, timestamp, total_markets, binary_markets, pre_filtered, scored, top_markets_count, configs_generated
- `discovered_markets` table to track individual market discoveries
- Fields: session_id, ticker, title, score, volume_24h, spread, mid_price, liquidity, reasons (JSON), generated_config (JSON), selected_for_trading (boolean)

**Files to modify:**

- `schema.sql`: Add discovery_sessions and discovered_markets tables
- `analytics.py`: Add methods `create_discovery_session()`, `log_discovered_market()`, `get_discovery_session()`

### 2. Refactor runner.py to Do Discovery First (`runner.py`)

**New flow:**

1. Load config.yaml (only for discovery/scoring/risk settings, not strategy configs)
2. Run market discovery and scoring
3. Generate configs for top markets (both YES and NO sides)
4. Log discovery session and all discovered markets to database
5. Run MM bot with generated configs (not from config.yaml)

**Changes:**

- Remove dependency on static strategy configs in config.yaml
- Add `discover_and_generate_configs()` function that:
- Runs discovery → pre-filtering → scoring
- Generates configs for top N markets
- Logs everything to database
- Returns list of generated configs
- Modify `__main__` to call discovery first, then run strategies from generated configs
- Keep config.yaml for discovery/scoring/risk/throttling settings only

**Files to modify:**

- `runner.py`: Complete refactor of main flow

### 3. Add Discovery Session Logging (`analytics.py`)

**New methods:**

- `create_discovery_session()`: Create a new discovery session record
- `log_discovered_market()`: Log a discovered market with its score and generated config
- `get_discovery_session()`: Retrieve discovery session details
- `get_discovered_markets()`: Get all markets from a discovery session

**Files to modify:**

- `analytics.py`: Add discovery session methods

### 4. Update config.yaml Structure (`config.yaml`)

**Remove all static strategy configs:**

- Keep only: discovery, scoring, risk, as_model, extreme_prices, throttling sections
- Remove all individual market configs (KXBTCMINY-80000, etc.)
- Add optional `discovery` section with parameters like:
- `max_markets_to_trade`: Number of top markets to generate configs for
- `sides_to_trade`: ['yes', 'no'] or ['yes'] or ['no']
- `discovery_interval_seconds`: How often to re-run discovery (optional, for future)

**Files to modify:**

- `config.yaml`: Clean up to only have settings, not strategy configs

### 5. Enhanced Logging for Discovery (`market_discovery.py`, `market_scorer.py`)

**Add discovery session context:**

- Log discovery session ID when starting
- Log summary statistics at end of discovery
- Include discovery session metadata in logs

**Files to modify:**

- `market_discovery.py`: Add session logging
- `market_scorer.py`: Add session logging

### 6. Error Handling and Recovery (`runner.py`)

**Add robust error handling:**

- If discovery fails, log error but don't crash
- If config generation fails for a market, skip it and continue
- If MM bot fails for one market, continue with others
- Log all errors to database

**Files to modify:**

- `runner.py`: Add error handling throughout

## Implementation Order

1. Database schema updates (1) - add discovery session tables
2. Analytics methods (3) - add discovery logging methods
3. Config.yaml cleanup (4) - remove static strategy configs
4. Runner refactor (2) - implement discovery-first flow
5. Enhanced logging (5) - add session context
6. Error handling (6) - add robustness

## Testing Strategy

- Test discovery → config generation → logging flow
- Verify all discovered markets are logged to database
- Test that MM bot runs correctly with generated configs
- Test error handling when discovery fails or markets are invalid
- Verify config.yaml still works for settings-only mode

### To-dos

- [ ] Add pre-filtering to market_discovery.py: time-to-expiry (1-60 days), category exclusions, recent activity (N trades in X minutes)
- [ ] Rewrite market_scorer.py scoring with normalized features, weighted sum formula, fix volume metric consistency
- [ ] Add book depth calculation (sum of size at best bid/ask and next N levels) to market_discovery.py
- [ ] Add recent trade activity metrics (trades in last 5/15 min, volume in last hour) to market_discovery.py
- [ ] Implement per-market sigma estimation from historical price data (analytics DB or polling) in market_discovery.py
- [ ] Implement risk-aware max_position calculation based on liquidity and risk budget in dynamic_config.py
- [ ] Replace hard skip with 3-band extreme price handling (normal/soft/hard) in dynamic_config.py and mm.py
- [ ] Update calculate_optimal_spread() in mm.py to use proper AS formula and clamp to min/max spread
- [ ] Make inventory_skew_factor dynamic based on liquidity/position limit in dynamic_config.py
- [ ] Add dynamic throttling (spread widening, book health, adverse selection) to mm.py AvellanedaMarketMaker
- [ ] Add discovery/scoring config section to config.yaml with all thresholds and weights
- [ ] Add startup logging of thresholds and fix volume label consistency in market_scorer.py