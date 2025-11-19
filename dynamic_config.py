import logging
import math
from typing import Dict, Optional
from market_discovery import MarketDiscovery


def get_extreme_price_band(mid_price: float, config: Dict) -> str:
    """
    Determine extreme price band: normal, soft_extreme, or hard_extreme
    """
    extreme_config = config.get('extreme_prices', {})
    normal_band = extreme_config.get('normal_band', [0.15, 0.85])
    soft_extreme_bands = extreme_config.get('soft_extreme_band', [[0.05, 0.15], [0.85, 0.95]])
    hard_extreme_bands = extreme_config.get('hard_extreme_band', [[0.0, 0.05], [0.95, 1.0]])
    
    if normal_band[0] <= mid_price <= normal_band[1]:
        return 'normal'
    
    for band in soft_extreme_bands:
        if band[0] <= mid_price <= band[1]:
            return 'soft_extreme'
    
    for band in hard_extreme_bands:
        if band[0] <= mid_price <= band[1]:
            return 'hard_extreme'
    
    return 'normal'  # Default


def calculate_risk_budget(market_data: Dict, discovery: MarketDiscovery, config: Dict, n_active_markets: int = 1) -> float:
    """
    Calculate max dollar exposure using sublinear relationship with liquidity
    Returns exposure in dollars
    """
    risk_config = config.get('risk', {})
    alpha = risk_config.get('alpha', 0.1)
    per_market_cap = risk_config.get('per_market_cap', 2000)
    global_budget = risk_config.get('global_risk_budget', 10000)
    
    # Get liquidity
    if 'liquidity' in market_data:
        liquidity = market_data['liquidity']  # Already in dollars
    else:
        liquidity = market_data.get('liquidity', 0) / 100  # Convert cents to dollars
    
    # Sublinear relationship: raw = alpha * sqrt(liquidity)
    raw_exposure = alpha * math.sqrt(max(liquidity, 0))
    
    # Apply caps
    global_cap_per_market = global_budget / max(n_active_markets, 1)
    max_exposure = min(raw_exposure, per_market_cap, global_cap_per_market)
    
    # Category-level cap
    category = discovery.get_category(market_data.get('market_data', market_data))
    category_caps = risk_config.get('category_caps', {})
    category_cap = category_caps.get(category, category_caps.get('default', 2000))
    max_exposure = min(max_exposure, category_cap)
    
    return max_exposure


def generate_config_for_market(
    market_data: Dict, 
    side: str = 'yes',
    discovery: Optional[MarketDiscovery] = None,
    config: Optional[Dict] = None,
    n_active_markets: int = 1
) -> Optional[Dict]:
    """
    Generate config for a market based on its characteristics
    Uses risk-aware sizing, extreme price handling, and estimated sigma
    
    This function is used for both discovery-based and volatility-based configs.
    For volatility configs, the manager will override T (session duration) and dt
    (quote refresh rate) after calling this function.
    """
    logger = logging.getLogger("DynamicConfig")
    
    if config is None:
        config = {}
    
    # Handle both scored market dict and raw market dict
    if 'score' in market_data:
        # This is a scored market dict (volume_24h already in dollars, spread already calculated)
        ticker = market_data['ticker']
        volume_24h = market_data.get('volume_24h', 0)  # Already in dollars
        spread = market_data.get('spread', 0.05)  # Already in dollars
        mid_price = market_data.get('mid_price', 0.5)
        raw_market = market_data.get('market_data', market_data)
    else:
        # This is a raw market dict (volume_24h in cents, need to calculate spread)
        ticker = market_data['ticker']
        volume_24h = market_data.get('volume_24h', 0) / 100  # Convert cents to dollars
        # Calculate spread from bid/ask
        yes_spread = (market_data.get('yes_ask', 0) - market_data.get('yes_bid', 0)) / 100
        no_spread = (market_data.get('no_ask', 0) - market_data.get('no_bid', 0)) / 100
        spread = min(yes_spread, no_spread) if yes_spread > 0 and no_spread > 0 else 0.05
        # Calculate mid price
        yes_mid = (market_data.get('yes_bid', 0) + market_data.get('yes_ask', 0)) / 200
        no_mid = (market_data.get('no_bid', 0) + market_data.get('no_ask', 0)) / 200
        mid_price = yes_mid if side == 'yes' else no_mid
        raw_market = market_data
    
    # Determine extreme price band
    extreme_band = get_extreme_price_band(mid_price, config)
    
    # Skip extreme markets entirely - don't generate configs for them
    if extreme_band in ['soft_extreme', 'hard_extreme']:
        logger.debug(f"Skipping {ticker}-{side}: extreme price band ({extreme_band}), mid_price={mid_price:.4f}")
        return None
    
    # Calculate base max_position from risk budget
    if discovery:
        max_dollar_exposure = calculate_risk_budget(market_data, discovery, config, n_active_markets)
        
        # Apply long-dated penalty if applicable
        time_to_expiry = discovery.get_time_to_expiry_days(raw_market)
        risk_config = config.get('risk', {})
        long_dated_days = risk_config.get('long_dated_penalty_days', 30)
        long_dated_factor = risk_config.get('long_dated_penalty_factor', 0.7)
        if time_to_expiry and time_to_expiry > long_dated_days:
            max_dollar_exposure *= long_dated_factor
        
        # Convert to contracts (each contract is $1)
        base_max_position = int(max_dollar_exposure)
    else:
        # Fallback to simple volume-based sizing
        if volume_24h > 100000:
            base_max_position = 50
        elif volume_24h > 50000:
            base_max_position = 30
        elif volume_24h > 10000:
            base_max_position = 20
        elif volume_24h > 5000:
            base_max_position = 15
        else:
            base_max_position = 10
    
    # Apply extreme price haircuts
    extreme_config = config.get('extreme_prices', {})
    if extreme_band == 'soft_extreme':
        factor = extreme_config.get('soft_extreme_factor', 0.3)
        max_position = max(1, int(base_max_position * factor))
    elif extreme_band == 'hard_extreme':
        factor = extreme_config.get('hard_extreme_factor', 0.1)
        max_position = max(1, int(base_max_position * factor))
    else:
        max_position = base_max_position
    
    # Estimate sigma
    if discovery:
        sigma = discovery.estimate_sigma(raw_market, mid_price)
    else:
        # Fallback to spread-based heuristic
        if spread > 0.05:
            sigma = 0.003
        elif spread > 0.02:
            sigma = 0.002
        else:
            sigma = 0.001
    
    # Get category for k parameter
    if discovery:
        category = discovery.get_category(raw_market)
    else:
        category = 'default'
    
    as_config = config.get('as_model', {})
    k_by_category = as_config.get('k_by_category', {})
    k = k_by_category.get(category, k_by_category.get('default', 1.5))
    base_gamma = as_config.get('base_gamma', 0.1)
    
    # Calculate dynamic inventory_skew_factor (inverse of liquidity/position limit)
    liquidity = raw_market.get('liquidity', 0) / 100  # Convert cents to dollars
    if liquidity > 0 and max_position > 0:
        # Inverse relationship: more liquidity or larger position limit → smaller skew
        # Keep range tame to avoid aggressive flipping
        base_skew = 0.0005
        liquidity_factor = 1.0 / (1.0 + liquidity / 10000)  # Normalize liquidity
        position_factor = 1.0 / (1.0 + max_position / 20)  # Normalize position
        inventory_skew_factor = base_skew * (liquidity_factor + position_factor) / 2
        inventory_skew_factor = max(0.0001, min(0.001, inventory_skew_factor))  # Clamp to reasonable range
    else:
        inventory_skew_factor = 0.0005
    
    # Set min_spread to AT LEAST the market spread + 0.5¢ buffer
    min_spread = max(0.01, spread + 0.005)
    
    # Determine if one-sided quoting is needed for hard extremes
    one_sided = False
    if extreme_band == 'hard_extreme' and extreme_config.get('hard_extreme_one_sided', True):
        one_sided = True
    
    config_dict = {
        'api': {
            'market_ticker': ticker,
            'trade_side': side
        },
        'market_maker': {
            'max_position': max_position,
            'order_expiration': 86400,
            'gamma': base_gamma,
            'k': k,
            'sigma': sigma,
            'T': 86400,
            'min_spread': min_spread,
            'position_limit_buffer': 0.05,
            'inventory_skew_factor': inventory_skew_factor,
            'extreme_band': extreme_band,
            'one_sided_quoting': one_sided
        },
        'dt': 5.0
    }
    
    logger.info(
        f"Generated config for {ticker}-{side}: "
        f"max_position={max_position} (band={extreme_band}), "
        f"sigma={sigma:.4f}, min_spread={min_spread:.3f}, "
        f"inventory_skew={inventory_skew_factor:.6f}, k={k}, "
        f"one_sided={one_sided}"
    )
    
    return config_dict

