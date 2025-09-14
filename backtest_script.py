class StrategyConfig:
    # Universe Filtering
    MIN_PRICE_THRESHOLD = 10000
    MIN_AVG_VOLUME = 100000
    MIN_AVG_VND_VOLUME = MIN_PRICE_THRESHOLD * MIN_AVG_VOLUME
    AVG_DOLLAR_VOLUME_WINDOW = 42

    # Entry/Exit Signals
    ATR_WINDOW = 42
    ATR_MULTIPLIER = 10

    # Position Sizing & Risk Management
    VOLATILITY_WINDOW = 42
    TARGET_VOLATILITY = 0.30
    MIN_ASSUMED_HOLDINGS = 30
    MAX_LEVERAGE = 2.0

    # Transaction Costs
    COMMISSION_RATE = 0.0015
    SELL_TAX_RATE = 0.001
    SLIPPAGE_RATE = 0.0005
    
    # Turnover Control (Optional - for advanced version)
    MIN_REBALANCE_WEIGHT_CHANGE = 0.0005
    MAX_PARTICIPATION_RATE = 0.10
    MAX_COST_TO_TRADE_RATIO = 0.01