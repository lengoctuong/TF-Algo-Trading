# FROM MIN DATE TO MAX DATE
class StrategyConfig:
    # Universe Filtering
    MIN_PRICE_THRESHOLD = 10_000
    MIN_AVG_VOLUME = 100_000
    AVG_VOLUME_WINDOW = 42 # Cannot change after loading

    # Entry/Exit Signals
    ATR_WINDOW = 42 # Cannot change after loading
    ATR_MULTIPLIER = 10

    # Position Sizing & Risk Management
    VOLATILITY_WINDOW = 42 # Cannot change after loading
    TARGET_VOLATILITY = 0.30
    MIN_ASSUMED_HOLDINGS = 20
    MAX_LEVERAGE = 1.0

    # --- NÂNG CẤP: Turnover Control ---
    USE_TURNOVER_CONTROL = True # Bật/tắt cơ chế kiểm soát
    # Ngưỡng thay đổi trọng số tối thiểu để tái cân bằng
    REBALANCE_THRESHOLD = 0.003

    # Transaction Costs
    COMMISSION_RATE = 0.0015
    SELL_TAX_RATE = 0.001
    SLIPPAGE_RATE = 0.0005
    
    # Initial Capital
    INITIAL_CAPITAL = 100_000_000
