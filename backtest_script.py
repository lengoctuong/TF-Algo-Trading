#%%
import pandas as pd
import numpy as np
import os
from tqdm import tqdm

# ==============================================================================
# BƯỚC 1: CẤU HÌNH CHIẾN LƯỢC (ĐÃ CẬP NHẬT)
# ==============================================================================
class StrategyConfig:
    # Universe Filtering
    MIN_PRICE_THRESHOLD = 10_000
    MIN_AVG_VOLUME = 100_000
    AVG_VOLUME_WINDOW = 42 # CANNOT CHANGE AFTER LOADING

    # Entry/Exit Signals
    ATR_WINDOW = 42 # CANNOT CHANGE AFTER LOADING
    ATR_MULTIPLIER = 10

    # Position Sizing & Risk Management
    VOLATILITY_WINDOW = 42 # CANNOT CHANGE AFTER LOADING
    TARGET_VOLATILITY = 0.30
    MIN_ASSUMED_HOLDINGS = 30
    MAX_LEVERAGE = 1.0

    # --- NÂNG CẤP: Turnover Control ---
    USE_TURNOVER_CONTROL = True # Bật/tắt cơ chế kiểm soát
    # Ngưỡng thay đổi trọng số tối thiểu để tái cân bằng
    REBALANCE_THRESHOLD = 0.0015

    # Transaction Costs
    COMMISSION_RATE = 0.0015
    SELL_TAX_RATE = 0.001
    SLIPPAGE_RATE = 0.0005
    
    # Initial Capital
    INITIAL_CAPITAL = 100_000_000

# ==============================================================================
# BƯỚC 2: CHUẨN BỊ VÀ XỬ LÝ DỮ LIỆU (Không đổi)
# ==============================================================================
def calculate_indicators(df, config):
    df = df.sort_values('time').reset_index(drop=True)
    df['ath'] = df['close'].cummax()
    df['prev_close'] = df['close'].shift(1)
    df['prev_close_safe'] = df['prev_close'].fillna(df['close'])
    df.loc[df['prev_close_safe'] == 0, 'prev_close_safe'] = df['close']
    tr1 = df['high'] - df['low']
    tr2 = abs(df['high'] - df['prev_close_safe'])
    tr3 = abs(df['low'] - df['prev_close_safe'])
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['atr'] = tr.ewm(span=config.ATR_WINDOW, adjust=False).mean()
    ratio = df['close'] / df['prev_close_safe']
    ratio[ratio <= 0] = 1 
    df['daily_return'] = np.log(ratio)
    df['volatility'] = df['daily_return'].rolling(window=config.VOLATILITY_WINDOW).std() * np.sqrt(252)
    df['vnd_volume'] = df['volume'] * df['close']
    df['avg_volume'] = df['volume'].rolling(window=config.AVG_VOLUME_WINDOW).mean()
    df['avg_vnd_volume'] = df['vnd_volume'].rolling(window=config.AVG_VOLUME_WINDOW).mean()
    return df.drop(columns=['prev_close', 'daily_return', 'vnd_volume', 'prev_close_safe'])

def load_and_prepare_data(data_path, config):
    all_files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    all_data = []
    print("Bắt đầu đọc và xử lý dữ liệu...")
    for filename in tqdm(all_files, desc="Đang xử lý các mã CP"):
        ticker = filename.split('.')[0]
        filepath = os.path.join(data_path, filename)
        try:
            df = pd.read_csv(filepath)
            df.columns = df.columns.str.lower()
            df = df[['time', 'open', 'high', 'low', 'close', 'volume']]
            df['time'] = pd.to_datetime(df['time'])
            df['ticker'] = ticker
            price_cols = ['open', 'high', 'low', 'close']
            for col in price_cols:
                df[col] = df[col] * 1000.0
            df_with_indicators = calculate_indicators(df, config)
            all_data.append(df_with_indicators)
        except Exception as e:
            print(f"Lỗi khi xử lý file {filename}: {e}")
    full_df = pd.concat(all_data, ignore_index=True)
    full_df = full_df.sort_values(by=['time', 'ticker']).reset_index(drop=True)
    print(f"\nXử lý dữ liệu hoàn tất. Tổng cộng {full_df['ticker'].nunique()} mã cổ phiếu.")
    print(f"Dữ liệu từ {full_df['time'].min().date()} đến {full_df['time'].max().date()}.")
    return full_df.set_index(['time', 'ticker'])

# ==============================================================================
# BƯỚC 3: CLASS QUẢN LÝ DANH MỤC (CẬP NHẬT)
# ==============================================================================
class Portfolio:
    def __init__(self, config):
        self.config = config
        self.cash = config.INITIAL_CAPITAL
        self.holdings = {}  # { 'FPT': {'quantity': 100, 'entry_price': 120000}, ... }
        self.stop_losses = {} # { 'FPT': 112100, ... }
        self.history = []

    def get_stock_value(self, current_prices):
        stock_value = 0
        for ticker, position in self.holdings.items():
            price = current_prices.get(ticker, 0)
            stock_value += position['quantity'] * price
        return stock_value

    def get_total_value(self, current_prices):
        return self.cash + self.get_stock_value(current_prices)

    def record_nav(self, date, current_prices):
        nav = self.get_total_value(current_prices)
        stock_val = self.get_stock_value(current_prices)
        exposure = (stock_val / nav) if nav > 0 else 0
        self.history.append({'date': date, 'nav': nav, 'cash': self.cash, 'exposure': exposure, 'holdings_count': len(self.holdings)})

    def execute_buy(self, ticker, price, quantity, sl_data=None):
        cost = price * quantity * (1 + self.config.COMMISSION_RATE + self.config.SLIPPAGE_RATE)
        if self.cash < cost:
            print(f"  > [WARNING] Không đủ tiền mặt để mua {quantity} {ticker}.")
            return False
        self.cash -= cost
        if ticker in self.holdings:
            # Mua thêm (tái cân bằng)
            total_quantity = self.holdings[ticker]['quantity'] + quantity
            total_cost_old = self.holdings[ticker]['entry_price'] * self.holdings[ticker]['quantity']
            self.holdings[ticker]['entry_price'] = (total_cost_old + price * quantity) / total_quantity
            self.holdings[ticker]['quantity'] = total_quantity
            print(f"  > MUA THÊM {quantity} {ticker} @ {price:,.0f} VND")
        else:
            # Mua mới
            self.holdings[ticker] = {'quantity': quantity, 'entry_price': price}
            print(f"  > MUA MỚI {quantity} {ticker} @ {price:,.0f} VND")
            if sl_data:
                sl_ath, sl_atr, sl_close = sl_data['ath'], sl_data['atr'], sl_data['close']
                if sl_close > 0 and pd.notna(sl_atr):
                    discount_factor = (1 - sl_atr / sl_close) ** self.config.ATR_MULTIPLIER
                    self.stop_losses[ticker] = sl_ath * discount_factor
                else:
                    self.stop_losses[ticker] = price * 0.93
                    print(f"  > [WARNING] Dữ liệu ATR/Close không hợp lệ cho {ticker}. Đặt SL mặc định.")
        return True

    def execute_sell(self, ticker, price, quantity):
        if ticker in self.holdings and self.holdings[ticker]['quantity'] >= quantity:
            revenue = price * quantity * (1 - self.config.COMMISSION_RATE - self.config.SELL_TAX_RATE - self.config.SLIPPAGE_RATE)
            self.cash += revenue
            self.holdings[ticker]['quantity'] -= quantity
            
            action = "BÁN HẾT" if self.holdings[ticker]['quantity'] == 0 else "BÁN BỚT"
            print(f"  > {action} {quantity} {ticker} @ {price:,.0f} VND")
            
            if self.holdings[ticker]['quantity'] == 0:
                del self.holdings[ticker]
                del self.stop_losses[ticker]
        else:
            print(f"  > [WARNING] Lỗi: Bán {ticker} với số lượng không hợp lệ.")

# ==============================================================================
# BƯỚC 4: LOGIC CHÍNH CỦA BACKTEST (PHIÊN BẢN SỬA LỖI)
# ==============================================================================
def run_backtest_(data, config, from_date=None):
    portfolio = Portfolio(config)
    all_dates = data.index.get_level_values('time').unique().sort_values()

    if from_date:
        try:
            start_date = pd.to_datetime(from_date)
            # Lọc để chỉ lấy các ngày lớn hơn hoặc bằng ngày bắt đầu
            all_dates = all_dates[all_dates >= start_date]
            if len(all_dates) == 0:
                print(f"Không có dữ liệu nào từ ngày {from_date} trở đi. Dừng backtest.")
                return pd.DataFrame() # Trả về DataFrame rỗng
            print(f"Backtest sẽ bắt đầu từ ngày: {all_dates[0].date()}")
        except Exception as e:
            print(f"Lỗi định dạng ngày '{from_date}'. Vui lòng dùng 'YYYY-MM-DD'. Lỗi: {e}")
            print("Backtest sẽ chạy trên toàn bộ dữ liệu.")
    
    # --- THAY ĐỔI 1: Tách biệt trade_list và sl_data_list ---
    trade_list = {} # {'FPT': 100 (mua), 'VNM': -50 (bán)}
    sl_data_list = {} # {'FPT': {'ath': ..., 'atr': ..., 'close': ...}}

    print("\nBắt đầu quá trình backtest...")
    for i in tqdm(range(len(all_dates)), desc="Đang mô phỏng giao dịch"):
        today = all_dates[i]
        
        # Dữ liệu giá mở cửa của hôm nay để thực thi lệnh
        try:
            daily_open_prices = data.loc[today, 'open'].to_dict()
        except KeyError:
            # Nếu chỉ có 1 mã trong ngày, nó không trả về dict
            df_temp = data.loc[today]
            if isinstance(df_temp, pd.Series):
                 daily_open_prices = {df_temp.name: df_temp['open']}
            else: # DataFrame
                 daily_open_prices = df_temp['open'].to_dict()
        
        # --- 1. (Đầu ngày) THỰC THI GIAO DỊCH ĐÃ QUYẾT ĐỊNH TỪ HÔM TRƯỚC ---
        # --- THAY ĐỔI 2: Sắp xếp chỉ trên trade_list, không còn lỗi ---
        sorted_trades = sorted(trade_list.items(), key=lambda item: item[1]) 
        
        for ticker, quantity_delta in sorted_trades:
            if ticker in daily_open_prices:
                price = daily_open_prices[ticker]
                if quantity_delta < 0: # Lệnh bán
                    portfolio.execute_sell(ticker, price, abs(quantity_delta))
                elif quantity_delta > 0: # Lệnh mua
                    # Lấy dữ liệu SL từ danh sách riêng
                    sl_data_for_buy = sl_data_list.get(ticker)
                    portfolio.execute_buy(ticker, price, quantity_delta, sl_data=sl_data_for_buy)
            else:
                print(f'[WARNING] Mã {ticker} (quyết định mua | bán): không tìm thấy trong thông tin giá của ngày hiện tại.')

        # --- 2. (Cuối ngày) KẾT THÚC NGÀY GIAO DỊCH & GHI NHẬN NAV ---
        # Xử lý trường hợp chỉ có 1 mã trong ngày
        try:
            daily_close_prices = data.loc[today, 'close'].to_dict()
        except KeyError:
             df_temp = data.loc[today]
             if isinstance(df_temp, pd.Series):
                 daily_close_prices = {df_temp.name: df_temp['close']}
             else:
                 daily_close_prices = df_temp['close'].to_dict()

        portfolio.record_nav(today, daily_close_prices)
        nav_eod = portfolio.get_total_value(daily_close_prices)
        
        if i % 100 == 0:
            print(f"\n--- Ngày: {today.date()} ---")
            print(f"NAV: {nav_eod:,.0f} VND | Tiền mặt: {portfolio.cash:,.0f} VND | CP: {len(portfolio.holdings)}")

        if nav_eod <= 0:
            print("NAV âm! Dừng backtest.")
            break

        # --- 3. (Cuối ngày) RA QUYẾT ĐỊNH CHO NGÀY MAI ---
        trade_list.clear()
        sl_data_list.clear()
        
        daily_data_today = data.loc[today]

        # A, B, C, D, E... (Các bước tính toán không đổi)
        # ... (giữ nguyên phần code tính toán sell_due_to_sl, new_signals, target_portfolio_tickers, target_weights) ...
        # A. Xác định các mã cần bán do Stop-Loss
        sell_due_to_sl = set()
        for ticker, position in list(portfolio.holdings.items()):
            if ticker in daily_data_today.index:
                if daily_data_today.loc[ticker, 'close'] < portfolio.stop_losses.get(ticker, float('inf')):
                    sell_due_to_sl.add(ticker)
            else:
                print(f'[WARNING] Mã {ticker} (holding): không tìm thấy trong thông tin giá của ngày hiện tại.')
        
        # B. Xác định các tín hiệu mua mới
        eligible = daily_data_today[
            (daily_data_today['close'] > config.MIN_PRICE_THRESHOLD) &
            (daily_data_today['avg_volume'] > config.MIN_AVG_VOLUME) &
            (daily_data_today['volatility'] > 0)
        ]
        new_signals = eligible[
            (eligible['close'] >= eligible['ath']) &
            (~eligible.index.isin(portfolio.holdings.keys()))
        ].index.tolist()

        # C. Xây dựng danh mục mục tiêu cho ngày mai
        current_holdings_to_keep = [t for t in portfolio.holdings.keys() if t not in sell_due_to_sl]
        target_portfolio_tickers = sorted(list(set(current_holdings_to_keep + new_signals)))

        if not target_portfolio_tickers:
            for ticker, pos in portfolio.holdings.items():
                trade_list[ticker] = -pos['quantity']
            continue
            
        # D. Tính toán trọng số lý tưởng cho danh mục mục tiêu
        n_holdings = len(target_portfolio_tickers)
        target_weights = {}
        total_weight = 0
        for ticker in target_portfolio_tickers:
            if ticker in daily_data_today.index:
                vol = daily_data_today.loc[ticker, 'volatility']
                if pd.notna(vol) and vol > 0:
                    weight = (config.TARGET_VOLATILITY / vol) * (1 / max(config.MIN_ASSUMED_HOLDINGS, n_holdings))
                    target_weights[ticker] = weight
                    total_weight += weight
                else:
                    print(f'[INFO] Mã {ticker} có volatility trong n ngày không hợp lệ.')
            else:
                if ticker not in new_signals:
                    print(f'[WARNING] Mã {ticker} (tín hiệu mua): không tìm thấy trong thông tin giá của ngày hiện tại.')

        # E. Điều chỉnh trọng số theo đòn bẩy tối đa
        if total_weight > config.MAX_LEVERAGE:
            correction_factor = config.MAX_LEVERAGE / total_weight
            target_weights = {t: w * correction_factor for t, w in target_weights.items()}

        # F. Tạo danh sách giao dịch delta
        for ticker in set(list(portfolio.holdings.keys()) + target_portfolio_tickers):
            current_quantity = portfolio.holdings.get(ticker, {}).get('quantity', 0)
            target_weight = target_weights.get(ticker, 0)
            
            # Xử lý trường hợp chỉ có 1 mã trong ngày (daily_data_today là Series)
            if isinstance(daily_data_today, pd.Series):
                 if ticker == daily_data_today.name:
                      estimated_price = daily_data_today['close']
                 else: # Mã này không có dữ liệu hôm nay
                      estimated_price = 0
            else: # DataFrame
                 estimated_price = daily_data_today.loc[ticker, 'close'] if ticker in daily_data_today.index else 0
            
            target_quantity = 0
            if estimated_price > 0:
                target_quantity = int((target_weight * nav_eod) / estimated_price)
            
            quantity_delta = target_quantity - current_quantity

            # G. ÁP DỤNG TURNOVER CONTROL
            if config.USE_TURNOVER_CONTROL:
                trade_value = abs(quantity_delta) * estimated_price if estimated_price > 0 else 0
                
                # Bỏ qua giao dịch nếu sự thay đổi trọng số quá nhỏ
                # Logic này áp dụng cho tái cân bằng, không áp dụng cho mua mới hoàn toàn
                if ticker in portfolio.holdings: # Chỉ check cho mã đang có
                    weight_change_threshold = config.REBALANCE_THRESHOLD * nav_eod
                    if trade_value < weight_change_threshold:
                        print(f'[INFO] Bỏ qua tái cân bằng mã {ticker}: {trade_value} < {weight_change_threshold}.')
                        continue

            if quantity_delta != 0:
                trade_list[ticker] = quantity_delta
                # --- THAY ĐỔI 3: Lưu SL data vào danh sách riêng ---
                if ticker in new_signals and quantity_delta > 0:
                    if isinstance(daily_data_today, pd.Series) and ticker == daily_data_today.name:
                        sl_data_list[ticker] = daily_data_today[['ath', 'atr', 'close']].to_dict()
                    elif isinstance(daily_data_today, pd.DataFrame):
                        sl_data_list[ticker] = daily_data_today.loc[ticker, ['ath', 'atr', 'close']].to_dict()


        # H. Cập nhật trailing stop-loss
        for ticker in current_holdings_to_keep:
            if isinstance(daily_data_today, pd.Series) and ticker == daily_data_today.name:
                data_row = daily_data_today
            elif isinstance(daily_data_today, pd.DataFrame) and ticker in daily_data_today.index:
                data_row = daily_data_today.loc[ticker]
            else:
                continue
                
            if data_row['close'] > 0 and pd.notna(data_row['atr']):
                new_sl_candidate = data_row['ath'] * ((1 - data_row['atr'] / data_row['close']) ** config.ATR_MULTIPLIER)
                if new_sl_candidate > portfolio.stop_losses.get(ticker, 0):
                    portfolio.stop_losses[ticker] = new_sl_candidate

    return pd.DataFrame(portfolio.history).set_index('date')

def run_backtest(data, config, from_date=None, end_date=None, log_file="backtest_log.txt"):
    portfolio = Portfolio(config)
    all_dates = data.index.get_level_values('time').unique().sort_values()

    with open(log_file, "w", encoding="utf-8") as log:

        if from_date:
            try:
                if from_date:
                    start_date = pd.to_datetime(from_date)
                    all_dates = all_dates[all_dates >= start_date]

                if end_date:
                    end_date = pd.to_datetime(end_date)
                    all_dates = all_dates[all_dates <= end_date]

                if len(all_dates) == 0:
                    log.write(f"Không có dữ liệu nào trong khoảng từ {from_date} đến {end_date}. Dừng backtest.\n")
                    return pd.DataFrame()

                log.write(f"Backtest sẽ chạy từ ngày: {all_dates[0].date()} đến {all_dates[-1].date()}\n")

            except Exception as e:
                log.write(f"Lỗi định dạng ngày. Vui lòng dùng 'YYYY-MM-DD'. Lỗi: {e}\n")
                log.write("Backtest sẽ chạy trên toàn bộ dữ liệu.\n")

        # --- THAY ĐỔI 1: Tách biệt trade_list và sl_data_list ---
        trade_list = {}
        sl_data_list = {}

        log.write("\nBắt đầu quá trình backtest...\n")
        for i in tqdm(range(len(all_dates)), desc="Đang mô phỏng giao dịch"):
            today = all_dates[i]

            try:
                daily_open_prices = data.loc[today, 'open'].to_dict()
            except KeyError:
                df_temp = data.loc[today]
                if isinstance(df_temp, pd.Series):
                    daily_open_prices = {df_temp.name: df_temp['open']}
                else:
                    daily_open_prices = df_temp['open'].to_dict()

            sorted_trades = sorted(trade_list.items(), key=lambda item: item[1]) 

            for ticker, quantity_delta in sorted_trades:
                if ticker in daily_open_prices:
                    price = daily_open_prices[ticker]
                    if quantity_delta < 0:
                        portfolio.execute_sell(ticker, price, abs(quantity_delta))
                    elif quantity_delta > 0:
                        sl_data_for_buy = sl_data_list.get(ticker)
                        portfolio.execute_buy(ticker, price, quantity_delta, sl_data=sl_data_for_buy)
                else:
                    log.write(f"[WARNING] Mã {ticker} (quyết định mua | bán): không tìm thấy trong thông tin giá của ngày hiện tại.\n")

            try:
                daily_close_prices = data.loc[today, 'close'].to_dict()
            except KeyError:
                df_temp = data.loc[today]
                if isinstance(df_temp, pd.Series):
                    daily_close_prices = {df_temp.name: df_temp['close']}
                else:
                    daily_close_prices = df_temp['close'].to_dict()

            portfolio.record_nav(today, daily_close_prices)
            nav_eod = portfolio.get_total_value(daily_close_prices)

            if i % 100 == 0:
                log.write(f"\n--- Ngày: {today.date()} ---\n")
                log.write(f"NAV: {nav_eod:,.0f} VND | Tiền mặt: {portfolio.cash:,.0f} VND | CP: {len(portfolio.holdings)}\n")

            if nav_eod <= 0:
                log.write("NAV âm! Dừng backtest.\n")
                break

            trade_list.clear()
            sl_data_list.clear()
            
            daily_data_today = data.loc[today]

            sell_due_to_sl = set()
            for ticker, position in list(portfolio.holdings.items()):
                if ticker in daily_data_today.index:
                    if daily_data_today.loc[ticker, 'close'] < portfolio.stop_losses.get(ticker, float('inf')):
                        sell_due_to_sl.add(ticker)
                else:
                    log.write(f"[WARNING] Mã {ticker} (holding): không tìm thấy trong thông tin giá của ngày hiện tại.\n")

            eligible = daily_data_today[
                (daily_data_today['close'] > config.MIN_PRICE_THRESHOLD) &
                (daily_data_today['avg_volume'] > config.MIN_AVG_VOLUME) &
                (daily_data_today['volatility'] > 0)
            ]
            new_signals = eligible[
                (eligible['close'] >= eligible['ath']) &
                (~eligible.index.isin(portfolio.holdings.keys()))
            ].index.tolist()

            current_holdings_to_keep = [t for t in portfolio.holdings.keys() if t not in sell_due_to_sl]
            target_portfolio_tickers = sorted(list(set(current_holdings_to_keep + new_signals)))

            if not target_portfolio_tickers:
                for ticker, pos in portfolio.holdings.items():
                    trade_list[ticker] = -pos['quantity']
                continue

            n_holdings = len(target_portfolio_tickers)
            target_weights = {}
            total_weight = 0
            for ticker in target_portfolio_tickers:
                if ticker in daily_data_today.index:
                    vol = daily_data_today.loc[ticker, 'volatility']
                    if pd.notna(vol) and vol > 0:
                        weight = (config.TARGET_VOLATILITY / vol) * (1 / max(config.MIN_ASSUMED_HOLDINGS, n_holdings))
                        target_weights[ticker] = weight
                        total_weight += weight
                    else:
                        log.write(f"[INFO] Mã {ticker} có volatility trong n ngày không hợp lệ.\n")
                else:
                    if ticker not in new_signals:
                        log.write(f"[WARNING] Mã {ticker} (tín hiệu mua): không tìm thấy trong thông tin giá của ngày hiện tại.\n")

            if total_weight > config.MAX_LEVERAGE:
                correction_factor = config.MAX_LEVERAGE / total_weight
                target_weights = {t: w * correction_factor for t, w in target_weights.items()}

            for ticker in set(list(portfolio.holdings.keys()) + target_portfolio_tickers):
                current_quantity = portfolio.holdings.get(ticker, {}).get('quantity', 0)
                target_weight = target_weights.get(ticker, 0)

                if isinstance(daily_data_today, pd.Series):
                    if ticker == daily_data_today.name:
                        estimated_price = daily_data_today['close']
                    else:
                        estimated_price = 0
                else:
                    estimated_price = daily_data_today.loc[ticker, 'close'] if ticker in daily_data_today.index else 0

                target_quantity = 0
                if estimated_price > 0:
                    target_quantity = int((target_weight * nav_eod) / estimated_price)

                quantity_delta = target_quantity - current_quantity

                if config.USE_TURNOVER_CONTROL:
                    trade_value = abs(quantity_delta) * estimated_price if estimated_price > 0 else 0
                    if ticker in portfolio.holdings:
                        weight_change_threshold = config.REBALANCE_THRESHOLD * nav_eod
                        if trade_value < weight_change_threshold:
                            # log.write(f"[INFO] Bỏ qua tái cân bằng mã {ticker}: {trade_value} < {weight_change_threshold}.\n")
                            continue

                if quantity_delta != 0:
                    trade_list[ticker] = quantity_delta
                    if ticker in new_signals and quantity_delta > 0:
                        if isinstance(daily_data_today, pd.Series) and ticker == daily_data_today.name:
                            sl_data_list[ticker] = daily_data_today[['ath', 'atr', 'close']].to_dict()
                        elif isinstance(daily_data_today, pd.DataFrame):
                            sl_data_list[ticker] = daily_data_today.loc[ticker, ['ath', 'atr', 'close']].to_dict()

            for ticker in current_holdings_to_keep:
                if isinstance(daily_data_today, pd.Series) and ticker == daily_data_today.name:
                    data_row = daily_data_today
                elif isinstance(daily_data_today, pd.DataFrame) and ticker in daily_data_today.index:
                    data_row = daily_data_today.loc[ticker]
                else:
                    continue

                if data_row['close'] > 0 and pd.notna(data_row['atr']):
                    new_sl_candidate = data_row['ath'] * ((1 - data_row['atr'] / data_row['close']) ** config.ATR_MULTIPLIER)
                    if new_sl_candidate > portfolio.stop_losses.get(ticker, 0):
                        portfolio.stop_losses[ticker] = new_sl_candidate

    return pd.DataFrame(portfolio.history).set_index('date')

#%%
# ==============================================================================
# BƯỚC 5: CHẠY CHƯƠNG TRÌNH (Không đổi)
# ==============================================================================
DATA_PATH = '/mnt/c/Users/HOME/Downloads/TF-algo-trading/processed_stock_history_backup'
config = StrategyConfig()

full_data = load_and_prepare_data(DATA_PATH, config)

#%%
config = StrategyConfig()
config.USE_TURNOVER_CONTROL = True
config.REBALANCE_THRESHOLD = 0.003
config.MIN_ASSUMED_HOLDINGS = 20
# config.MAX_LEVERAGE = 2
# config.MIN_AVG_VOLUME = 50_000
from_date=None
end_date=None
# from_date="2016-01-01"
# end_date="2025-03-31"
results = run_backtest(full_data, config, log_file='/mnt/c/Users/HOME/Downloads/TF-algo-trading/backtest.log', from_date=from_date, end_date=end_date)

#%%
print("\n--- KẾT QUẢ BACKTEST ---")
print(results.tail())

final_nav = results['nav'].iloc[-1]
initial_nav = config.INITIAL_CAPITAL
years = (results.index.max() - results.index.min()).days / 365.25
cagr = ((final_nav / initial_nav) ** (1 / years)) - 1 if years > 0 and initial_nav > 0 else 0
max_drawdown = (1 - results['nav'] / results['nav'].cummax()).max()
avg_exposure = results['exposure'].mean()
avg_holdings = results['holdings_count'].mean()

print(f"\n--- THỐNG KÊ HIỆU SUẤT ---")
print(f"Vốn ban đầu:      {initial_nav:,.0f} VND")
print(f"NAV cuối kỳ:       {final_nav:,.0f} VND")
print(f"Thời gian:         {years:.2f} năm")
print(f"CAGR:              {cagr:.2%}")
print(f"Max Drawdown:      {max_drawdown:.2%}")
print(f"Exposure TB:       {avg_exposure:.2%}")
print(f"Số lượng CP TB:    {avg_holdings:.1f}")

try:
    import matplotlib.pyplot as plt
    fig, ax1 = plt.subplots(figsize=(15, 8))
    ax1.plot(results.index, results['nav'], color='blue', label='NAV')
    ax1.set_xlabel('Thời gian')
    ax1.set_ylabel('Tổng giá trị tài sản (NAV)', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.set_yscale('log')
    ax1.set_title('Hiệu suất Chiến lược Trend Following (có Rebalancing)')
    
    ax2 = ax1.twinx()
    ax2.plot(results.index, results['exposure'] * 100, color='red', alpha=0.5, linestyle='--', label='Exposure (%)')
    ax2.set_ylabel('Mức độ tiếp xúc (%)', color='red')
    ax2.tick_params(axis='y', labelcolor='red')
    ax2.axhline(100, color='grey', linestyle=':', linewidth=1)
    
    fig.tight_layout()
    plt.show()
except ImportError:
    print("\nVui lòng cài đặt matplotlib (`pip install matplotlib`) để vẽ biểu đồ.")
# %%
