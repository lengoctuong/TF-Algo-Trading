# ==============================================================================
# BƯỚC 4: LOGIC CHÍNH CỦA BACKTEST (PHIÊN BẢN ĐÃ SỬA LỖI THỜI GIAN)
# ==============================================================================
def run_backtest(data, config):
    portfolio = Portfolio(config)
    all_dates = data.index.get_level_values('time').unique().sort_values()

    # Khởi tạo các danh sách quyết định giao dịch
    buy_list = {}  # { 'ticker': quantity }
    sell_list = {} # { 'ticker': quantity }

    print("\nBắt đầu quá trình backtest...")
    for i in tqdm(range(len(all_dates)), desc="Đang mô phỏng giao dịch"):
        today = all_dates[i]
        
        # Lấy dữ liệu của ngày hôm nay
        daily_data = data.loc[today]
        
        # --- 1. (Đầu ngày) THỰC THI GIAO DỊCH ĐÃ QUYẾT ĐỊNH TỪ HÔM TRƯỚC ---
        # Bán trước để giải phóng tiền mặt
        for ticker, quantity in sell_list.items():
            if ticker in daily_data.index:
                sell_price = daily_data.loc[ticker, 'open']
                portfolio.execute_sell(ticker, sell_price, quantity)
            else:
                print(f'[WARNING] Mã {ticker} (quyết định bán): không tìm thấy trong thông tin giá của ngày hiện tại.')
        
        # Mua sau
        for ticker, quantity in buy_list.items():
            if ticker in daily_data.index:
                buy_price = daily_data.loc[ticker, 'open']
                portfolio.execute_buy(ticker, buy_price, quantity)
            else:
                print(f'[WARNING] Mã {ticker} (quyết định mua): không tìm thấy trong thông tin giá của ngày hiện tại.')
        
        # --- 2. (Cuối ngày) KẾT THÚC NGÀY GIAO DỊCH & GHI NHẬN NAV ---
        # Lấy giá đóng cửa của ngày hôm nay để tính NAV
        current_prices = daily_data['close'].to_dict()
        portfolio.record_nav(today, current_prices)
        nav_eod = portfolio.get_total_value(current_prices) # NAV cuối ngày (End-of-Day)
        
        # In log
        if i % 100 == 0: # In log định kỳ để tránh quá nhiều output
            print(f"\n--- Ngày: {today.date()} ---")
            print(f"NAV: {nav_eod:,.0f} VND | Tiền mặt: {portfolio.cash:,.0f} VND | CP: {len(portfolio.holdings)}")

        # --- 3. (Cuối ngày) RA QUYẾT ĐỊNH CHO NGÀY MAI ---
        # Reset danh sách quyết định cho ngày mai
        buy_list.clear()
        sell_list.clear()

        # A. Quyết định bán (Stop-Loss)
        for ticker, position in list(portfolio.holdings.items()):
            if ticker in daily_data.index:
                current_close = daily_data.loc[ticker, 'close']
                if current_close < portfolio.stop_losses.get(ticker, np.inf):
                    sell_list[ticker] = position['quantity'] # Bán hết
            else:
                print(f'[WARNING] Mã {ticker} (holding): không tìm thấy trong thông tin giá của ngày hiện tại.')

        # B. Quyết định mua mới
        # 1. Lọc vũ trụ & tìm tín hiệu
        eligible = daily_data[
            (daily_data['close'] > config.MIN_PRICE_THRESHOLD) &
            (daily_data['avg_volume'] > config.MIN_AVG_VOLUME) &
            (daily_data['volatility'] > 0) # Thêm kiểm tra volatility ở đây
        ]
        new_signals = eligible[
            (eligible['close'] >= eligible['ath']) &
            (~eligible.index.isin(portfolio.holdings.keys()))
        ]

        # 2. Tính toán trọng số (sử dụng NAV cuối ngày hôm nay)
        current_holdings = [t for t in portfolio.holdings.keys() if t not in sell_list]
        target_holdings = current_holdings + list(new_signals.index)
        
        if not target_holdings:
            continue
            
        n_holdings = len(target_holdings)
        weights = {}
        total_weight = 0
        for ticker in target_holdings:
            if ticker in daily_data.index:
                vol = daily_data.loc[ticker, 'volatility']
                if pd.notna(vol) and vol > 0: # Kiểm tra lại cho chắc
                    weight = (config.TARGET_VOLATILITY / vol) * (1 / max(config.MIN_ASSUMED_HOLDINGS, n_holdings))
                    weights[ticker] = weight
                    total_weight += weight
                else:
                    print(f'[INFO] Mã {ticker} có volatility trong n ngày không hợp lệ.')
            else:
                if ticker not in new_signals.index:
                    print(f'[WARNING] Mã {ticker} (tín hiệu mua): không tìm thấy trong thông tin giá của ngày hiện tại.')

        if total_weight > config.MAX_LEVERAGE:
            correction_factor = config.MAX_LEVERAGE / total_weight
            weights = {t: w * correction_factor for t, w in weights.items()}

        # 3. Tạo lệnh mua và cập nhật stop-loss
        for ticker in new_signals.index:
            if ticker in weights:
                # Dùng giá đóng cửa hôm nay để ước tính số lượng
                estimated_price = daily_data.loc[ticker, 'close']
                target_value = weights[ticker] * nav_eod
                quantity = int(target_value / estimated_price)
                
                if quantity > 0:
                    buy_list[ticker] = quantity
                    
                    # Tính và lưu stop-loss cho ngày mai
                    sl_ath = daily_data.loc[ticker, 'ath']
                    sl_atr = daily_data.loc[ticker, 'atr']
                    sl_close = daily_data.loc[ticker, 'close']
                    discount_factor = (1 - sl_atr / sl_close) ** config.ATR_MULTIPLIER
                    portfolio.stop_losses[ticker] = sl_ath * discount_factor
        
        # C. Cập nhật trailing stop-loss
        for ticker in current_holdings:
            if ticker in daily_data.index:
                new_ath = daily_data.loc[ticker, 'ath']
                new_atr = daily_data.loc[ticker, 'atr']
                new_close = daily_data.loc[ticker, 'close']
                new_sl_candidate = new_ath * ((1 - new_atr / new_close) ** config.ATR_MULTIPLIER)
                
                if new_sl_candidate > portfolio.stop_losses.get(ticker, 0):
                    portfolio.stop_losses[ticker] = new_sl_candidate

    return pd.DataFrame(portfolio.history).set_index('date')
