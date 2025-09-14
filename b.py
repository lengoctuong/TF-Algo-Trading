# ==============================================================================
# BƯỚC 4: LOGIC CHÍNH CỦA BACKTEST (PHIÊN BẢN SỬA LỖI SL)
# ==============================================================================
def run_backtest(data, config):
    portfolio = Portfolio(config)
    all_dates = data.index.get_level_values('time').unique().sort_values()

    # Khởi tạo các danh sách quyết định giao dịch
    buy_list = {}  # { 'ticker': {'quantity': Q, 'sl_data':{...}} }
    sell_list = {} # { 'ticker': quantity }

    print("\nBắt đầu quá trình backtest...")
    for i in tqdm(range(len(all_dates)), desc="Đang mô phỏng giao dịch"):
        today = all_dates[i]
        daily_data_today = data.loc[today]

        # --- 1. (Đầu ngày) THỰC THI GIAO DỊCH ĐÃ QUYẾT ĐỊNH TỪ HÔM TRƯỚC ---
        # Bán trước
        for ticker, quantity in sell_list.items():
            if ticker in daily_data_today.index:
                sell_price = daily_data_today.loc[ticker, 'open']
                portfolio.execute_sell(ticker, sell_price, quantity)
            else:
                print(f'[WARNING] Mã {ticker} (quyết định bán): không tìm thấy trong thông tin giá của ngày hiện tại.')
        
        # Mua sau
        for ticker, order_details in buy_list.items():
            if ticker in daily_data_today.index:
                buy_price = daily_data_today.loc[ticker, 'open']
                quantity = order_details['quantity']
                sl_data = order_details['sl_data']
                portfolio.execute_buy(ticker, buy_price, quantity, sl_data=sl_data)
            else:
                print(f'[WARNING] Mã {ticker} (quyết định mua): không tìm thấy trong thông tin giá của ngày hiện tại.')
        
        # --- 2. (Cuối ngày) KẾT THÚC NGÀY GIAO DỊCH & GHI NHẬN NAV ---
        current_prices = daily_data_today['close'].to_dict()
        portfolio.record_nav(today, current_prices)
        nav_eod = portfolio.get_total_value(current_prices)
        
        if i % 100 == 0:
            print(f"\n--- Ngày: {today.date()} ---")
            print(f"NAV: {nav_eod:,.0f} VND | Tiền mặt: {portfolio.cash:,.0f} VND | CP: {len(portfolio.holdings)}")

        # --- 3. (Cuối ngày) RA QUYẾT ĐỊNH CHO NGÀY MAI ---
        buy_list.clear()
        sell_list.clear()
        
        # A. Quyết định bán (Stop-Loss)
        for ticker, position in list(portfolio.holdings.items()):
            if ticker in daily_data_today.index:
                if daily_data_today.loc[ticker, 'close'] < portfolio.stop_losses['ticker']:
                    sell_list[ticker] = position['quantity']
            else:
                print(f'[WARNING] Mã {ticker} (holding): không tìm thấy trong thông tin giá của ngày hiện tại.')

        # B. Quyết định mua mới
        # 1. Lọc vũ trụ & tìm tín hiệu
        eligible = daily_data_today[
            (daily_data_today['close'] > config.MIN_PRICE_THRESHOLD) &
            (daily_data_today['avg_volume'] > config.MIN_AVG_VOLUME) &
            (daily_data_today['volatility'] > 0)
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
            if ticker in daily_data_today.index:
                vol = daily_data_today.loc[ticker, 'volatility']
                if pd.notna(vol) and vol > 0:
                    weight = (config.TARGET_VOLATILITY / vol) * (1 / max(config.MIN_ASSUMED_HOLDINGS, n_holdings))
                    weights[ticker] = weight
                    total_weight += weight
                else:
                    print(f'[INFO] Mã {ticker} có volatility trong n ngày không hợp lệ.')
            else:
                if ticker not in new_signals.index:
                    print(f'[WARNING] Mã {ticker} (tín hiệu mua): không tìm thấy trong thông tin giá của ngày hiện tại.')

        if total_weight > config.MAX_LEVERAGE:
            weights = {t: w * (config.MAX_LEVERAGE / total_weight) for t, w in weights.items()}

        # 3. Tạo lệnh mua và gói kèm dữ liệu SL
        for ticker in new_signals.index:
            if ticker in weights:
                estimated_price = daily_data_today.loc[ticker, 'close']
                target_value = weights[ticker] * nav_eod
                quantity = int(target_value / estimated_price)
                
                if quantity > 0:
                    # Gói dữ liệu cần thiết để tính SL vào lệnh mua
                    sl_data_package = {
                        'ath': daily_data_today.loc[ticker, 'ath'],
                        'atr': daily_data_today.loc[ticker, 'atr'],
                        'close': daily_data_today.loc[ticker, 'close']
                    }
                    buy_list[ticker] = {
                        'quantity': quantity,
                        'sl_data': sl_data_package
                    }
        
        # C. Cập nhật trailing stop-loss cho các vị thế hiện tại
        for ticker in current_holdings:
            if ticker in daily_data_today.index:
                data_row = daily_data_today.loc[ticker]
                new_sl_candidate = data_row['ath'] * ((1 - data_row['atr'] / data_row['close']) ** config.ATR_MULTIPLIER)
                
                if new_sl_candidate > portfolio.stop_losses.get(ticker, 0):
                    portfolio.stop_losses[ticker] = new_sl_candidate

    return pd.DataFrame(portfolio.history).set_index('date')