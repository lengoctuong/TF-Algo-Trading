def run_backtest(data, config, from_date=None, log_file="backtest_log.txt"):
    portfolio = Portfolio(config)
    all_dates = data.index.get_level_values('time').unique().sort_values()

    with open(log_file, "w", encoding="utf-8") as log:

        if from_date:
            try:
                start_date = pd.to_datetime(from_date)
                # Lọc để chỉ lấy các ngày lớn hơn hoặc bằng ngày bắt đầu
                all_dates = all_dates[all_dates >= start_date]
                if len(all_dates) == 0:
                    log.write(f"Không có dữ liệu nào từ ngày {from_date} trở đi. Dừng backtest.\n")
                    return pd.DataFrame()  # Trả về DataFrame rỗng
                log.write(f"Backtest sẽ bắt đầu từ ngày: {all_dates[0].date()}\n")
            except Exception as e:
                log.write(f"Lỗi định dạng ngày '{from_date}'. Vui lòng dùng 'YYYY-MM-DD'. Lỗi: {e}\n")
                log.write("Backtest sẽ chạy trên toàn bộ dữ liệu.\n")

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

            # --- 1. (Đầu ngày) THỰC THI GIAO DỊCH ĐÃ QUYẾT ĐỊNH TỪ HÔM TRƯỚC ---
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

            # --- 2. (Cuối ngày) KẾT THÚC NGÀY GIAO DỊCH & GHI NHẬN NAV ---
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

            # --- 3. (Cuối ngày) RA QUYẾT ĐỊNH CHO NGÀY MAI ---
            trade_list.clear()
            sl_data_list.clear()
            
            daily_data_today = data.loc[today]

            # A. Xác định các mã cần bán do Stop-Loss
            sell_due_to_sl = set()
            for ticker, position in list(portfolio.holdings.items()):
                if ticker in daily_data_today.index:
                    if daily_data_today.loc[ticker, 'close'] < portfolio.stop_losses.get(ticker, float('inf')):
                        sell_due_to_sl.add(ticker)
                else:
                    log.write(f"[WARNING] Mã {ticker} (holding): không tìm thấy trong thông tin giá của ngày hiện tại.\n")

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
                        log.write(f"[INFO] Mã {ticker} có volatility trong n ngày không hợp lệ.\n")
                else:
                    if ticker not in new_signals:
                        log.write(f"[WARNING] Mã {ticker} (tín hiệu mua): không tìm thấy trong thông tin giá của ngày hiện tại.\n")

            # E. Điều chỉnh trọng số theo đòn bẩy tối đa
            if total_weight > config.MAX_LEVERAGE:
                correction_factor = config.MAX_LEVERAGE / total_weight
                target_weights = {t: w * correction_factor for t, w in target_weights.items()}

            # F. Tạo danh sách giao dịch delta
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

                # G. ÁP DỤNG TURNOVER CONTROL
                if config.USE_TURNOVER_CONTROL:
                    trade_value = abs(quantity_delta) * estimated_price if estimated_price > 0 else 0
                    if ticker in portfolio.holdings:
                        weight_change_threshold = config.REBALANCE_THRESHOLD * nav_eod
                        if trade_value < weight_change_threshold:
                            log.write(f"[INFO] Bỏ qua tái cân bằng mã {ticker}: {trade_value} < {weight_change_threshold}.\n")
                            continue

                if quantity_delta != 0:
                    trade_list[ticker] = quantity_delta
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
