import os
import time
from datetime import datetime
from vnstock import Listing, Quote

def download_all_histories(
    symbols: list = [],
    start_date: str = "2000-01-01",
    end_date: str = None,
    source: str = "vci",
    folder: str = "stock_histories",
    interval: str = "1D",
    sleep_time: float = 1.0,
):
    """
    Tải lịch sử giá của tất cả cổ phiếu từ source (VCI) giữa start_date và end_date,
    lưu mỗi mã thành CSV trong folder.

    Args:
        start_date: ngày bắt đầu (YYYY-MM-DD)
        end_date: ngày kết thúc (YYYY-MM-DD). Nếu None → lấy ngày hiện tại
        source: nguồn dữ liệu, ví dụ "vci"
        folder: thư mục để lưu file CSV
        interval: khoảng thời gian ("1D", "1H", "1min", tùy hỗ trợ)
        sleep_time: chờ giữa các request để tránh rate limit
    """

    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")

    # tạo folder nếu chưa tồn tại
    os.makedirs(folder, exist_ok=True)

    if not symbols:
        # lấy danh sách tất cả mã stock
        listing = Listing(source=source)
        df_symbols = listing.all_symbols(to_df=True)  # DataFrame chứa các mã

        # giả sử cột tên mã là "symbol" hoặc "ticker" — kiểm tra nếu tên khác
        # ví dụ df_symbols có cột "symbol" hoặc "ticker"
        if "symbol" in df_symbols.columns:
            symbols = df_symbols["symbol"].tolist()
        elif "ticker" in df_symbols.columns:
            symbols = df_symbols["ticker"].tolist()
        else:
            # nếu không đúng tên cột, in ra để bạn chỉnh
            print("Warning: không tìm cột symbol/ticker trong df_symbols:", df_symbols.columns)
            symbols = df_symbols.iloc[:,0].astype(str).tolist()  # giả sử cột đầu là mã

    print(f"Tổng số mã sẽ tải: {len(symbols)}")

    for sym in symbols:
        try:
            print(f"Đang tải: {sym}")
            quote = Quote(symbol=sym, source=source)
            df_hist = quote.history(start=start_date, end=end_date, interval=interval, show_log=False)

            if df_hist is None or df_hist.empty:
                print(f"Không có dữ liệu cho mã {sym}")
            else:
                # Có thể xử lý: tên cột ngày, sắp xếp
                df_hist = df_hist.sort_values(by="time")  # nếu cột ngày là "time"
                filename = os.path.join(folder, f"{sym}.csv")
                df_hist.to_csv(filename, index=False)
                print(f"Đã lưu {filename} ({len(df_hist)} bản ghi)")

        except Exception as e:
            if "RateLimitExceed" in str(e):
                print(f"Bị rate limit khi tải mã {sym}, chờ {sleep_time*5} giây rồi thử lại.")
                time.sleep(sleep_time * 5)
                # thử lại lần nữa
                try:
                    df_hist = quote.history(start=start_date, end=end_date, interval=interval, show_log=False)
                    if df_hist is not None and not df_hist.empty:
                        filename = os.path.join(folder, f"{sym}.csv")
                        df_hist.to_csv(filename, index=False)
                        print(f"Đã lưu sau retry {filename}")
                except Exception as e2:
                    print(f"Lỗi sau khi retry mã {sym}: {e2}")
            else:
                print(f"Lỗi khi tải mã {sym}: {e}")

        # chờ để tránh gửi request quá nhanh
        time.sleep(sleep_time)

if __name__ == "__main__":
    symbols = ['VPC', 'VIS', 'VHI', 'VDM', 'TS5', 'TAC', 'T12', 'SVL', 'SON', 'PYU', 'PDT', 'NNQ', 'MXC', 'MEG', 'IPH', 'HUX', 'HTK', 'HNE', 'HLE', 'HGR', 'HGC', 'HGA', 'HDO', 'HAW', 'HAB', 'GTK', 'GQN', 'DX2', 'DNB', 'DKH', 'CT5', 'BXT', 'BUD', 'BPW', 'BLU']
    symbols = ['VPC', 'HDO']

    download_all_histories(
        symbols=symbols,
        start_date="2000-01-01",
        end_date=None,
        source="vci",
        folder="/mnt/c/Users/HOME/Downloads/TF-algo-trading/vci_stock_history",
        interval="1D",
        sleep_time=1.0  # bạn có thể tăng nếu bị lỗi rate limit
    )
