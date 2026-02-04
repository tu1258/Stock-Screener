import pandas as pd
import yfinance as yf
from ftplib import FTP
from io import StringIO
import re
from datetime import date, timedelta
import time

OUTPUT_FILE = "stock_data.csv"
MAX_TICKERS = 250        # 先測，之後拿掉
DAYS = 400              # 1 年

def get_nasdaq_tickers(limit=None):
    ftp = FTP("ftp.nasdaqtrader.com")
    ftp.login()
    ftp.cwd("SymbolDirectory")

    data = StringIO()
    ftp.retrlines("RETR nasdaqtraded.txt", lambda x: data.write(x + "\n"))
    ftp.quit()

    data.seek(0)
    tickers = []

    for line in data.readlines():
        cols = line.split("|")
        if len(cols) < 8:
            continue
        ticker = cols[1]
        is_etf = cols[5]
        is_test = cols[7]

        if re.fullmatch(r"[A-Z]+", ticker) and is_etf == "N" and is_test == "N":
            tickers.append(ticker)

    return tickers[:limit] if limit else tickers


def main():
    end = date.today()
    start = end - timedelta(days=DAYS)

    tickers = get_nasdaq_tickers(250) # MAX_TICKERS
    print(f"Downloading {len(tickers)} tickers")

    rows = []

    for i, ticker in enumerate(tickers, 1):
        try:
            df = yf.download(
                ticker,
                start=start,
                end=end,
                progress=False,
                auto_adjust=False,
            )

            if df.empty:
                continue

            df = df.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
            df.columns = ["date", "open", "high", "low", "close", "volume"]
            df["ticker"] = ticker
            df["date"] = df["date"].dt.strftime("%Y-%m-%d")

            rows.append(df)
            print(f"[{i}/{len(tickers)}] {ticker}")

            time.sleep(0.1)  # 避免被 Yahoo ban

        except Exception as e:
            print(f"Failed {ticker}: {e}")

    if not rows:
        raise RuntimeError("No data downloaded")

    result = pd.concat(rows, ignore_index=True)
    result = result[["ticker", "date", "open", "high", "low", "close", "volume"]]
    result.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved {OUTPUT_FILE}, rows={len(result)}")


if __name__ == "__main__":
    main()
