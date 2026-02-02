import yfinance as yf
import pandas as pd
import time
import re
from ftplib import FTP
from io import StringIO
from datetime import date, timedelta
import json

# ---------- CONFIG ----------
MAX_TICKERS = 10          # 先抓 10 檔
DAYS = 365 + 183          # 1.5 years
OUTPUT_JSON = "rs_data.json"
OUTPUT_CSV = "rs_data.csv"
# ----------------------------

def get_nasdaq_tickers(limit=10):
    ftp = FTP("ftp.nasdaqtrader.com")
    ftp.login()
    ftp.cwd("SymbolDirectory")

    buf = StringIO()
    ftp.retrlines("RETR nasdaqtraded.txt", lambda x: buf.write(x + "\n"))
    ftp.quit()

    buf.seek(0)
    tickers = []

    for line in buf.readlines():
        cols = line.split("|")
        if len(cols) < 8:
            continue
        ticker = cols[1]
        etf = cols[5]
        test = cols[7]

        if re.match(r"^[A-Z]+$", ticker) and etf == "N" and test == "N":
            tickers.append(ticker)
        if len(tickers) >= limit:
            break

    return tickers

def fetch_close_from_yf(ticker, start, end):
    df = yf.Ticker(ticker).history(start=start, end=end)
    if df.empty:
        return None

    candles = []
    for ts, row in df.iterrows():
        candles.append({
            "datetime": int(ts.timestamp()),
            "close": float(row["Close"])
        })
    return candles

def main():
    today = date.today()
    start_date = today - timedelta(days=DAYS)

    tickers = get_nasdaq_tickers(MAX_TICKERS)
    print(f"Fetched {len(tickers)} tickers")

    rs_data = []
    rs_json = {}

    for t in tickers:
        print(f"Downloading {t}")
        candles = fetch_close_from_yf(t, start_date, today)
        if not candles:
            print(f"  ❌ no data")
            continue

        rs_json[t] = candles
        for c in candles:
            rs_data.append([t, c["datetime"], c["close"]])

        time.sleep(0.1)

    # JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(rs_json, f, indent=2)

    # CSV
    df = pd.DataFrame(rs_data, columns=["ticker", "datetime", "close"])
    df.to_csv(OUTPUT_CSV, index=False)

    print("✅ Done")
    print(f" - {OUTPUT_JSON}")
    print(f" - {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
