import pandas as pd
import yfinance as yf
from ftplib import FTP
from io import StringIO
import re
from datetime import date, timedelta
import time

OUTPUT_FILE = "stock_data.csv"
TICKER_FILE = "stock_ticker.csv"
INDUSTRY_FILE = "ticker_industry.csv"
DAYS = 400

def get_nasdaq_tickers(limit=None):
    ftp = FTP("ftp.nasdaqtrader.com")
    ftp.login()
    ftp.cwd("SymbolDirectory")
    data = StringIO()
    ftp.retrlines("RETR nasdaqtraded.txt", lambda x: data.write(x + "\n"))
    ftp.quit()
    data.seek(0)
    raw_tickers = []
    for line in data.readlines():
        cols = line.split("|")
        if len(cols) < 8:
            continue
        ticker = cols[1]
        is_etf = cols[5]
        is_test = cols[7]
        if is_etf == "N" and is_test == "N" and len(ticker) <= 4:
            if "$" in ticker or "." in ticker:
                continue
            raw_tickers.append(ticker)
    return raw_tickers[:limit] if limit else raw_tickers

def fetch_industry_meta(tickers: list) -> pd.DataFrame:
    """抓每個 ticker 的 sector / industry / industryKey，失敗跳過。"""
    rows = []
    total = len(tickers)
    for i, ticker in enumerate(tickers, 1):
        for attempt in range(3):
            try:
                info = yf.Ticker(ticker).info
                rows.append({
                    "ticker":      ticker,
                    "sector":      info.get("sector", ""),
                    "industry":    info.get("industry", ""),
                    "industryKey": info.get("industryKey", ""),
                })
                break
            except Exception:
                if attempt < 2:
                    time.sleep(2)
        if i % 100 == 0 or i == total:
            success = len(rows)
            print(f"  [{i}/{total}] industry meta 成功 {success} 檔，失敗 {i - success} 檔")
        time.sleep(0.3)
    return pd.DataFrame(rows, columns=["ticker", "sector", "industry", "industryKey"])

def main():
    end = date.today()
    start = end - timedelta(days=DAYS)
    tickers = get_nasdaq_tickers()
    print(f"Downloading {len(tickers)} tickers")

    # OHLCV
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
            time.sleep(0.1)
        except Exception as e:
            print(f"Failed {ticker}: {e}")

    if not rows:
        raise RuntimeError("No data downloaded")

    result = pd.concat(rows, ignore_index=True)
    result = result[["ticker", "date", "open", "high", "low", "close", "volume"]]
    result.to_csv(OUTPUT_FILE, index=False)
    pd.DataFrame(tickers, columns=["ticker"]).to_csv(TICKER_FILE, index=False)
    print(f"Saved {OUTPUT_FILE}, rows={len(result)}")

    # Industry meta
    print(f"\n抓取 industry meta（共 {len(tickers)} 檔）...")
    df_industry = fetch_industry_meta(tickers)
    df_industry.to_csv(INDUSTRY_FILE, index=False)
    success_rate = len(df_industry) / len(tickers) * 100
    print(f"Saved {INDUSTRY_FILE}，成功率 {success_rate:.1f}%（{len(df_industry)}/{len(tickers)}）")

if __name__ == "__main__":
    main()
