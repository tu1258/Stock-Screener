import pandas as pd
import requests
import yfinance as yf
from ftplib import FTP
from io import StringIO
from datetime import date, timedelta
import time

OUTPUT_FILE  = "stock_data.csv"
TICKER_FILE  = "stock_ticker.csv"
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
        ticker  = cols[1]
        is_etf  = cols[5]
        is_test = cols[7]
        if is_etf == "N" and is_test == "N" and len(ticker) <= 4:
            if "$" in ticker or "." in ticker:
                continue
            raw_tickers.append(ticker)
    return raw_tickers[:limit] if limit else raw_tickers

def fetch_industry_meta(tickers: list) -> pd.DataFrame:
    """Nasdaq screener API 一次抓全市場 sector/industry，再 join 到 ticker 清單。"""
    print("  從 Nasdaq screener 抓取 industry 資料...")
    url = "https://api.nasdaq.com/api/screener/stocks"
    params  = {"tableonly": "true", "limit": 25000, "download": "true"}
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        rows = resp.json()["data"]["rows"]
        df_nasdaq = pd.DataFrame(rows)[["symbol", "sector", "industry"]]
        df_nasdaq.columns = ["ticker", "sector", "industry"]
        df_nasdaq["ticker"] = df_nasdaq["ticker"].str.upper().str.strip()
    except Exception as e:
        print(f"  ⚠️ Nasdaq screener 失敗：{e}，回傳空表")
        return pd.DataFrame(columns=["ticker", "sector", "industry"])

    df_tickers = pd.DataFrame({"ticker": [t.upper() for t in tickers]})
    df_merged  = df_tickers.merge(df_nasdaq, on="ticker", how="left")

    total    = len(df_merged)
    success  = df_merged["industry"].notna().sum()
    print(f"  完成：{success}/{total} 檔有 industry 資料（{success/total*100:.1f}%）")
    return df_merged

def main():
    end   = date.today()
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
    print(f"Saved {INDUSTRY_FILE}")

if __name__ == "__main__":
    main()
