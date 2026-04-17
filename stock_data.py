import os
import pandas as pd
import yfinance as yf
from ftplib import FTP
from io import StringIO
from datetime import date, timedelta
import time

OUTPUT_FILE   = "stock_data.csv"
TICKER_FILE   = "stock_ticker.csv"
INDUSTRY_FILE = "ticker_industry.csv"
DAYS          = 400
MAX_RETRIES   = 3


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
    """從 yfinance 逐筆抓 sector/industry，結果存入 INDUSTRY_FILE。
    若檔案已存在則直接讀取，不重複下載。"""
    if os.path.exists(INDUSTRY_FILE):
        print(f"  {INDUSTRY_FILE} 已存在，直接讀取（跳過下載）")
        return pd.read_csv(INDUSTRY_FILE)

    print(f"  開始從 yfinance 抓取 {len(tickers)} 檔的 industry 資料...")
    rows = []
    for i, ticker in enumerate(tickers, 1):
        sector   = ""
        industry = ""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                info     = yf.Ticker(ticker).info
                sector   = info.get("sector",   "") or ""
                industry = info.get("industry", "") or ""
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    print(f"  info failed {ticker} (attempt {attempt}), retrying: {e}")
                    time.sleep(1)
                else:
                    print(f"  info failed {ticker} after {MAX_RETRIES} attempts: {e}")
        rows.append({"ticker": ticker, "sector": sector, "industry": industry})
        if i % 50 == 0 or i == len(tickers):
            print(f"  [{i}/{len(tickers)}] 進度...")
        time.sleep(0.1)

    df = pd.DataFrame(rows)
    total   = len(df)
    success = (df["industry"] != "").sum()
    print(f"  完成：{success}/{total} 檔有 industry 資料（{success/total*100:.1f}%）")
    return df


def main():
    end   = date.today()
    start = end - timedelta(days=DAYS)

    tickers = get_nasdaq_tickers(500)
    print(f"Downloading {len(tickers)} tickers")

    # OHLCV
    rows = []
    for i, ticker in enumerate(tickers, 1):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                df = yf.download(
                    ticker,
                    start=start,
                    end=end,
                    progress=False,
                    auto_adjust=False,
                )
                if df.empty:
                    break
                df = df.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
                df.columns = ["date", "open", "high", "low", "close", "volume"]
                df["ticker"] = ticker
                df["date"]   = df["date"].dt.strftime("%Y-%m-%d")
                rows.append(df)
                print(f"[{i}/{len(tickers)}] {ticker}")
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    wait = 1
                    print(f"Failed {ticker} (attempt {attempt}), retrying in {wait}s: {e}")
                    time.sleep(wait)
                else:
                    print(f"Failed {ticker} after {MAX_RETRIES} attempts: {e}")
        time.sleep(0.1)

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
