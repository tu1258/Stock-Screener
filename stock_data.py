import pandas as pd
import yfinance as yf
import requests
from ftplib import FTP
from io import StringIO
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
import threading
import time

OUTPUT_FILE   = "stock_data.csv"
TICKER_FILE   = "stock_ticker.csv"
INDUSTRY_FILE = "ticker_industry.csv"
DAYS          = 400
HEADERS       = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

_print_lock = threading.Lock()
def safe_print(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs)

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

def task_ohlcv(tickers, start, end):
    safe_print("  [OHLCV] 開始...")
    t0   = time.time()
    rows = []
    for i, ticker in enumerate(tickers, 1):
        t = time.time()
        try:
            df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
            if not df.empty:
                df = df.reset_index()[["Date", "Open", "High", "Low", "Close", "Volume"]]
                df.columns = ["date", "open", "high", "low", "close", "volume"]
                df["ticker"] = ticker
                df["date"]   = df["date"].dt.strftime("%Y-%m-%d")
                rows.append(df)
            safe_print(f"  [OHLCV] [{i}/{len(tickers)}] {ticker} {time.time()-t:.2f}s rows={len(df)}", flush=True)
        except Exception as e:
            safe_print(f"  [OHLCV] [{i}/{len(tickers)}] {ticker} 失敗：{e}", flush=True)

    elapsed = time.time() - t0
    result  = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    safe_print(f"  [OHLCV] 完成：{elapsed:.1f}s  total rows={len(result)}")
    return result, elapsed

def task_industry(tickers):
    safe_print("  [Industry] 開始...")
    t0   = time.time()
    rows = []
    for i, ticker in enumerate(tickers, 1):
        t = time.time()
        try:
            url      = f"https://api.nasdaq.com/api/quote/{ticker}/summary?assetclass=stocks"
            r        = requests.get(url, headers=HEADERS, timeout=10)
            summary  = r.json().get("data", {}).get("summaryData", {})
            sector   = summary.get("Sector",   {}).get("value", "")
            industry = summary.get("Industry", {}).get("value", "")
            rows.append({
                "ticker":   ticker,
                "sector":   "" if sector   == "N/A" else sector,
                "industry": "" if industry == "N/A" else industry,
            })
            safe_print(f"  [Industry] [{i}/{len(tickers)}] {ticker} {time.time()-t:.2f}s industry={industry}", flush=True)
        except Exception as e:
            safe_print(f"  [Industry] [{i}/{len(tickers)}] {ticker} 失敗：{e}", flush=True)
            rows.append({"ticker": ticker, "sector": "", "industry": ""})

    elapsed = time.time() - t0
    result  = pd.DataFrame(rows)
    success = (result["industry"] != "").sum()
    safe_print(f"  [Industry] 完成：{elapsed:.1f}s  成功={success}/{len(result)}")
    return result, elapsed

def main():
    t_total = time.time()
    end     = date.today()
    start   = end - timedelta(days=DAYS)

    print("[1/2] 從 NASDAQ FTP 取得 ticker 清單...")
    t0      = time.time()
    tickers = get_nasdaq_tickers()
    print(f"      取得 {len(tickers)} 個 ticker（{time.time()-t0:.1f}s）")
    pd.DataFrame(tickers, columns=["ticker"]).to_csv(TICKER_FILE, index=False)
    print(f"      Saved {TICKER_FILE}")

    print("\n[2/2] OHLCV 與 Industry 並行下載...")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_ohlcv    = executor.submit(task_ohlcv,    tickers, start, end)
        f_industry = executor.submit(task_industry, tickers)
        df_ohlcv,    t_ohlcv    = f_ohlcv.result()
        df_industry, t_industry = f_industry.result()
    total_par = time.time() - t0

    if df_ohlcv.empty:
        raise RuntimeError("No OHLCV data downloaded")

    df_ohlcv = df_ohlcv[["ticker", "date", "open", "high", "low", "close", "volume"]]
    df_ohlcv.to_csv(OUTPUT_FILE, index=False)
    print(f"\n      Saved {OUTPUT_FILE}，rows={len(df_ohlcv)}")

    df_industry.to_csv(INDUSTRY_FILE, index=False)
    total   = len(df_industry)
    success = (df_industry["industry"] != "").sum()
    print(f"      Saved {INDUSTRY_FILE}，industry成功={success}/{total}（{success/total*100:.1f}%）")

    print(f"\n      OHLCV耗時={t_ohlcv:.1f}s  Industry耗時={t_industry:.1f}s  並行總計={total_par:.1f}s")
    print(f"完成，總耗時 {time.time()-t_total:.1f}s")

if __name__ == "__main__":
    main()
