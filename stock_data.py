import pandas as pd
import yfinance as yf
from ftplib import FTP
from io import StringIO
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
import time

OUTPUT_FILE   = "stock_data.csv"
TICKER_FILE   = "stock_ticker.csv"
INDUSTRY_FILE = "ticker_industry.csv"
DAYS          = 400
BATCH_SIZE    = 100
INFO_WORKERS  = 10

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

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_one_industry(ticker):
    try:
        info = yf.Ticker(ticker).info
        return {
            "ticker":   ticker,
            "sector":   info.get("sector",   "") or "",
            "industry": info.get("industry", "") or "",
        }
    except:
        return {"ticker": ticker, "sector": "", "industry": ""}

def main():
    t_total = time.time()
    end   = date.today()
    start = end - timedelta(days=DAYS)

    print(f"[1/3] 從 NASDAQ FTP 取得 ticker 清單...")
    t0 = time.time()
    tickers = get_nasdaq_tickers()
    print(f"      取得 {len(tickers)} 個 ticker（{time.time()-t0:.1f}s）")

    print(f"\n[2/3] OHLCV 下載（batch={BATCH_SIZE}，共 {len(list(chunks(tickers, BATCH_SIZE)))} 批）...")
    t0 = time.time()
    rows = []
    batches = list(chunks(tickers, BATCH_SIZE))
    for i, batch in enumerate(batches, 1):
        try:
            df_batch = yf.download(
                batch,
                start=start,
                end=end,
                progress=False,
                auto_adjust=True,
                group_by="ticker",
            )
            for ticker in batch:
                try:
                    t_df = df_batch[ticker][["Open", "High", "Low", "Close", "Volume"]].dropna(how="all").reset_index()
                    t_df.columns = ["date", "open", "high", "low", "close", "volume"]
                    t_df["ticker"] = ticker
                    t_df["date"] = t_df["date"].dt.strftime("%Y-%m-%d")
                    rows.append(t_df)
                except:
                    pass
        except Exception as e:
            print(f"      batch {i} 失敗：{e}")
        print(f"      [{i}/{len(batches)}] 完成", flush=True)

    if not rows:
        raise RuntimeError("No data downloaded")

    result = pd.concat(rows, ignore_index=True)
    result = result[["ticker", "date", "open", "high", "low", "close", "volume"]]
    result.to_csv(OUTPUT_FILE, index=False)
    pd.DataFrame(tickers, columns=["ticker"]).to_csv(TICKER_FILE, index=False)
    print(f"      Saved {OUTPUT_FILE}，rows={len(result)}（{time.time()-t0:.1f}s）")

    print(f"\n[3/3] Industry 下載（{INFO_WORKERS} 線程並行，共 {len(tickers)} 檔）...")
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=INFO_WORKERS) as executor:
        industry_list = list(executor.map(fetch_one_industry, tickers))
    df_industry = pd.DataFrame(industry_list)
    total       = len(df_industry)
    ind_success = (df_industry["industry"] != "").sum()
    df_industry.to_csv(INDUSTRY_FILE, index=False)
    print(f"      {ind_success}/{total} 檔有 industry（{ind_success/total*100:.1f}%）（{time.time()-t0:.1f}s）")
    print(f"      Saved {INDUSTRY_FILE}")

    print(f"\n完成，總耗時 {time.time()-t_total:.1f}s")

if __name__ == "__main__":
    main()
