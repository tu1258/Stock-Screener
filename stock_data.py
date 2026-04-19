import pandas as pd
import yfinance as yf
from datetime import date, timedelta
from finvizfinance.screener.overview import Overview
import time

OUTPUT_FILE   = "stock_data.csv"
TICKER_FILE   = "stock_ticker.csv"
INDUSTRY_FILE = "ticker_industry.csv"
DAYS          = 400

def fetch_finviz_data(limit=None):
    print("  從 Finviz 取得所有股票資料...")
    foverview = Overview()
    foverview.set_filter(filters_dict={"Industry": "Stocks only (ex-Funds)"})
    df = foverview.screener_view(verbose=1)
    df = df[df["Industry"] != "Shell Companies"].reset_index(drop=True)
    if limit:
        df = df.head(limit)
    print(f"\n  完成，共 {len(df)} 筆")
    return df[["Ticker", "Sector", "Industry"]]
    
def main():
    t_total = time.time()
    end     = date.today()
    start   = end - timedelta(days=DAYS)

    print("[1/3] 從 Finviz 取得 ticker 與 industry...")
    t0         = time.time()
    df_finviz  = fetch_finviz_data(1000) # SIZE
    tickers    = df_finviz["Ticker"].tolist()
    print(f"      取得 {len(tickers)} 個 ticker（{time.time()-t0:.1f}s）")

    pd.DataFrame(tickers, columns=["ticker"]).to_csv(TICKER_FILE, index=False)
    print(f"      Saved {TICKER_FILE}")

    df_industry = df_finviz.rename(columns={"Ticker": "ticker", "Sector": "sector", "Industry": "industry"})
    total       = len(df_industry)
    success     = (df_industry["industry"] != "").sum()
    df_industry.to_csv(INDUSTRY_FILE, index=False)
    print(f"      Saved {INDUSTRY_FILE}，成功={success}/{total}（{success/total*100:.1f}%）")

    print(f"\n[2/3] OHLCV 下載（共 {len(tickers)} 檔）...")
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
            print(f"  [{i}/{len(tickers)}] {ticker} {time.time()-t:.2f}s rows={len(df)}", flush=True)
        except Exception as e:
            print(f"  [{i}/{len(tickers)}] {ticker} 失敗：{e}", flush=True)

    if not rows:
        raise RuntimeError("No data downloaded")

    result = pd.concat(rows, ignore_index=True)
    result = result[["ticker", "date", "open", "high", "low", "close", "volume"]]
    result.to_csv(OUTPUT_FILE, index=False)
    print(f"      Saved {OUTPUT_FILE}，rows={len(result)}（{time.time()-t0:.1f}s）")

    print(f"\n完成，總耗時 {time.time()-t_total:.1f}s")

if __name__ == "__main__":
    main()
