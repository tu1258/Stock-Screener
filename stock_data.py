import pandas as pd
import yfinance as yf
from ftplib import FTP
from io import StringIO
from datetime import date, timedelta
from finvizfinance.screener.overview import Overview
from finvizfinance.group.overview import Overview as GroupOverview
import time

OUTPUT_FILE   = "stock_data.csv"
TICKER_FILE   = "stock_ticker.csv"
INDUSTRY_FILE = "ticker_industry.csv"
DAYS          = 400

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

def fetch_industry_map():
    print("  取得 finviz industry 清單...")
    g          = GroupOverview()
    df_groups  = g.screener_view(group="Industry", order="Name")
    industries = df_groups["Name"].tolist()
    print(f"  共 {len(industries)} 個 industry")

    foverview  = Overview()
    ticker_map = {}
    for i, industry in enumerate(industries, 1):
        t = time.time()
        try:
            foverview.set_filter(filters_dict={"Industry": industry})
            df = foverview.screener_view(verbose=0)
            if df is not None and not df.empty:
                sector = df["Sector"].iloc[0] if "Sector" in df.columns else ""
                for ticker in df["Ticker"]:
                    ticker_map[ticker] = (sector, industry)
                print(f"  [{i}/{len(industries)}] {industry} {time.time()-t:.1f}s  tickers={len(df)}", flush=True)
            else:
                print(f"  [{i}/{len(industries)}] {industry} {time.time()-t:.1f}s  tickers=0", flush=True)
        except Exception as e:
            print(f"  [{i}/{len(industries)}] {industry} 失敗：{e}", flush=True)
        time.sleep(1)
    return ticker_map

def main():
    t_total = time.time()
    end     = date.today()
    start   = end - timedelta(days=DAYS)

    print("[1/3] 從 NASDAQ FTP 取得 ticker 清單...")
    t0      = time.time()
    tickers = get_nasdaq_tickers()
    print(f"      取得 {len(tickers)} 個 ticker（{time.time()-t0:.1f}s）")
    pd.DataFrame(tickers, columns=["ticker"]).to_csv(TICKER_FILE, index=False)
    print(f"      Saved {TICKER_FILE}")

    print("\n[2/3] 從 Finviz 取得 industry 資料...")
    t0            = time.time()
    ticker_map    = fetch_industry_map()
    industry_list = []
    for ticker in tickers:
        sector, industry = ticker_map.get(ticker, ("", ""))
        industry_list.append({"ticker": ticker, "sector": sector, "industry": industry})
    df_industry = pd.DataFrame(industry_list)
    total       = len(df_industry)
    success     = (df_industry["industry"] != "").sum()
    df_industry.to_csv(INDUSTRY_FILE, index=False)
    print(f"      Saved {INDUSTRY_FILE}，成功={success}/{total}（{success/total*100:.1f}%）（{time.time()-t0:.1f}s）")

    print(f"\n[3/3] OHLCV 下載（共 {len(tickers)} 檔）...")
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
