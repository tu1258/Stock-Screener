import pandas as pd
import yfinance as yf
from ftplib import FTP
from io import StringIO
import re
from datetime import date
import numpy as np
import time

OUTPUT_FILE = "financial_data.csv"

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
        cols = line.strip().split("|")
        if len(cols) < 8:
            continue
        ticker = cols[1]
        is_etf = cols[5]
        is_test = cols[7]

        if re.fullmatch(r"[A-Z]+", ticker) and is_etf == "N" and is_test == "N":
            tickers.append(ticker)

    return tickers[:limit] if limit else tickers

def format_number(x):
    """Convert a number to M/B string representation without decimals."""
    if pd.isna(x) or x == 0:
        return "0"
    if x >= 1_000_000_000:
        return f"{int(round(x/1_000_000_000))}B"
    elif x >= 1_000_000:
        return f"{int(round(x/1_000_000))}M"
    else:
        return str(int(round(x)))

def get_latest_4q_revenue(ticker: str, today: pd.Timestamp):
    """Fetch latest 4 reported quarters of revenue relative to today."""
    try:
        stock = yf.Ticker(ticker)
        q = stock.quarterly_financials

        if q is None or q.empty:
            return ["0", "0", "0", "0", np.nan, np.nan]

        q = q.T.sort_index()           # oldest → newest
        q = q[q.index <= today]        # 只保留今天之前的報表
        last4 = q.tail(4)

        revenues = last4.get("Total Revenue")
        if revenues is None:
            return ["0", "0", "0", "0", np.nan, np.nan]

        revenues = revenues.fillna(0).tolist()
        if len(revenues) < 4:
            revenues = [0]*(4-len(revenues)) + revenues

        rev_q1, rev_q2, rev_q3, rev_q4 = revenues

        rev_qoq = (rev_q4 - rev_q3) / rev_q3 if rev_q3 != 0 else np.nan
        rev_yoy = (rev_q4 - rev_q1) / rev_q1 if rev_q1 != 0 else np.nan

        return [
            format_number(rev_q1),
            format_number(rev_q2),
            format_number(rev_q3),
            format_number(rev_q4),
            rev_qoq,
            rev_yoy
        ]
    except Exception:
        return ["0", "0", "0", "0", np.nan, np.nan]

def build_revenue_csv(tickers: list[str]):
    today = pd.Timestamp(date.today())
    rows = []

    for i, ticker in enumerate(tickers, 1):
        rev_q1, rev_q2, rev_q3, rev_q4, rev_qoq, rev_yoy = get_latest_4q_revenue(ticker, today)
        rows.append({
            "ticker": ticker,
            "rev_q1": rev_q1,
            "rev_q2": rev_q2,
            "rev_q3": rev_q3,
            "rev_q4": rev_q4,
            "rev_qoq": rev_qoq,
            "rev_yoy": rev_yoy,
        })

        print(f"[{i}/{len(tickers)}] {ticker}")
        time.sleep(0.1)  # 避免 Yahoo ban

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved → {OUTPUT_FILE}")

if __name__ == "__main__":
    tickers = get_nasdaq_tickers(100)  # 取得 NASDAQ 全部股票
    print(f"Total tickers: {len(tickers)}")
    build_revenue_csv(tickers)
