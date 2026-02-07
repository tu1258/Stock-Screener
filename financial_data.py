import pandas as pd
import yfinance as yf
from datetime import date
import numpy as np
import time

OUTPUT_FILE = "financial_data.csv"
TICKER_FILE = "stock_ticker.csv"  # 直接读取这个 CSV

def format_number(x):
    """Convert number to M/B string with 2 decimals, handle negatives, treat abs(x)<10k as 0."""
    if pd.isna(x) or abs(x) < 10_000:
        return "0"

    sign = "-" if x < 0 else ""
    x_abs = abs(x)

    if x_abs >= 1_000_000_000:
        return f"{sign}{x_abs/1_000_000_000:.2f}B"
    else:  # 所有 >=10_000 且 < 1B 的都用 M 表示
        return f"{sign}{x_abs/1_000_000:.2f}M"

def get_latest_4q_revenue(ticker: str, today: pd.Timestamp):
    """Fetch latest 4 reported quarters of revenue relative to today."""
    try:
        stock = yf.Ticker(ticker)
        q = stock.quarterly_financials

        if q is None or q.empty:
            return ["0", "0", "0", "0", np.nan, np.nan]

        q = q.T.sort_index()           # oldest → newest
        q = q[q.index <= today]        # 只保留今天之前的报表
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
    # 直接從 stock_ticker.csv 讀取 ticker
    df_tickers = pd.read_csv(TICKER_FILE)
    tickers = df_tickers["ticker"].tolist()
    print(f"Total tickers: {len(tickers)}")
    build_revenue_csv(tickers)
