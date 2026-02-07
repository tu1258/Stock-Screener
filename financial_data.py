import pandas as pd
import yfinance as yf
from datetime import date
import numpy as np
import time

OUTPUT_FILE = "financial_data.csv"
TICKER_FILE = "stock_ticker.csv"  # Read tickers from this CSV

def format_number(x):
    """Convert number to M/B string with 2 decimals, handle negatives,
       treat abs(x) < 10,000 as 0."""
    if pd.isna(x) or abs(x) < 10_000:
        return "0"

    sign = "-" if x < 0 else ""
    x_abs = abs(x)

    if x_abs >= 1_000_000_000:
        return f"{sign}{x_abs/1_000_000_000:.2f}B"
    else:  # All numbers >=10k and <1B use M
        return f"{sign}{x_abs/1_000_000:.2f}M"

def format_percent(x):
    """Convert float to percentage string with 2 decimals. Return empty string for NaN."""
    if pd.isna(x):
        return ""
    return f"{x*100:.2f}%"

def get_latest_5q_revenue(ticker: str, today: pd.Timestamp):
    """Fetch the latest 5 reported quarters of revenue.
       Skip empty/zero quarters when determining latest, and calculate QoQ/YoY."""
    try:
        stock = yf.Ticker(ticker)
        q = stock.quarterly_financials

        if q is None or q.empty:
            return ["0"]*5 + ["", ""]

        q = q.T.sort_index()  # oldest → newest
        q = q[q.index <= today]  # keep only reports before today
        last5 = q.tail(5)  # latest 5 quarters

        revenues = last5.get("Total Revenue")
        if revenues is None:
            return ["0"]*5 + ["", ""]

        revenues = revenues.fillna(0).tolist()
        if len(revenues) < 5:
            revenues = [0]*(5-len(revenues)) + revenues

        # Replace empty or zero quarters with previous valid value for latest determination
        rev_list = revenues.copy()
        latest_idx = None
        for i in range(4, -1, -1):  # iterate from newest to oldest
            if rev_list[i] != 0:
                latest_idx = i
                break
        if latest_idx is None:
            # all quarters zero
            latest_idx = 4  # fallback
        rev_q4 = rev_list[latest_idx]

        # find previous valid quarter for QoQ
        prev_idx = None
        for i in range(latest_idx-1, -1, -1):
            if rev_list[i] != 0:
                prev_idx = i
                break
        rev_prev = rev_list[prev_idx] if prev_idx is not None else np.nan

        # find same quarter last year for YoY (assuming 4 quarters ago)
        yoy_idx = latest_idx - 4
        rev_yoy_base = rev_list[yoy_idx] if 0 <= yoy_idx < 5 else np.nan

        # Format all 5 quarters
        rev_fmt = [format_number(r) for r in rev_list]

        # QoQ and YoY calculations
        rev_qoq = format_percent((rev_q4 - rev_prev)/rev_prev if prev_idx is not None and rev_prev != 0 else np.nan)
        rev_yoy = format_percent((rev_q4 - rev_yoy_base)/rev_yoy_base if not pd.isna(rev_yoy_base) and rev_yoy_base != 0 else np.nan)

        return rev_fmt + [rev_qoq, rev_yoy]

    except Exception:
        return ["0"]*5 + ["", ""]

def build_revenue_csv(tickers: list[str]):
    """Build CSV containing 5-quarter revenue, QoQ, and YoY for each ticker."""
    today = pd.Timestamp(date.today())
    rows = []

    for i, ticker in enumerate(tickers, 1):
        rev_q0, rev_q1, rev_q2, rev_q3, rev_q4, rev_qoq, rev_yoy = get_latest_5q_revenue(ticker, today)
        rows.append({
            "ticker": ticker,
            "rev_q0": rev_q0,   # Oldest quarter
            "rev_q1": rev_q1,
            "rev_q2": rev_q2,
            "rev_q3": rev_q3,
            "rev_q4": rev_q4,   # Latest quarter (skip empty)
            "rev_qoq": rev_qoq,
            "rev_yoy": rev_yoy,
        })

        print(f"[{i}/{len(tickers)}] {ticker}")
        time.sleep(0.1)  # avoid Yahoo rate limit

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved → {OUTPUT_FILE}")

if __name__ == "__main__":
    # Read tickers from CSV file
    df_tickers = pd.read_csv(TICKER_FILE)
    tickers = df_tickers["ticker"].tolist()
    print(f"Total tickers: {len(tickers)}")
    build_revenue_csv(tickers)
