import pandas as pd
import yfinance as yf
from datetime import date
import numpy as np
import time

OUTPUT_FILE = "financial_data.csv"
TICKER_FILE = "stock_ticker.csv"

def format_number_for_csv(x):
    """Format number for CSV output: use M/B with 2 decimals, negatives handled,
       abs(x)<10k or 0 → "-"."""
    if pd.isna(x) or abs(x) < 10_000:
        return "-"
    sign = "-" if x < 0 else ""
    x_abs = abs(x)
    if x_abs >= 1_000_000_000:
        return f"{sign}{x_abs/1_000_000_000:.2f}B"
    else:
        return f"{sign}{x_abs/1_000_000:.2f}M"

def format_percent_for_csv(x):
    """Format percentage for CSV output: 2 decimals, NaN → "-"."""
    if pd.isna(x):
        return "-"
    return f"{x*100:.2f}%"

def get_latest_5q_revenue(ticker: str, today: pd.Timestamp):
    """Get latest 5 reported quarters. Determine latest valid quarter for q0,
       compute QoQ/YoY, return raw numbers for calculation."""
    try:
        stock = yf.Ticker(ticker)
        q = stock.quarterly_financials

        if q is None or q.empty:
            return [0]*5 + [np.nan, np.nan]

        q = q.T.sort_index()
        q = q[q.index <= today]
        last5 = q.tail(5)

        revenues = last5.get("Total Revenue")
        if revenues is None:
            return [0]*5 + [np.nan, np.nan]

        revenues = revenues.fillna(0).tolist()
        if len(revenues) < 5:
            revenues = [0]*(5-len(revenues)) + revenues

        # Find latest valid (non-zero) quarter
        latest_idx = None
        for i in range(4, -1, -1):
            if revenues[i] != 0:
                latest_idx = i
                break
        if latest_idx is None:
            latest_idx = 4
        rev_q0 = revenues[latest_idx]

        # Previous valid quarter for QoQ
        prev_idx = None
        for i in range(latest_idx-1, -1, -1):
            if revenues[i] != 0:
                prev_idx = i
                break
        rev_prev = revenues[prev_idx] if prev_idx is not None else np.nan

        # Same quarter last year for YoY
        yoy_idx = latest_idx - 4
        rev_yoy_base = revenues[yoy_idx] if 0 <= yoy_idx < 5 else np.nan

        # Reorder for CSV: q0 = latest, q1, ..., q4 = oldest
        rev_ordered = []
        for i in range(5):
            idx = latest_idx - i
            if 0 <= idx < 5:
                rev_ordered.append(revenues[idx])
            else:
                rev_ordered.append(0)

        # QoQ / YoY calculations
        rev_qoq = (rev_q0 - rev_prev)/rev_prev if prev_idx is not None and rev_prev != 0 else np.nan
        rev_yoy = (rev_q0 - rev_yoy_base)/rev_yoy_base if not pd.isna(rev_yoy_base) and rev_yoy_base != 0 else np.nan

        return rev_ordered + [rev_qoq, rev_yoy]

    except Exception:
        return [0]*5 + [np.nan, np.nan]

def build_revenue_csv(tickers: list[str]):
    """Build CSV containing 5-quarter revenue (q0 latest → q4 oldest), QoQ, YoY.
       Format output: small/0/NaN → '-', M/B with 2 decimals, percentage with 2 decimals."""
    today = pd.Timestamp(date.today())
    rows = []

    for i, ticker in enumerate(tickers, 1):
        revs = get_latest_5q_revenue(ticker, today)
        # Format for CSV
        rev_fmt = [format_number_for_csv(x) for x in revs[:5]]
        rev_qoq_fmt = format_percent_for_csv(revs[5])
        rev_yoy_fmt = format_percent_for_csv(revs[6])

        rows.append({
            "ticker": ticker,
            "rev_q0": rev_fmt[0],  # latest
            "rev_q1": rev_fmt[1],
            "rev_q2": rev_fmt[2],
            "rev_q3": rev_fmt[3],
            "rev_q4": rev_fmt[4],  # oldest
            "rev_qoq": rev_qoq_fmt,
            "rev_yoy": rev_yoy_fmt,
        })

        print(f"[{i}/{len(tickers)}] {ticker}")
        time.sleep(0.1)

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved → {OUTPUT_FILE}")

if __name__ == "__main__":
    df_tickers = pd.read_csv(TICKER_FILE)
    tickers = df_tickers["ticker"].tolist()
    print(f"Total tickers: {len(tickers)}")
    build_revenue_csv(tickers)
