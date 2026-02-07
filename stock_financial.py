import pandas as pd
import yfinance as yf
from datetime import date
import numpy as np

OUTPUT_FILE = "revenue_data.csv"


def get_latest_4q_revenue(ticker: str, today: pd.Timestamp):
    """
    Fetch latest 4 reported quarters of revenue relative to *today*,
    not relative to historical trading dates.
    """
    try:
        stock = yf.Ticker(ticker)
        q = stock.quarterly_financials

        if q is None or q.empty:
            return [0, 0, 0, 0, np.nan, np.nan]

        # Ensure chronological order (oldest → newest)
        q = q.T.sort_index()

        # Keep only reports announced before today
        q = q[q.index <= today]

        # Take last 4 quarters
        last4 = q.tail(4)

        revenues = last4.get("Total Revenue")
        if revenues is None:
            return [0, 0, 0, 0, np.nan, np.nan]

        revenues = revenues.fillna(0).tolist()

        # Pad to 4 quarters
        if len(revenues) < 4:
            revenues = [0] * (4 - len(revenues)) + revenues

        rev_q1, rev_q2, rev_q3, rev_q4 = revenues  # oldest → newest

        # QoQ
        rev_qoq = (rev_q4 - rev_q3) / rev_q3 if rev_q3 != 0 else np.nan

        # YoY (compare newest vs same quarter last year)
        rev_yoy = (rev_q4 - rev_q1) / rev_q1 if rev_q1 != 0 else np.nan

        return [rev_q1, rev_q2, rev_q3, rev_q4, rev_qoq, rev_yoy]

    except Exception:
        return [0, 0, 0, 0, np.nan, np.nan]


def build_revenue_csv(tickers: list[str]):
    today = pd.Timestamp(date.today())

    rows = []

    for i, ticker in enumerate(tickers, 1):
        rev_q1, rev_q2, rev_q3, rev_q4, rev_qoq, rev_yoy = get_latest_4q_revenue(
            ticker, today
        )

        rows.append(
            {
                "ticker": ticker,
                "rev_q1": rev_q1,
                "rev_q2": rev_q2,
                "rev_q3": rev_q3,
                "rev_q4": rev_q4,
                "rev_qoq": rev_qoq,
                "rev_yoy": rev_yoy,
            }
        )

        print(f"[{i}/{len(tickers)}] {ticker}")

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved → {OUTPUT_FILE}")


if __name__ == "__main__":
    # Example usage
    sample_tickers = ["AAPL", "MSFT", "NVDA"]
    build_revenue_csv(sample_tickers)
