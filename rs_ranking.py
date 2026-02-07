import os
import pandas as pd
import numpy as np
import yfinance as yf

DIR = os.path.dirname(os.path.realpath(__file__))

PRICE_DATA_CSV = os.path.join(DIR, "stock_data.csv")
OUTPUT_CSV = os.path.join(DIR, "stock_data_rs.csv")

REFERENCE_TICKER = "^GSPC"   # S&P500 index ticker in Yahoo Finance
MIN_DATA_POINTS = 20


# ----------------- Relative Strength Core ----------------- #
def relative_strength(closes, closes_ref):
    """Compute RS = (1 + stock_strength) / (1 + ref_strength) * 100"""
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    return round(rs, 2)


def strength(closes):
    """Weighted yearly performance (recent quarter double weight)."""
    try:
        q1 = quarters_perf(closes, 1)
        q2 = quarters_perf(closes, 2)
        q3 = quarters_perf(closes, 3)
        q4 = quarters_perf(closes, 4)
        return 0.4*q1 + 0.2*q2 + 0.2*q3 + 0.2*q4
    except Exception:
        return np.nan


def quarters_perf(closes, n):
    """Return cumulative performance of last n quarters."""
    length = min(len(closes), n * int(252 / 4))
    prices = closes.tail(length)

    if len(prices) < 2:
        return np.nan

    pct_chg = prices.pct_change().dropna()
    perf_cum = (pct_chg + 1).cumprod() - 1
    return perf_cum.tail(1).item()


# ----------------- Main ----------------- #
def main():
    df_all = pd.read_csv(PRICE_DATA_CSV, parse_dates=["date"])
    tickers = df_all["ticker"].unique()

    # --- download SPX directly from Yahoo ---
    df_ref = yf.download(REFERENCE_TICKER, period="2y", progress=False)

    if df_ref.empty:
        raise RuntimeError("Failed to download SPX from Yahoo Finance")

    closes_ref = df_ref["Close"].reset_index(drop=True)

    # --- compute RS ---
    relative_strengths = []

    for ticker in tickers:

        df = df_all[df_all["ticker"] == ticker].sort_values("date")
        closes = df["close"].reset_index(drop=True)

        if len(closes) < MIN_DATA_POINTS:
            rs_score = np.nan
        else:
            rs_score = relative_strength(closes, closes_ref)

            # filter abnormal values
            if rs_score > 1000 or rs_score < 0:
                rs_score = np.nan

        relative_strengths.append({
            "ticker": ticker,
            "score": rs_score,
            "RS": np.nan
        })

    # --- create dataframe ---
    df = pd.DataFrame(relative_strengths)

    # --- percentile ranking ---
    valid_scores = df["score"].dropna()

    if len(valid_scores) > 0:
        df.loc[valid_scores.index, "RS"] = pd.qcut(
            valid_scores,
            100,
            labels=False,
            duplicates="drop"
        )

    # --- sort ---
    df = df.sort_values("score", ascending=False)

    # --- save ---
    df.to_csv(OUTPUT_CSV, index=False)

    print(f"RS calculation finished → {OUTPUT_CSV}")
    print(f"Total tickers processed: {len(df)}")


if __name__ == "__main__":
    main()
