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
        return 0


def quarters_perf(closes, n):
    """Return cumulative performance of last n quarters."""
    length = min(len(closes), n * int(252 / 4))
    prices = closes.tail(length)
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
    ranks = []

    for ticker in tickers:
        if ticker == REFERENCE_TICKER:
            rs_score = 100.0
        else:
            df = df_all[df_all['ticker'] == ticker].sort_values("date")
            closes = df['close'].reset_index(drop=True)
    
            if len(closes) < MIN_DATA_POINTS:
                rs_score = np.nan
                continue  # 或者 rs = np.nan，然後 append
            else:
                rs_score = relative_strength(closes, closes_ref)
                if rs_score > 1000:
                    continue
    
        # append 到 list
        relative_strengths.append({
            "ticker": ticker,
            "score": rs_score,
            "RS": 100.
        })
        rs_score = relative_strength(closes, closes_ref)
        

    # 把 RS append 到原本的 dataframe
    df = pd.DataFrame(
        relative_strengths,
        columns=[
            "ticker",
            "score",
            "RS",
        ]
    )
    
    # === 用整個市場做 percentile ===
    df["RS"] = pd.qcut(df["score"], 100, labels=False, duplicates="drop")
 
    # RS 大的在前
    df = df.sort_values("score", ascending=False)
       
    # ===== TradingView RS RATING =====
    percentile_values = [98, 89, 69, 49, 29, 9, 1]
    first_rs_values = {}
    
    for percentile in percentile_values:
        first_row = df[df["RS"] == percentile].iloc[0]
        first_rs_values[percentile] = first_row["score"]
    
    # ===== 最終輸出 =====
    df.to_csv(OUTPUT_CSV, index=False)

    # ===== 找出 stock_ticker.csv 裡面漏掉的 ticker =====
    df_tickers = pd.read_csv("stock_ticker.csv")  # 你原本的 ticker 列表
    missing_tickers = set(df_tickers["ticker"]) - set(df["ticker"])
    
    if missing_tickers:
        print("\n⚠️ 以下 ticker 在 stock_data.csv 沒有資料 / RS 無法計算:")
        for t in sorted(missing_tickers):
            print(t)
    else:
        print("\n✅ 所有 ticker 都有資料")

if __name__ == "__main__":
    main()
