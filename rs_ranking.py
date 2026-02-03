import os
import pandas as pd
import numpy as np

DIR = os.path.dirname(os.path.realpath(__file__))

PRICE_DATA_CSV = os.path.join(DIR, "stock_data.csv")      # 原始 OHLCV
OUTPUT_CSV = os.path.join(DIR, "stock_data_rs.csv")    # 最終輸出
REFERENCE_TICKER = "SPX"  # 基準股票
MIN_DATA_POINTS = 21     # 至少1個月以上

# ----------------- Relative Strength ----------------- #
def relative_strength(closes, closes_ref):
    """計算 RS = (1+strength_stock)/(1+strength_ref) * 100"""
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    return round(rs, 2)

def strength(closes):
    """計算過去一年股價表現，最近一季權重雙倍"""
    try:
        q1 = quarters_perf(closes, 1)
        q2 = quarters_perf(closes, 2)
        q3 = quarters_perf(closes, 3)
        q4 = quarters_perf(closes, 4)
        return 0.4*q1 + 0.2*q2 + 0.2*q3 + 0.2*q4
    except:
        return 0

def quarters_perf(closes, n):
    length = min(len(closes), n*int(252/4))  # 每季約 63 天
    prices = closes.tail(length)
    pct_chg = prices.pct_change().dropna()
    perf_cum = (pct_chg + 1).cumprod() - 1
    return perf_cum.tail(1).item()

# ----------------- Main ----------------- #
def main():
    df_all = pd.read_csv(PRICE_DATA_CSV, parse_dates=["date"])
    tickers = df_all['ticker'].unique()

    # 基準股票收盤價
    df_ref = df_all[df_all['ticker'] == REFERENCE_TICKER].sort_values("date")
    closes_ref = df_ref['close'].reset_index(drop=True)

    # 記錄每個 ticker 的 RS
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
            "score": rs,
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
    
    # === Fred 核心：用整個市場做 percentile ===
    df["RS"] = pd.qcut(df["score"], 100, labels=False, duplicates="drop")
 
    # RS 大的在前
    df = df.sort_values("score", ascending=False)
       
    # ===== TradingView RS RATING（完全照抄 Fred）=====
    percentile_values = [98, 89, 69, 49, 29, 9, 1]
    first_rs_values = {}
    
    for percentile in percentile_values:
        first_row = df[df["RS"] == percentile].iloc[0]
        first_rs_values[percentile] = first_row["score"]
    
    # ===== 最終輸出 =====
    df.to_csv(OUTPUT_CSV, index=False)

if __name__ == "__main__":
    main()
