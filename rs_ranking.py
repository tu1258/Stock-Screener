import os
import pandas as pd
import numpy as np

DIR = os.path.dirname(os.path.realpath(__file__))

PRICE_DATA_CSV = os.path.join(DIR, "rs_data.csv")      # 原始 OHLCV
OUTPUT_CSV = os.path.join(DIR, "stock_data_rs.csv")    # 最終輸出
REFERENCE_TICKER = "SPY"  # 基準股票
MIN_DATA_POINTS = 120     # 至少六個月以上 (~6*20天)

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
    df_all = pd.read_csv(PRICE_DATA_CSV, parse_dates=["Date"])
    tickers = df_all['Ticker'].unique()

    # 基準股票收盤價
    df_ref = df_all[df_all['Ticker'] == REFERENCE_TICKER].sort_values("Date")
    closes_ref = df_ref['Close'].reset_index(drop=True)

    # 記錄每個 ticker 的 RS
    rs_dict = {}

    for ticker in tickers:
        if ticker == REFERENCE_TICKER:
            rs_dict[ticker] = 100.0  # 基準固定 100
            continue

        df = df_all[df_all['Ticker'] == ticker].sort_values("Date")
        closes = df['Close'].reset_index(drop=True)

        if len(closes) < MIN_DATA_POINTS:
            rs_dict[ticker] = np.nan
            continue

        rs = relative_strength(closes, closes_ref)
        rs_dict[ticker] = rs

    # 把 RS append 到原本的 dataframe
    df_all['RS'] = df_all['Ticker'].map(rs_dict)

    # 儲存到 CSV
    df_all.to_csv(OUTPUT_CSV, index=False)
    print(f"RS 已經計算完成並存到 {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
