import pandas as pd
import numpy as np

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"

# ---------------- 指標計算 ---------------- #
def compute_indicators(df):
    df = df.sort_values("date")

    # 10日平均成交值
    df["avg_value_10"] = df["close"] * df["volume"].rolling(10).mean()

    # ATR 20日百分比
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift(1)).abs()
    lc = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    df["atr_20_pct"] = tr.rolling(20).mean() / df["close"] * 100

    # 均線
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()
    df["ma200_prev"] = df["ma200"].shift(1)

    # 5日高低距離
    high5 = df["high"].rolling(5).max()
    low5 = df["low"].rolling(5).min()
    df["dist_high5_pct"] = (high5 - df["close"]) / high5 * 100
    df["dist_low5_pct"] = (df["close"] - low5) / low5 * 100

    return df

# ---------------- 主程式 ---------------- #
def main():
    # 讀檔
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    rs_df = pd.read_csv(RS_CSV)

    # ---------- 1. RS 篩選 ----------
    rs_filtered = rs_df[rs_df["RS"] > 90].copy()
    rs_filtered = rs_filtered.sort_values("RS", ascending=False)

    # 取得符合 RS 的 ticker
    rs_tickers = rs_filtered["ticker"].tolist()

    # ---------- 2. 技術分析篩選 ----------
    tech_filtered = (
        price_df[price_df["ticker"].isin(rs_tickers)]
        .groupby("ticker", group_keys=False)
        .apply(compute_indicators)
        .groupby("ticker", group_keys=False)
        .tail(1)  # 最新一筆
    )

    tech_filtered = tech_filtered[
        (tech_filtered["avg_value_10"] > 100_000_000) &
        (tech_filtered["atr_20_pct"] > 1) &
        (tech_filtered["close"] > tech_filtered["ma20"]) &
        (tech_filtered["close"] > tech_filtered["ma50"]) &
        (tech_filtered["ma50"] > tech_filtered["ma200"]) &
        (tech_filtered["ma200"] > tech_filtered["ma200_prev"]) &
        (tech_filtered["dist_high5_pct"] <= 10) &
        (tech_filtered["dist_low5_pct"] <= 10)
    ]

    # 依 RS 排序，只輸出 ticker
    final_tickers = tech_filtered.merge(
        rs_filtered[["ticker", "RS"]],
        on="ticker",
        how="left"
    ).sort_values("RS", ascending=False)["ticker"]

    final_tickers.to_csv(OUTPUT_CSV, index=False, header=True)
    print(f"Saved {len(final_tickers)} tickers to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
