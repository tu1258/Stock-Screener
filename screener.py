import pandas as pd
import numpy as np

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"

# ---------------- 指標計算（向量化，不用 apply） ---------------- #
def compute_indicators_vectorized(df):
    # 確保按照 ticker 與日期排序
    df = df.sort_values(["ticker", "date"])

    # 10日平均成交值
    df["avg_value_10"] = df.groupby("ticker")["volume"].transform(lambda x: x.rolling(10).mean()) * df["close"]

    # ATR 20日百分比
    hl = df["high"] - df["low"]
    hc = (df["high"] - df.groupby("ticker")["close"].shift(1)).abs()
    lc = (df["low"] - df.groupby("ticker")["close"].shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    df["atr_20_pct"] = df.groupby("ticker")[tr.name].transform(lambda x: x.rolling(20).mean()) / df["close"] * 100

    # 均線
    df["ma20"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(20).mean())
    df["ma50"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(50).mean())
    df["ma200"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(200).mean())
    df["ma200_prev"] = df.groupby("ticker")["ma200"].shift(1)

    # 5日高低距離
    high5 = df.groupby("ticker")["high"].transform(lambda x: x.rolling(5).max())
    low5 = df.groupby("ticker")["low"].transform(lambda x: x.rolling(5).min())
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
    rs_tickers = rs_filtered["ticker"].tolist()

    # ---------- 2. 技術分析篩選 ----------
    price_df = price_df[price_df["ticker"].isin(rs_tickers)].copy()

    # 計算技術指標（向量化）
    price_df = compute_indicators_vectorized(price_df)

    # 技術分析條件篩選
    tech_filtered = price_df[
        (price_df["avg_value_10"] > 100_000_000) &
        (price_df["atr_20_pct"] > 1) &
        (price_df["close"] > price_df["ma20"]) &
        (price_df["close"] > price_df["ma50"]) &
        (price_df["ma50"] > price_df["ma200"]) &
        (price_df["ma200"] > price_df["ma200_prev"]) &
        (price_df["dist_high5_pct"] <= 10) &
        (price_df["dist_low5_pct"] <= 10)
    ]

    # 每個 ticker 只取最新一天
    tech_filtered = tech_filtered.sort_values(["ticker", "date"]).groupby("ticker", group_keys=False).tail(1)

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
