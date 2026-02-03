import pandas as pd
import numpy as np

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"


def compute_indicators(df):
    df = df.sort_values("date")

    # 10 日平均成交值
    df["avg_value_10"] = df["close"] * df["volume"].rolling(10).mean()

    # ATR 20 (%)
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    df["atr_20_pct"] = tr.rolling(20).mean() / df["close"] * 100

    # Moving averages
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()
    df["ma200_prev"] = df["ma200"].shift(1)

    # 5 日高低
    high5 = df["high"].rolling(5).max()
    low5 = df["low"].rolling(5).min()
    df["dist_high5_pct"] = (high5 - df["close"]) / high5 * 100
    df["dist_low5_pct"] = (df["close"] - low5) / low5 * 100

    return df


def main():
    # ======================
    # 1. RS 第一關
    # ======================
    rs_df = pd.read_csv(RS_CSV)

    rs_pass = (
        rs_df[rs_df["RS"] > 90]
        .sort_values("RS", ascending=False)
        .reset_index(drop=True)
    )

    rs_tickers = rs_pass["ticker"].tolist()

    # ======================
    # 2. 技術分析
    # ======================
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    price_df = price_df[price_df["ticker"].isin(rs_tickers)]

    latest = (
        price_df
        .groupby("ticker", group_keys=False)
        .apply(compute_indicators)
        .groupby("ticker", group_keys=False)
        .tail(1)
        .reset_index(drop=True)
    )

    tech_filtered = latest[
        (latest["avg_value_10"] > 100_000_000) &
        (latest["atr_20_pct"] > 1) &
        (latest["close"] > latest["ma20"]) &
        (latest["close"] > latest["ma50"]) &
        (latest["ma50"] > latest["ma200"]) &
        (latest["ma200"] > latest["ma200_prev"]) &
        (latest["dist_high5_pct"] <= 10) &
        (latest["dist_low5_pct"] <= 10)
    ]

    # ======================
    # 3. 用 RS 排序（但不重算）
    # ======================
    final = (
        tech_filtered[["ticker"]]
        .merge(rs_pass[["ticker", "RS"]], on="ticker", how="left")
        .sort_values("RS", ascending=False)
        [["ticker"]]
    )

    final.to_csv(OUTPUT_CSV, index=False, header=False)
    print(f"Saved {OUTPUT_CSV}, rows={len(final)}")


if __name__ == "__main__":
    main()
