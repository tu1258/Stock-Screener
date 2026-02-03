import pandas as pd
import numpy as np

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"


def compute_indicators(df):
    df = df.sort_values("date")

    df["avg_value_10"] = df["close"] * df["volume"].rolling(10).mean()

    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift(1)).abs()
    lc = (df["low"] - df["close"].shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    df["atr_20_pct"] = tr.rolling(20).mean() / df["close"] * 100

    df["ma20"] = df["close"].rolling(20).mean()
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()
    df["ma200_prev"] = df["ma200"].shift(1)

    df["high5"] = df["high"].rolling(5).max()
    df["low5"] = df["low"].rolling(5).min()
    df["dist_high5_pct"] = (df["high5"] - df["close"]) / df["high5"] * 100
    df["dist_low5_pct"] = (df["close"] - df["low5"]) / df["low5"] * 100

    return df


def main():
    # ===== 1️⃣ RS universe（只篩一次）=====
    rs_df = pd.read_csv(RS_CSV)
    rs_universe = rs_df.loc[rs_df["RS"] > 90, ["ticker", "RS"]]

    # ===== 2️⃣ price 只留下 RS > 90 的股票 =====
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    price_df = price_df[price_df["ticker"].isin(rs_universe["ticker"])]

    # ===== 3️⃣ 技術指標 + 只取最新一根 =====
    latest = (
        price_df
        .groupby("ticker", group_keys=False)
        .apply(compute_indicators)
        .groupby("ticker")
        .tail(1)
        .reset_index(drop=True)
    )

    # ===== 4️⃣ 技術面篩選 =====
    screened = latest[
        (latest["avg_value_10"] > 100_000_000) &
        (latest["atr_20_pct"] > 1) &
        (latest["close"] > latest["ma20"]) &
        (latest["close"] > latest["ma50"]) &
        (latest["ma50"] > latest["ma200"]) &
        (latest["ma200"] > latest["ma200_prev"]) &
        (latest["dist_high5_pct"] <= 10) &
        (latest["dist_low5_pct"] <= 10)
    ]

    # ===== 5️⃣ 把 RS 貼回來（不再篩）=====
    screened = screened.merge(rs_universe, on="ticker", how="left")

    screened = screened.sort_values("RS", ascending=False)
    screened.to_csv(OUTPUT_CSV, index=False)

    print(f"Saved {OUTPUT_CSV}, rows={len(screened)}")


if __name__ == "__main__":
    main()
