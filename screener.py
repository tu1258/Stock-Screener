import pandas as pd
import numpy as np

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"


def compute_indicators(df):
    """
    計算各種技術指標
    - 10日平均成交值 (volume * close)
    - 20日ATR%
    - 20/50/200日均線
    - 5日高低距離
    """
    df = df.sort_values("date")
    df["avg_value_10"] = df["close"] * df["volume"].rolling(10).mean()

    # ATR
    df["hl"] = df["high"] - df["low"]
    df["hc"] = (df["high"] - df["close"].shift(1)).abs()
    df["lc"] = (df["low"] - df["close"].shift(1)).abs()
    df["tr"] = df[["hl", "hc", "lc"]].max(axis=1)
    df["atr_20_pct"] = (df["tr"].rolling(20).mean() / df["close"]) * 100

    # Moving averages
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()
    df["ma200_prev"] = df["ma200"].shift(1)

    # 5日高低距離
    df["high5"] = df["high"].rolling(5).max()
    df["low5"] = df["low"].rolling(5).min()
    df["dist_high5_pct"] = (df["high5"] - df["close"]) / df["high5"] * 100
    df["dist_low5_pct"] = (df["close"] - df["low5"]) / df["low5"] * 100

    return df


def main():
    # 讀資料
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    rs_df = pd.read_csv(RS_CSV)

    # 統一欄位名稱小寫
    price_df.columns = price_df.columns.str.strip().str.lower()
    rs_df.columns = rs_df.columns.str.strip().str.lower()

    # 只取最新一筆做篩選
    latest_price = (
        price_df.sort_values("date")
        .groupby("ticker")
        .apply(compute_indicators)
        .groupby("ticker")
        .tail(1)
        .reset_index(drop=True)
    )

    # ===============================
    # 第一階段: 用技術指標篩選
    # ===============================
    screened_price = latest_price[
        (latest_price["avg_value_10"] > 100_000_000) &
        (latest_price["atr_20_pct"] > 1) &
        (latest_price["close"] > latest_price["ma20"]) &
        (latest_price["close"] > latest_price["ma50"]) &
        (latest_price["ma50"] > latest_price["ma200"]) &
        (latest_price["ma200"] > latest_price["ma200_prev"]) &
        (latest_price["dist_high5_pct"] <= 10) &
        (latest_price["dist_low5_pct"] <= 10)
    ]

    # ===============================
    # 第二階段: 用 RS 篩選
    # ===============================
    screened = rs_df[
        (rs_df["RS rank"] > 90) &
        (rs_df["ticker"].isin(screened_price["ticker"]))
    ]

    # 把技術指標也合併回來
    final_df = screened.merge(
        screened_price,
        on="ticker",
        how="left"
    )

    # 排序
    final_df = final_df.sort_values("RS rank", ascending=False)

    # 輸出
    final_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved screened result to {OUTPUT_CSV}, rows={len(final_df)}")


if __name__ == "__main__":
    main()
