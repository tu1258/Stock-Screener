import pandas as pd
import numpy as np

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"

# ----------------- 技術指標計算 ----------------- #
def compute_indicators(df):
    df = df.sort_values("date")
    df["avg_value_10"] = df["close"] * df["volume"].rolling(10).mean()

    # ATR %
    df["hl"] = df["high"] - df["low"]
    df["hc"] = (df["high"] - df["close"].shift(1)).abs()
    df["lc"] = (df["low"] - df["close"].shift(1)).abs()
    df["tr"] = df[["hl","hc","lc"]].max(axis=1)
    df["atr_20_pct"] = df["tr"].rolling(20).mean() / df["close"] * 100

    # 移動平均線
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

# ----------------- 主程式 ----------------- #
def main():
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    rs_df = pd.read_csv(RS_CSV)
    rs_filtered = rs_df[rs_df["score"] > 90]
    rs_tickers = rs_filtered["ticker"].tolist()
    
    price_filtered = price_df[price_df["ticker"].isin(rs_tickers)]
    
    latest_price = (
        price_filtered.groupby("ticker")
        .apply(compute_indicators)
        .groupby("ticker")
        .tail(1)
        .reset_index(drop=True)
    )
    
    screened = latest_price[
        (latest_price["avg_value_10"] > 100_000_000) &
        (latest_price["atr_20_pct"] > 1) &
        (latest_price["close"] > latest_price["ma20"]) &
        (latest_price["close"] > latest_price["ma50"]) &
        (latest_price["ma50"] > latest_price["ma200"]) &
        (latest_price["ma200"] > latest_price["ma200_prev"]) &
        (latest_price["dist_high5_pct"] <= 10) &
        (latest_price["dist_low5_pct"] <= 10)
    ]
    
    # 如果想保留 RS 排序，可以先 merge RS 再排序
    screened = screened.merge(
        rs_filtered[["ticker", "RS"]],
        on="ticker",
        how="left"
    ).sort_values("RS", ascending=False)


    # 排序 & 輸出
    screened.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved screened result to {OUTPUT_CSV}, rows={len(screened)}")

if __name__ == "__main__":
    main()
