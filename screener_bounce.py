import pandas as pd
import pandas_ta_classic as ta  # 如果不用ta-lib也可以改成純pandas
import numpy as np

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist_bounce_10ma.csv"
OUTPUT_TXT = "watchlist_bounce_10ma.txt"
# ---------------- 技術指標計算 ---------------- #
def compute_indicators_vectorized(df):
    # 確保按ticker與日期排序
    df = df.sort_values(["ticker", "date"]).copy()

    # 10日平均成交值
    df["avg_value_10"] = df.groupby("ticker")["volume"].transform(lambda x: x.rolling(10).mean()) * df["close"]

    # ATR 20日百分比 (用pandas-ta)
    df["atr_20"] = df.groupby("ticker").apply(lambda g: ta.atr(g["high"], g["low"], g["close"], length=20)).reset_index(level=0, drop=True)
    df["atr_20_pct"] = df["atr_20"] / df["close"] * 100

    # 均線
    df["ma10"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(10).mean())
    df["ma20"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(20).mean())
    df["ma50"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(50).mean())
    df["ma200"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(200).mean())
    df["ma20_prev"] = df.groupby("ticker")["ma20"].shift(1)
    df["ma50_prev"] = df.groupby("ticker")["ma50"].shift(1)
    df["ma200_prev"] = df.groupby("ticker")["ma200"].shift(1)

    # 新高
    df["high10"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(10).max())
    df["high20"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(20).max())
    df["high50"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(50).max())
    df["52wH"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(252).max())

    return df

# ---------------- 主程式 ---------------- #
def main():
    # 讀檔
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    rs_df = pd.read_csv(RS_CSV)

    # ---------- 1. RS 篩選 ----------
    rs_filtered = rs_df[rs_df["RS"] > 80].copy()
    rs_filtered = rs_filtered.sort_values("RS", ascending=False)
    rs_tickers = rs_filtered["ticker"].tolist()

    # ---------- 2. 計算技術指標 ----------
    price_df = price_df[price_df["ticker"].isin(rs_tickers)]
    price_df = compute_indicators_vectorized(price_df)

    # ---------- 3. 技術分析篩選 ----------
    tech_filtered_10 = price_df[
 #       (price_df["avg_value_10"] > 10_000_000) &
 #       (price_df["atr_20_pct"] > 1) &
 #       (price_df["close"] > price_df["ma20"]) &
 #       (price_df["ma20"] > price_df["ma50"]) &
 #       (price_df["ma50"] > price_df["ma200"]) &
 #       (price_df["ma20"] > price_df["ma20_prev"]) &
 #       (price_df["ma50"] > price_df["ma50_prev"]) &
 #       (price_df["ma200"] > price_df["ma200_prev"]) &
 #       (price_df["high10"] == price_df["52wH"]) #&
        (price_df["close"] - price_df["ma10"] < price_df["atr_20"])
    ]

    # 只取符合條件的 ticker 名單
    selected_tickers_10 = tech_filtered_10["ticker"].unique()
    
    # 從完整 price_df 抓「真正最後一天」
    latest_df_10 = (
        price_df[price_df["ticker"].isin(selected_tickers_10)]
        .sort_values(["ticker", "date"])
        .groupby("ticker", group_keys=False)
        .tail(1)
    )
    
    # merge RS 並排序
    final_tickers_10 = (
        latest_df_10.merge(rs_filtered[["ticker", "RS"]], on="ticker", how="left")
        .sort_values("RS", ascending=False)[[
            "ticker", "RS", "close", "volume",
            "ma10", "ma20", "ma50", "ma200",
            "high10", "atr_20", "atr_20_pct", "avg_value_10"
        ]]
    )

    final_tickers_10 = final_tickers_10.round(3)

    # 輸出
    final_tickers_10.to_csv(OUTPUT_CSV, index=False, header=True)
    final_tickers_10["ticker"].to_csv(OUTPUT_TXT, index=False, header=False)

if __name__ == "__main__":
    main()
