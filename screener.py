import pandas as pd
import pandas_ta_classic as ta  # 如果不用ta-lib也可以改成純pandas
import numpy as np

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"
OUTPUT_TXT = "watchlist.txt"
# ---------------- 技術指標計算 ---------------- #
def compute_indicators_vectorized(df):
    # 確保按ticker與日期排序
    df = df.sort_values(["ticker", "date"]).copy()

    # 10日平均成交值
    df["avg_value_10"] = df.groupby("ticker")["volume"].transform(lambda x: x.rolling(10).mean()) * df["close"] / 1_000_000

    # ATR 20日百分比 (用pandas-ta)
    df["atr_20"] = df.groupby("ticker").apply(lambda g: ta.atr(g["high"], g["low"], g["close"], length=20)).reset_index(level=0, drop=True)
    df["atr_20_pct"] = df["atr_20"] / df["close"] * 100

    # 均線
    df["ma20"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(20).mean())
    df["ma50"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(50).mean())
    df["ma200"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(200).mean())
    df["ma200_prev"] = df.groupby("ticker")["ma200"].shift(1)

    # 5日高低距離
    df["high5"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(5).max())
    df["low5"] = df.groupby("ticker")["low"].transform(lambda x: x.rolling(5).min())
    df["dist_high5_pct"] = (df["high5"] - df["close"]) / df["close"] * 100
    df["dist_low5_pct"] = (df["close"] - df["low5"]) / df["close"] * 100

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

    # ---------- 2. 計算技術指標 ----------
    price_df = price_df[price_df["ticker"].isin(rs_tickers)]
    price_df = compute_indicators_vectorized(price_df)
    latest_df = (
        price_df.sort_values(["ticker", "date"])
                .groupby("ticker", group_keys=False)
                .tail(1)
    )
    # ---------- 3. 技術分析篩選 ----------
    tech_filtered = latest_df[
        (latest_df["avg_value_10"] > 10) &
        (latest_df["atr_20_pct"] > 1) &
        (latest_df["close"] > latest_df["ma50"]) &
        (latest_df["ma50"] > latest_df["ma200"]) &
        (latest_df["ma200"] > latest_df["ma200_prev"]) &
        (latest_df["dist_high5_pct"] <= 10) &
        (latest_df["dist_low5_pct"] <= 10)
    ]

    # merge RS 並排序
    final_tickers = (
        tech_filtered.merge(rs_filtered[["ticker", "RS"]], on="ticker", how="left")
        .sort_values("RS", ascending=False)[[
            "ticker", "RS", "close", "volume",
            "ma20", "ma50", "ma200",
            "atr_20_pct", "dist_high5_pct", "dist_low5_pct", "avg_value_10"
        ]]
    )

    final_tickers = final_tickers.round(3)

    # 輸出
    final_tickers.to_csv(OUTPUT_CSV, index=False, header=True)
    final_tickers["ticker"].to_csv(OUTPUT_TXT, index=False, header=False)

if __name__ == "__main__":
    main()
