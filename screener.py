import pandas as pd
import numpy as np
import os

os.makedirs("csv", exist_ok=True)
os.makedirs("txt", exist_ok=True)

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "csv/watchlist.csv"
OUTPUT_TXT = "txt/watchlist.txt"
# ---------------- 技術指標計算 ---------------- #
def compute_indicators_vectorized(df):
    # 確保按ticker與日期排序
    df = df.sort_values(["ticker", "date"]).copy()

    # 10日平均成交值
    df["avg_value_10"] = df.groupby("ticker")["volume"].transform(lambda x: x.rolling(10).mean()) * df["close"] / 1_000_000
    
    # ATR
    df['prev_close'] = df.groupby('ticker')['close'].shift(1)
    df['tr'] = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['prev_close']).abs(),
        (df['low'] - df['prev_close']).abs()
    ], axis=1).max(axis=1)

    df['atr_5'] = df.groupby('ticker')['tr'].transform(lambda x: x.rolling(5).mean())
    df['atr_10'] = df.groupby('ticker')['tr'].transform(lambda x: x.rolling(10).mean())
    df['atr_50'] = df.groupby('ticker')['tr'].transform(lambda x: x.rolling(50).mean())
    df["atr_14_pct"] = df.groupby('ticker')['tr'].transform(lambda x: x.rolling(14).mean()) / df["close"] * 100

    # 均線
    df["ma20"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(20).mean())
    df["ma50"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(50).mean())
    df["ma200"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(200).mean())

    # 5日高低距離
    df["high5"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(5).max())
    df["low5"] = df.groupby("ticker")["low"].transform(lambda x: x.rolling(5).min())
    df["range_5"] = df["high5"] - df["low5"]
    df["high10"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(10).max())
    df["low10"] = df.groupby("ticker")["low"].transform(lambda x: x.rolling(10).min())
    df["range_10"] = df["high10"] - df["low10"]

    # 價量
    df["chg"] = df.groupby("ticker")["close"].diff()
    df["up_vol"] = np.where(df["chg"] > 0, df["volume"] * df["chg"], 0)
    df["down_vol"] = np.where(df["chg"] < 0, df["volume"] * -df["chg"], 0)
    df["up_vol_5"] = df.groupby("ticker")["up_vol"].transform(lambda x: x.rolling(5).sum()) / 1_000
    df["down_vol_5"] = df.groupby("ticker")["down_vol"].transform(lambda x: x.rolling(5).sum()) / 1_000
    df["up_vol_10"] = df.groupby("ticker")["up_vol"].transform(lambda x: x.rolling(10).sum()) / 1_000
    df["down_vol_10"] = df.groupby("ticker")["down_vol"].transform(lambda x: x.rolling(10).sum()) / 1_000

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
        (latest_df["avg_value_10"] > 100) &
        (latest_df["atr_14_pct"] > 1) & (latest_df["atr_14_pct"] < 10) &
        (latest_df["close"] > latest_df["ma50"]) &
        (latest_df["ma50"] > latest_df["ma200"]) &
        (latest_df["up_vol_10"] > latest_df["down_vol_10"]) & 
        (latest_df["up_vol_5"] > latest_df["down_vol_5"]) 
        #(latest_df["range_5"] < latest_df["atr_5"] * 2.5)
        #(latest_df["range_10"] < latest_df["atr_10"] * 2.5)
    ]

    # merge RS 並排序
    final_tickers = (
        tech_filtered.merge(rs_filtered[["ticker", "RS"]], on="ticker", how="left")
        .sort_values("RS", ascending=False)[[
            "ticker", "RS", "close", "volume",
            "ma20", "ma50", "ma200",
            "atr_5", "atr_10", "atr_50", "range_5", "range_10",
            "up_vol_5", "down_vol_5", "up_vol_10", "down_vol_10", "avg_value_10"
        ]]
    )

    final_tickers = final_tickers.round(3)

    # 輸出
    final_tickers.to_csv(OUTPUT_CSV, index=False, header=True)
    final_tickers["ticker"].to_csv(OUTPUT_TXT, index=False, header=False)

if __name__ == "__main__":
    main()
