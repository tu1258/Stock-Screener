import pandas as pd
import numpy as np
import os

os.makedirs("csv", exist_ok=True)
os.makedirs("txt", exist_ok=True)

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV_50 = "csv/watchlist_bounce_50ma.csv"
OUTPUT_TXT_50 = "txt/watchlist_bounce_50ma.txt"

# ---------------- 技術指標計算 ---------------- #
def compute_indicators_vectorized(df):
    # 確保按ticker與日期排序
    df = df.sort_values(["ticker", "date"]).copy()

    # 10日平均成交值
    df["avg_value_10"] = df.groupby("ticker")["volume"].transform(lambda x: x.rolling(10).mean()) * df["close"] / 1_000_000

    # ATR 14日百分比
    df['prev_close'] = df.groupby('ticker')['close'].shift(1)
    df['tr'] = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['prev_close']).abs(),
        (df['low'] - df['prev_close']).abs()
    ], axis=1).max(axis=1)
    df["tr_pct"] = df["tr"] / df['prev_close'] * 100
    
    df["atr_14"] = df.groupby('ticker')['tr'].transform(lambda x: x.rolling(14).mean())
    df["atr_14_pct"] = df.groupby('ticker')['tr_pct'].transform(lambda x: x.rolling(14).mean())

    # 均線
    df["ma20"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(20).mean())
    df["ma50"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(50).mean())
    df["ma200"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(200).mean())

    # 多頭排列
    df["bullish"] = (df["close"] > df["ma50"]) & (df["ma20"] > df["ma50"]) & (df["ma50"] > df["ma200"])

    # 計算連續多頭排列天數
    def bullish_streak(x):
        streak = []
        count = 0
        for val in x:
            if val:
                count += 1
            else:
                count = 0
            streak.append(count)
        return pd.Series(streak, index=x.index)

    df["bullish_count"] = df.groupby("ticker")["bullish"].transform(bullish_streak)

    # 新高
    df["high50"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(50).max())
    df["52wH"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(252).max())

    return df

# ---------------- 主程式 ---------------- #
def main():
    # 讀檔
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    rs_df = pd.read_csv(RS_CSV)

    # ---------- 1. RS 篩選 ----------
    rs_filtered = rs_df[rs_df["RS"] >= 90].copy()
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
    tech_filtered_50 = latest_df[
        (latest_df["avg_value_10"] > 100) &
        (latest_df["atr_14_pct"] > 1) & (latest_df["atr_14_pct"] < 10) &
        (latest_df["bullish_count"] > 20) &
        (abs(latest_df["close"] - latest_df["ma50"]) < latest_df["atr_14"]) & 
        (latest_df["high50"] == latest_df["52wH"])
    ]
    
    # merge RS 並排序
    final_tickers_50 = (
         tech_filtered_50.merge(rs_filtered[["ticker", "RS"]], on="ticker", how="left")
        .sort_values("RS", ascending=False)[[
            "ticker", "RS", "close", "volume",
            "bullish_count", "ma20", "ma50", "ma200", "52wH",
            "atr_14_pct", "avg_value_10"
        ]]
    )
    final_tickers_50 = final_tickers_50.round(3)

    # 輸出
    final_tickers_50.to_csv(OUTPUT_CSV_50, index=False, header=True)
    final_tickers_50["ticker"].to_csv(OUTPUT_TXT_50, index=False, header=False)

if __name__ == "__main__":
    main()
