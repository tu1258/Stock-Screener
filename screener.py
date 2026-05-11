import pandas as pd
import numpy as np
import os
os.makedirs("csv", exist_ok=True)
os.makedirs("txt", exist_ok=True)
PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_rs.csv"
OUTPUT_CSV = "output/technical_watchlist.csv"
OUTPUT_TXT = "output/technical_watchlist.txt"
UNIVERSE_CSV = "output/universe_watchlist.csv"
UNIVERSE_TXT = "output/universe_watchlist.txt"

# ---------------- 技術指標計算 ---------------- #
def compute_indicators_vectorized(df):
    df = df.sort_values(["ticker", "date"]).copy()

    # 10日平均成交值
    df["avg_value_10"] = df.groupby("ticker")["volume"].transform(lambda x: x.rolling(10).mean()) * df["close"] / 1_000_000

    # 均線
    df["ma5"]   = df.groupby("ticker")["close"].transform(lambda x: x.rolling(5,   min_periods=1).mean())
    df["ma20"]  = df.groupby("ticker")["close"].transform(lambda x: x.rolling(20,  min_periods=1).mean())
    df["ma50"]  = df.groupby("ticker")["close"].transform(lambda x: x.rolling(50,  min_periods=1).mean())
    df["ma200"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(200, min_periods=1).mean())

    # DR = high/low - 1（單日振幅%）
    df["dr"] = (df["high"] / df["low"] - 1) * 100

    # ADR
    df["adr5"] = df.groupby("ticker")["dr"].transform(lambda x: x.rolling(5).mean())
    df["adr20"] = df.groupby("ticker")["dr"].transform(lambda x: x.rolling(20).mean())

    df["avg_bar"] = (df['close'] + df['high'] + df['low']) / 3
    df['distance'] = abs(df["avg_bar"] - df["ma5"])

    # 價量
    df["chg"] = df.groupby("ticker")["close"].diff()
    df["money_flow"] = df["volume"] * df["chg"]
    df["money_flow_avg"] = df.groupby('ticker')['money_flow'].transform(lambda x: x.rolling(10).mean())
    df["trade_chg"] = df["close"] - df["open"]
    df["trade_money_flow"] = df["volume"] * df["trade_chg"]
    df["trade_money_flow_avg"] = df.groupby('ticker')['trade_money_flow'].transform(lambda x: x.rolling(10).mean())

    # 暴漲過濾：排除20日內最大單日DR後，剩19日的均值
    df["dr_sum_20"] = df.groupby("ticker")["dr"].transform(lambda x: x.rolling(20).sum())
    df["dr_max_20"] = df.groupby("ticker")["dr"].transform(lambda x: x.rolling(20).max())
    df["adr_excl"]  = (df["dr_sum_20"] - df["dr_max_20"]) / 19

    return df

# ---------------- 主程式 ---------------- #
def main():
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    rs_df = pd.read_csv(RS_CSV)

    # ---------- 1. RS 篩選 ----------
    rs_filtered = rs_df[rs_df["RS"] >= 90].copy()
    rs_filtered = rs_filtered.sort_values("score", ascending=False)
    rs_tickers = rs_filtered["ticker"].tolist()

    # ---------- 2. 計算技術指標 ----------
    price_df = price_df[price_df["ticker"].isin(rs_tickers)]
    price_df = compute_indicators_vectorized(price_df)
    latest_df = (
        price_df.sort_values(["ticker", "date"])
                .groupby("ticker", group_keys=False)
                .tail(1)
    )

    # ---------- 3. Technical Watchlist 篩選 ----------
    tech_filtered = latest_df[
        (latest_df["avg_value_10"] > 25) &
        (latest_df["adr20"] > 2.5) & (latest_df["adr20"] < 25) &
        (latest_df["avg_bar"] >= latest_df["ma50"]) &
        (latest_df["ma50"] >= latest_df["ma200"]) &
        (latest_df["distance"] < latest_df["adr20"] / 100 * latest_df["close"]) &
        (latest_df["dr"] < latest_df["adr5"]) &
        (latest_df["dr_max_20"] <= 25 * latest_df["adr_excl"])
    ]

    final_tickers = (
        tech_filtered.merge(rs_filtered[["ticker", "score", "RS"]], on="ticker", how="left")
        .sort_values("score", ascending=False)[[
            "ticker", "RS", "close", "volume",
            "distance", "dr", "adr5", "adr20", "avg_value_10"
        ]]
    )
    final_tickers = final_tickers.round(3)
    final_tickers.to_csv(OUTPUT_CSV, index=False, header=True)
    final_tickers["ticker"].to_csv(OUTPUT_TXT, index=False, header=False)

    # ---------- 4. Universe Watchlist 篩選 ----------
    universe_filtered = latest_df[
        (latest_df["avg_value_10"] > 25) &
        (latest_df["adr20"] > 2.5) & (latest_df["adr20"] < 25) &
        (latest_df["avg_bar"] >= latest_df["ma50"]) &
        (latest_df["ma50"] >= latest_df["ma200"]) &
        (latest_df["dr_max_20"] <= 25 * latest_df["adr_excl"])
    ]

    universe_tickers = (
        universe_filtered.merge(rs_filtered[["ticker", "score", "RS"]], on="ticker", how="left")
        .sort_values("score", ascending=False)[[
            "ticker", "RS", "close", "volume",
            "distance", "dr", "adr5", "adr20", "avg_value_10"
        ]]
    )
    universe_tickers = universe_tickers.round(3)
    universe_tickers.to_csv(UNIVERSE_CSV, index=False, header=True)
    universe_tickers["ticker"].to_csv(UNIVERSE_TXT, index=False, header=False)

if __name__ == "__main__":
    main()
