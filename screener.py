import pandas as pd

PRICE_CSV = "stock_data.csv"
RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"


def compute_indicators(df):
    df = df.sort_values("date")

    df["avg_value_10"] = df["close"] * df["volume"].rolling(10).mean()

    # ATR %
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    df["atr_20_pct"] = tr.rolling(20).mean() / df["close"] * 100

    # MA
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()
    df["ma200_prev"] = df["ma200"].shift()

    # 5-day range
    high5 = df["high"].rolling(5).max()
    low5 = df["low"].rolling(5).min()
    df["dist_high5_pct"] = (high5 - df["close"]) / high5 * 100
    df["dist_low5_pct"] = (df["close"] - low5) / low5 * 100

    return df


def main():
    # ===== RS universe =====
    rs_df = pd.read_csv(RS_CSV)
    rs_df.columns = rs_df.columns.str.strip()
    rs_universe = rs_df.loc[rs_df["RS"] > 90, ["ticker", "RS"]]

    # ===== Price data =====
    price_df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    price_df.columns = price_df.columns.str.strip()
    price_df = price_df[price_df["ticker"].isin(rs_universe["ticker"])]

    # ===== Indicators + latest bar =====
    latest = (
        price_df
        .groupby("ticker", group_keys=False)
        .apply(compute_indicators)
        .groupby("ticker")
        .tail(1)
        .reset_index(drop=True)
    )

    # ===== Technical filter =====
    tech = latest[
        (latest["avg_value_10"] > 100_000_000) &
        (latest["atr_20_pct"] > 1) &
        (latest["close"] > latest["ma20"]) &
        (latest["close"] > latest["ma50"]) &
        (latest["ma50"] > latest["ma200"]) &
        (latest["ma200"] > latest["ma200_prev"]) &
        (latest["dist_high5_pct"] <= 10) &
        (latest["dist_low5_pct"] <= 10)
    ]

    # ===== RS sort only =====
    watchlist = (
        tech.merge(rs_universe, on="ticker", how="left")
        .sort_values("RS", ascending=False)
        [["ticker"]]
    )

    watchlist.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {OUTPUT_CSV}, tickers={len(watchlist)}")


if __name__ == "__main__":
    main()
