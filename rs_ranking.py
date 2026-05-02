import os
import pandas as pd
import numpy as np

DIR             = os.path.dirname(os.path.realpath(__file__))
PRICE_DATA_CSV  = os.path.join(DIR, "stock_data.csv")
OUTPUT_CSV      = os.path.join(DIR, "stock_rs.csv")
INDUSTRY_CSV    = os.path.join(DIR, "ticker_industry.csv")
INDUSTRY_RS_CSV = os.path.join(DIR, "industry_rs.csv")
MIN_DATA_POINTS = 21

def geo_monthly_return(closes, n):
    required = n * 21
    if len(closes) < required:
        return None
    prices = closes.tail(required)
    cumret = prices.iloc[-1] / prices.iloc[0] - 1
    return (1 + cumret) ** (1 / n) - 1

def strength(closes):
    try:
        months = [1, 2, 3, 4, 5, 6]
        values = [geo_monthly_return(closes, n) for n in months]
        valid  = [v for v in values if v is not None]
        if not valid:
            return 0
        return sum(valid) / len(valid)
    except Exception:
        return 0

def main():
    df_all  = pd.read_csv(PRICE_DATA_CSV, parse_dates=["date"])
    tickers = df_all["ticker"].unique()

    relative_strengths = []
    for ticker in tickers:
        df     = df_all[df_all["ticker"] == ticker].sort_values("date")
        closes = df["close"].reset_index(drop=True)
        if len(closes) < MIN_DATA_POINTS:
            continue
        score = strength(closes)
        relative_strengths.append({
            "ticker": ticker,
            "score":  round(score * 100, 2),
            "RS":     100.,
        })

    df = pd.DataFrame(relative_strengths, columns=["ticker", "score", "RS"])
    df["RS"] = pd.qcut(df["score"], 100, labels=False, duplicates="drop")
    df = df.sort_values("score", ascending=False)
    df.to_csv(OUTPUT_CSV, index=False)

    if os.path.exists(INDUSTRY_CSV):
        df_ind      = pd.read_csv(INDUSTRY_CSV)
        df_merged   = df.merge(df_ind[["ticker", "sector", "industry"]], on="ticker", how="inner")
        industry_rs = (
            df_merged.groupby(["industry", "sector"])
            .agg(avg_rs=("RS", "mean"), ticker_count=("ticker", "count"))
            .reset_index()
            .sort_values("avg_rs", ascending=False)
        )
        industry_rs["avg_rs"] = industry_rs["avg_rs"].round(1)
        industry_rs.to_csv(INDUSTRY_RS_CSV, index=False)
        print(f"Saved {INDUSTRY_RS_CSV}，共 {len(industry_rs)} 個 industry")
    else:
        print(f"⚠️ 找不到 {INDUSTRY_CSV}，跳過 industry RS 計算")

    df_tickers      = pd.read_csv(os.path.join(DIR, "stock_ticker.csv"))
    missing_tickers = set(df_tickers["ticker"]) - set(df["ticker"])
    if missing_tickers:
        print("\n⚠️ 以下 ticker 在 stock_data.csv 沒有資料 / RS 無法計算:")
        for t in missing_tickers:
            print(t)
    else:
        print("\n✅ 所有 ticker 都有資料")

if __name__ == "__main__":
    main()
