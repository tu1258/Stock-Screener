import os
import pandas as pd
import numpy as np
import yfinance as yf

DIR = os.path.dirname(os.path.realpath(__file__))
PRICE_DATA_CSV  = os.path.join(DIR, "stock_data.csv")
OUTPUT_CSV      = os.path.join(DIR, "stock_rs.csv")
INDUSTRY_CSV    = os.path.join(DIR, "ticker_industry.csv")
INDUSTRY_RS_CSV = os.path.join(DIR, "industry_rs.csv")

REFERENCE_TICKER = "^GSPC"
MIN_DATA_POINTS  = 63

def relative_strength(closes, closes_ref):
    rs_stock = strength(closes)
    rs_ref   = strength(closes_ref)
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    return round(rs, 2)

def strength(closes):
    try:
        m1  = geo_monthly_return(closes, 1)
        m3  = geo_monthly_return(closes, 3)
        m6  = geo_monthly_return(closes, 6)
        m12 = geo_monthly_return(closes, 12)

        return m1/4 + m3/4 + m6/4 + m12/4
    except Exception:
        return 0

def geo_monthly_return(closes, n):
    """取過去n個月的幾何平均月報酬"""
    length  = min(len(closes), n * 21)
    prices  = closes.tail(length)
    cumret  = prices.iloc[-1] / prices.iloc[0] - 1
    # 幾何平均：(1 + 累積報酬)^(1/n) - 1
    return (1 + cumret) ** (1 / n) - 1

def main():
    df_all  = pd.read_csv(PRICE_DATA_CSV, parse_dates=["date"])
    tickers = df_all["ticker"].unique()

    df_ref = yf.download(REFERENCE_TICKER, period="1y", progress=False)
    if df_ref.empty:
        raise RuntimeError("Failed to download SPX from Yahoo Finance")
    closes_ref = df_ref["Close"].reset_index(drop=True)

    relative_strengths = []
    for ticker in tickers:
        if ticker == REFERENCE_TICKER:
            continue
        df = df_all[df_all["ticker"] == ticker].sort_values("date")
        closes = df["close"].reset_index(drop=True)
        if len(closes) < MIN_DATA_POINTS:
            continue
        rs_score = relative_strength(closes, closes_ref)
        if rs_score > 1000:
            continue
        relative_strengths.append({
            "ticker": ticker,
            "score":  rs_score,
            "RS":     100.,
        })

    df = pd.DataFrame(relative_strengths, columns=["ticker", "score", "RS"])
    df["RS"] = pd.qcut(df["score"], 100, labels=False, duplicates="drop")
    df = df.sort_values("score", ascending=False)
    df.to_csv(OUTPUT_CSV, index=False)

    # industry RS
    if os.path.exists(INDUSTRY_CSV):
        df_ind    = pd.read_csv(INDUSTRY_CSV)
        df_merged = df.merge(df_ind[["ticker", "sector", "industry"]], on="ticker", how="inner")

        industry_rs = (
            df_merged.groupby(["industry", "sector"])
            .agg(
                avg_rs       = ("RS", "mean"),
                ticker_count = ("ticker", "count"),
            )
            .reset_index()
            .sort_values("avg_rs", ascending=False)
        )
        industry_rs["avg_rs"] = industry_rs["avg_rs"].round(1)
        industry_rs.to_csv(INDUSTRY_RS_CSV, index=False)
        print(f"Saved {INDUSTRY_RS_CSV}，共 {len(industry_rs)} 個 industry")
    else:
        print(f"⚠️ 找不到 {INDUSTRY_CSV}，跳過 industry RS 計算")

    # 檢查漏掉的 ticker
    df_tickers = pd.read_csv(os.path.join(DIR, "stock_ticker.csv"))
    missing_tickers = set(df_tickers["ticker"]) - set(df["ticker"])
    if missing_tickers:
        print("\n⚠️ 以下 ticker 在 stock_data.csv 沒有資料 / RS 無法計算:")
        for t in missing_tickers:
            print(t)
    else:
        print("\n✅ 所有 ticker 都有資料")

if __name__ == "__main__":
    main()
