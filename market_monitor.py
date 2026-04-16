import pandas as pd
import os

os.makedirs("csv", exist_ok=True)

PRICE_CSV  = "stock_data.csv"
OUTPUT_CSV = "output/market_monitor.csv"

def main():
    df = pd.read_csv(PRICE_CSV, parse_dates=["date"])
    df = df.sort_values(["ticker", "date"])

    # ── 均線 ──────────────────────────────────────────
    df["ma50"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(50).mean())

    # ── 52週最高/最低 ─────────────────────────────────
    df["high_52w"] = df.groupby("ticker")["high"].transform(lambda x: x.rolling(252).max())
    df["low_52w"]  = df.groupby("ticker")["low"].transform(lambda x: x.rolling(252).min())

    # ── 日漲跌幅 ──────────────────────────────────────
    df["pct_chg"] = df.groupby("ticker")["close"].pct_change() * 100

    # ── 取最近10個交易日 ──────────────────────────────
    latest_date  = df["date"].max()
    all_dates    = sorted(df["date"].unique())
    latest_idx   = all_dates.index(latest_date)
    window_dates = all_dates[max(0, latest_idx - 9): latest_idx + 1]

    # ── 逐日計算 ──────────────────────────────────────
    rows = []
    for d in reversed(window_dates):
        day_df = df[df["date"] == d]

        above = (day_df["close"] > day_df["ma50"]).sum()
        below = (day_df["close"] < day_df["ma50"]).sum()

        n_high = (day_df["high"] >= day_df["high_52w"]).sum()
        n_low  = (day_df["low"]  <= day_df["low_52w"]).sum()

        surge  = (day_df["pct_chg"] >= 4).sum()
        plunge = (day_df["pct_chg"] <= -4).sum()

        rows.append({
            "date"          : pd.Timestamp(d).date(),
            "50ma上"        : above,
            "50ma下"        : below,
            "50ma上/50ma下" : round(above / below, 2) if below else None,
            "52wH"          : n_high,
            "52wL"          : n_low,
            "52wH/52wL"     : round(n_high / n_low, 2) if n_low else None,
            "漲4%+"         : surge,
            "跌4%+"         : plunge,
            "漲/跌"         : round(surge / plunge, 2) if plunge else None,
        })

    result = pd.DataFrame(rows)
    result.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")

    print(result.to_string(index=False))
    print(f"\n✅ 已輸出：{OUTPUT_CSV}")

if __name__ == "__main__":
    main()
