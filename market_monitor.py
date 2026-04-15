import pandas as pd
import os

os.makedirs("txt", exist_ok=True)

PRICE_CSV = "stock_data.csv"
OUTPUT_TXT = "txt/market_monitor.txt"

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

    # ── 取最新交易日 ──────────────────────────────────
    latest_date = df["date"].max()
    latest_df   = df[df["date"] == latest_date].copy()
    total       = len(latest_df)

    # ── 1. 在 50MA 以上/以下 ──────────────────────────
    above_ma50 = (latest_df["close"] > latest_df["ma50"]).sum()
    below_ma50 = (latest_df["close"] < latest_df["ma50"]).sum()
    pct_above  = above_ma50 / total * 100 if total else 0
    pct_below  = below_ma50 / total * 100 if total else 0

    # ── 2. 52週新高 / 新低 ───────────────────────────
    new_high = (latest_df["high"] >= latest_df["high_52w"]).sum()
    new_low  = (latest_df["low"]  <= latest_df["low_52w"]).sum()

    # ── 3 & 4. 漲跌4+% 統計 ──────────────────────────
    all_dates    = sorted(df["date"].unique())
    latest_idx   = all_dates.index(latest_date)
    window_dates = all_dates[max(0, latest_idx - 4): latest_idx + 1]

    surge_total  = 0
    plunge_total = 0
    for d in window_dates:
        day_df = df[df["date"] == d]
        surge_total  += (day_df["pct_chg"] >= 4).sum()
        plunge_total += (day_df["pct_chg"] <= -4).sum()

    surge_today  = (latest_df["pct_chg"] >= 4).sum()
    plunge_today = (latest_df["pct_chg"] <= -4).sum()

    # ── 組報告 ────────────────────────────────────────
    report = ""
    report += "=" * 52 + "\n"
    report += f"  Market Monitor  |  {latest_date.date()}\n"
    report += "=" * 52 + "\n\n"

    report += "【1】50MA 多空比\n"
    report += f"  在50MA以上：{above_ma50:>5} 檔  ({pct_above:.1f}%)\n"
    report += f"  在50MA以下：{below_ma50:>5} 檔  ({pct_below:.1f}%)\n"
    report += f"  統計總檔數：{total:>5} 檔\n\n"

    report += "【2】52週新高 / 新低\n"
    report += f"  52週新高：{new_high:>5} 檔\n"
    report += f"  52週新低：{new_low:>5} 檔\n\n"

    report += "【3】漲跌幅 ≥4%\n"
    report += f"  今日      漲4%+: {surge_today:>4}  跌4%+: {plunge_today:>4}\n"
    report += f"  5日合計   漲4%+: {surge_total:>4}  跌4%+: {plunge_total:>4}\n"
    report += "=" * 52 + "\n"

    print(report)
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"✅ 已輸出：{OUTPUT_TXT}")

if __name__ == "__main__":
    main()
