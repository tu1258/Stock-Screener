import os
import csv
import json
import time
import datetime
import requests
import pandas as pd

RS_CSV            = "stock_rs.csv"
STOCK_DATA_CSV    = "stock_data.csv"
INPUT_TXT         = "output/technical_watchlist.txt"
OUTPUT_NEWS_CACHE = "output/news_cache.json"
MIN_AVG_VALUE_10D = 100
MIN_ATR_PCT       = 2.5

TODAY         = datetime.date.today().strftime("%Y-%m-%d")
ONE_MONTH_AGO = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")


def load_rs95_liquid(rs_csv, stock_data_csv):
    rs_map = {}
    with open(rs_csv, newline="") as f:
        for row in csv.DictReader(f):
            try:
                rs_int = int(float(row.get("RS", 0)))
                if rs_int >= 95:
                    rs_map[row["ticker"].upper()] = rs_int
            except (ValueError, KeyError):
                pass

    if not os.path.exists(stock_data_csv) or not rs_map:
        return sorted(rs_map.items(), key=lambda x: -x[1])

    price_df = pd.read_csv(stock_data_csv, parse_dates=["date"],
                           usecols=["ticker", "date", "high", "low", "close", "volume"])
    price_df = price_df[price_df["ticker"].str.upper().isin(rs_map)]
    price_df = price_df.sort_values(["ticker", "date"])
    price_df["daily_value"] = price_df["close"] * price_df["volume"] / 1_000_000
    avg_val = (
        price_df.groupby("ticker")["daily_value"]
        .apply(lambda x: x.tail(10).mean())
        .reset_index()
        .rename(columns={"daily_value": "avg_value_10"})
    )
    avg_val["ticker"] = avg_val["ticker"].str.upper()
    liquid_set = set(avg_val[avg_val["avg_value_10"] >= MIN_AVG_VALUE_10D]["ticker"].tolist())

    # TR / ATR
    price_df["prev_close"] = price_df.groupby("ticker")["close"].shift(1)
    price_df["tr"] = pd.concat([
        price_df["high"] - price_df["low"],
        (price_df["high"] - price_df["prev_close"]).abs(),
        (price_df["low"]  - price_df["prev_close"]).abs()
    ], axis=1).max(axis=1)
    price_df["tr_pct"] = price_df["tr"] / price_df["prev_close"] * 100
    price_df["atr_14_pct"] = price_df.groupby("ticker")["tr_pct"].transform(lambda x: x.rolling(14).mean())

    # 暴漲過濾
    price_df["tr_pct_sum_14"] = price_df.groupby("ticker")["tr_pct"].transform(lambda x: x.rolling(14).sum())
    price_df["tr_pct_max_14"] = price_df.groupby("ticker")["tr_pct"].transform(lambda x: x.rolling(14).max())
    price_df["atr_pct_13_excl"] = ((price_df["tr_pct_sum_14"] - price_df["tr_pct_max_14"]) / 13)

    latest = price_df.groupby("ticker").tail(1).copy()
    latest["ticker"] = latest["ticker"].str.upper()
    latest = latest.set_index("ticker")

    atr_ok_set   = set(latest[latest["atr_14_pct"] > MIN_ATR_PCT].index)
    no_spike_set = set(latest[latest["tr_pct_max_14"] <= 25 * latest["atr_pct_13_excl"]].index)

    valid_set = liquid_set #& atr_ok_set & no_spike_set
    return sorted([(t, rs) for t, rs in rs_map.items() if t in valid_set], key=lambda x: -x[1])


def fetch_finnhub_news(ticker, api_key):
    try:
        r = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params={"symbol": ticker, "from": ONE_MONTH_AGO, "to": TODAY, "token": api_key},
            timeout=10,
        )
        articles = r.json()
        if not isinstance(articles, list):
            return ""

        lines = []
        for article in articles:
            ts = article.get("datetime", 0)
            try:
                date_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            except Exception:
                date_str = "unknown"
            summary = article.get("summary", "").strip()
            if summary:
                lines.append("[{}] {}".format(date_str, summary))

        return "\n".join(lines)
    except Exception as e:
        print("  [Finnhub error {}] {}".format(ticker, e))
        return ""


def main():
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        raise EnvironmentError("FINNHUB_API_KEY 未設定")

    os.makedirs("output", exist_ok=True)

    rs95_tickers = load_rs95_liquid(RS_CSV, STOCK_DATA_CSV)
    rs95_set = {t for t, _ in rs95_tickers}

    with open(INPUT_TXT, "r") as f:
        technical_tickers = [line.strip().upper() for line in f if line.strip()]

    extra_tickers = [t for t in technical_tickers if t not in rs95_set]
    all_tickers = [(t, "RS{}".format(rs)) for t, rs in rs95_tickers] + \
                  [(t, "technical") for t in extra_tickers]

    print("📋 RS95: {} 檔，technical: {} 檔，合計去重: {} 檔\n".format(
        len(rs95_tickers), len(technical_tickers), len(all_tickers)))

    news_cache = {}
    total = len(all_tickers)
    for i, (ticker, label) in enumerate(all_tickers, 1):
        print("  [{:3d}/{}] {} ({}) ...".format(i, total, ticker, label), end=" ", flush=True)
        text = fetch_finnhub_news(ticker, api_key)
        news_cache[ticker] = text
        count = text.count("\n") + 1 if text else 0
        print("✓ {} 篇".format(count))
        time.sleep(1)

    with open(OUTPUT_NEWS_CACHE, "w", encoding="utf-8") as f:
        json.dump(news_cache, f, ensure_ascii=False, indent=2)

    print("\n✅ news cache 完成，共 {} 檔 → {}".format(len(news_cache), OUTPUT_NEWS_CACHE))


if __name__ == "__main__":
    main()
