import os
import csv
import json
import time
import datetime
import pandas as pd
from google import genai
from google.genai import types
import sys

RS_CSV            = "stock_rs.csv"
STOCK_DATA_CSV    = "stock_data.csv"
INDUSTRY_RS_CSV   = "industry_rs.csv"
TICKER_IND_CSV    = "ticker_industry.csv"
INPUT_NEWS_CACHE  = "output/news_cache.json"
OUTPUT_SUMMARIES  = "output/rs95_summaries.json"
OUTPUT_THEMES_CSV = "output/hot_theme.csv"
OUTPUT_THEMES_JSON= "output/hot_theme.json"
OUTPUT_HOT_WL     = "output/hot_theme_watchlist.txt"
MIN_AVG_VALUE_10D = 100

GEMINI_MODEL_SUMMARY = "gemma-4-31b-it"       # Phase 1：摘要，高 RPD，不需要 thinking
GEMINI_MODEL_THEMES  = "gemini-3-flash-preview" # Phase 2：主題歸納，需要市場知識

TODAY = datetime.date.today().strftime("%Y-%m-%d")


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
                           usecols=["ticker", "date", "close", "volume"])
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
    return sorted([(t, rs) for t, rs in rs_map.items() if t in liquid_set], key=lambda x: -x[1])


def load_industry_ranking(industry_rs_csv, ticker_ind_csv):
    ticker_to_industry = {}
    if os.path.exists(ticker_ind_csv):
        df_ind = pd.read_csv(ticker_ind_csv)
        for _, row in df_ind.iterrows():
            t   = str(row.get("ticker",   "")).upper()
            ind = str(row.get("industry", ""))
            if t and ind and ind != "nan":
                ticker_to_industry[t] = ind

    if not os.path.exists(industry_rs_csv):
        return "", ticker_to_industry

    df = pd.read_csv(industry_rs_csv).sort_values("avg_rs", ascending=False)
    lines = []
    for _, row in df.iterrows():
        lines.append("  {:5.1f}  {}（{}，{} 檔）".format(
            row.get("avg_rs", 0),
            row.get("industry", ""),
            row.get("sector", ""),
            int(row.get("ticker_count", 0)),
        ))
    return "\n".join(lines), ticker_to_industry


def build_summaries(client, rs95_tickers, news_cache):
    summaries = {}
    if os.path.exists(OUTPUT_SUMMARIES):
        with open(OUTPUT_SUMMARIES, "r", encoding="utf-8") as f:
            summaries = json.load(f)
        print("📂 載入既有摘要：{} 檔".format(len(summaries)))

    total = len(rs95_tickers)
    for i, (ticker, rs) in enumerate(rs95_tickers, 1):
        if ticker in summaries:
            print("  [{:3d}/{}] {} RS{} （快取）".format(i, total, ticker, rs))
            continue

        news_text = news_cache.get(ticker, "")
        if not news_text:
            print("  [{:3d}/{}] {} RS{} （無新聞）".format(i, total, ticker, rs))
            summaries[ticker] = ""
            continue

        print("  [{:3d}/{}] {} RS{} ...".format(i, total, ticker, rs), end=" ", flush=True)

        prompt = """Today is {today}. The following are recent news headlines (past 30 days) for stock {ticker}, sourced from Finnhub.
Based on this information, summarize from an investment theme and market catalyst perspective:
- What is the company's core business?
- What is the strongest current investment theme? Why is the market paying attention?
- What recent catalysts (earnings, products, partnerships, regulations, industry trends) are driving the stock?
- In which sector or theme does it have speculative potential or scarcity value?

Return a concise bullet-point summary focused on themes and catalysts only.

News (format: [YYYY-MM-DD] summary):
{news}""".format(today=TODAY, ticker=ticker, news=news_text)

        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_SUMMARY,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=1,
                    top_p=0.95,
                    top_k=64,
                    max_output_tokens=1024,
                ),
            )
            result = response.text.strip()
            summaries[ticker] = result
            print("✓ {} chars".format(len(result)))
        except Exception as e:
            print("❌ {}".format(str(e)[:100]))
        time.sleep(1)

    with open(OUTPUT_SUMMARIES, "w", encoding="utf-8") as f:
        json.dump(summaries, f, ensure_ascii=False, indent=2)

    failed = [t for t, rs in rs95_tickers if news_cache.get(t) and t not in summaries]
    if failed:
        print("⚠️ 摘要未完成 {} 檔：{}".format(len(failed), ", ".join(failed)))
        sys.exit(1)

    return summaries


def fetch_hot_themes(client, rs95_tickers, summaries, industry_text, ticker_to_industry):
    ticker_lines     = "\n".join("  RS{:>2}  {}".format(rs, t) for t, rs in rs95_tickers)
    ticker_ind_lines = "\n".join("  {}: {}".format(t, ticker_to_industry.get(t, "N/A")) for t, rs in rs95_tickers)
    summaries_text   = "\n\n".join(
        "[{} RS{}]\n{}".format(t, rs, summaries.get(t, "(no data)"))
        for t, rs in rs95_tickers
    )

    industry_section = ""
    if industry_text:
        industry_section = """
[Source C: Industry Average RS Ranking (sorted descending by avg RS across all stocks)]
{industry}

Notes:
- This ranking reflects the overall strength of each industry, not individual stocks.
- A high industry average RS means strong sector-wide capital momentum, not just a few outliers.
- Cross-reference with Source A to avoid single-stock distortion of theme assessment.
""".format(industry=industry_text)

    prompt = """Today is {today}. You are a senior US equity analyst. Your task is to identify today's hot investment themes.

[Source A: Stocks with RS>=95 and avg daily value>=100M USD (total {total} stocks, sorted by RS descending)]
{ticker_lines}

[Source B: Sector classification of RS95+ stocks (use your own knowledge as primary reference; this data is coarse)]
{ticker_ind_lines}
{industry_section}
[Source D: Recent news summaries for RS95+ stocks (sourced from Finnhub, past 30 days)]
{summaries_text}

Instructions:
- RS is a relative strength score. RS99 = top 1% of all stocks; RS95 = top 5%.
- RS weighting is LINEAR. Use these exact weights per stock:
    RS99 = 5,  RS98 = 4,  RS97 = 3,  RS96 = 2,  RS95 = 1
  A theme's score = sum of weights of all its member stocks.
- One stock may belong to multiple themes.
- Use your own knowledge for sector classification; Source B is only a rough reference.
- Use Source D to assess recent catalysts and market attention.
- Industry average RS (Source C) is a secondary signal only.

- You MUST output a JSON array of exactly 10 objects, sorted by weighted heat.
- Each object has these fields:
    "name": theme name in Traditional Chinese (string)
    "desc": description in Traditional Chinese, 20 characters or less (string)
    "tickers": comma-separated ticker symbols, uppercase, no spaces (string)
- Output raw JSON only. No markdown, no extra text.

""".format(
        today=TODAY,
        total=len(rs95_tickers),
        ticker_lines=ticker_lines,
        ticker_ind_lines=ticker_ind_lines,
        industry_section=industry_section,
        summaries_text=summaries_text,
    )

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL_THEMES,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                max_output_tokens=16384,
                response_mime_type="application/json",
            ),
        )
        raw = response.text.strip()
        print("  [themes raw]\n{}\n---".format(raw))
        hot_themes_list = json.loads(raw)

        for item in hot_themes_list:
            item["tickers"] = item.get("tickers", "").upper().replace(" ", "").rstrip(",")

        rs_lookup = {t: rs for t, rs in rs95_tickers}

        theme_count = {}
        for item in hot_themes_list:
            theme = item.get("name", "")
            for ticker in item.get("tickers", "").split(","):
                t = ticker.strip().upper()
                if t:
                    theme_count.setdefault(theme, []).append("{}(RS{})".format(t, rs_lookup.get(t, "?")))

        lines = ["## 題材統計（RS95+ 且成交值>=100M，共 {} 檔）\n".format(len(rs95_tickers))]
        for theme, members in sorted(theme_count.items(), key=lambda x: -len(x[1])):
            members_str = ", ".join(members[:20])
            suffix = "... 等共 {} 檔".format(len(members)) if len(members) > 20 else "共 {} 檔".format(len(members))
            lines.append("{}：{}　{}".format(theme, members_str, suffix))

        lines.append("\n## 今日熱門題材（Gemini 分析）\n")
        for item in hot_themes_list:
            lines.append("{} — {}（{}）".format(
                item.get("name", ""),
                item.get("desc", ""),
                item.get("tickers", ""),
            ))

        hot_themes_text = "\n".join(lines)
        return hot_themes_text, hot_themes_list, rs_lookup

    except Exception as e:
        print("  [themes error] {}".format(str(e)[:200]))

    return "", [], {}


def main():
    gemini_key = os.environ.get("GEMINI_API_KEY_HOT_THEME")
    if not gemini_key:
        print("GEMINI_API_KEY 未設定")
        sys.exit(1)

    client = genai.Client(api_key=gemini_key)
    os.makedirs("output", exist_ok=True)

    if not os.path.exists(INPUT_NEWS_CACHE):
        print("找不到 {}，請先執行 fetch_news.py".format(INPUT_NEWS_CACHE))
        sys.exit(1)

    with open(INPUT_NEWS_CACHE, "r", encoding="utf-8") as f:
        news_cache = json.load(f)
    print("📂 news cache 載入：{} 檔".format(len(news_cache)))

    rs95_tickers = load_rs95_liquid(RS_CSV, STOCK_DATA_CSV)
    industry_text, ticker_to_industry = load_industry_ranking(INDUSTRY_RS_CSV, TICKER_IND_CSV)
    print("📋 RS95 流動性篩選後：{} 檔\n".format(len(rs95_tickers)))

    print("📝 Phase 1：生成 RS95 摘要...")
    summaries = build_summaries(client, rs95_tickers, news_cache)
    print("✅ 摘要完成\n")

    print("🧠 Phase 2：歸納今日熱門題材...")
    hot_themes_text, hot_themes_list, rs_lookup = fetch_hot_themes(
        client, rs95_tickers, summaries, industry_text, ticker_to_industry
    )
    if not hot_themes_list:
        print("⚠️ Phase 2 失敗")
        sys.exit(1)

    with open(OUTPUT_THEMES_JSON, "w", encoding="utf-8") as f:
        json.dump({
            "hot_themes_text": hot_themes_text,
            "hot_themes_list": hot_themes_list,
            "rs_lookup":       rs_lookup,
        }, f, ensure_ascii=False, indent=2)

    theme_rows = []
    for rank, item in enumerate(hot_themes_list, 1):
        tickers_in_theme = [t.strip().upper() for t in item.get("tickers", "").split(",") if t.strip()]
        theme_rows.append({
            "rank":         rank,
            "theme":        item.get("name", ""),
            "desc":         item.get("desc", ""),
            "tickers":      ",".join(tickers_in_theme),
            "ticker_count": len(tickers_in_theme),
        })
    pd.DataFrame(theme_rows).to_csv(OUTPUT_THEMES_CSV, index=False, encoding="utf-8-sig")

    result, seen = [], set()
    for item in hot_themes_list[:10]:
        for t in sorted(
            [t.strip().upper() for t in item.get("tickers", "").split(",") if t.strip()],
            key=lambda t: -rs_lookup.get(t, 0),
        ):
            if t not in seen:
                seen.add(t)
                result.append(t)
    with open(OUTPUT_HOT_WL, "w", encoding="utf-8") as f:
        f.write("\n".join(result) + "\n")

    print("✅ 完成")
    print("   {}".format(OUTPUT_THEMES_JSON))
    print("   {}".format(OUTPUT_THEMES_CSV))
    print("   {}".format(OUTPUT_HOT_WL))


if __name__ == "__main__":
    main()
