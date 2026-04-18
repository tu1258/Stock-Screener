import os
import csv
import json
import time
import datetime
import requests
import pandas as pd
from google import genai
from google.genai import types

# ── 設定 ──────────────────────────────────────────────────────────────────
INPUT_TXT        = "output/technical_watchlist.txt"
RS_CSV           = "stock_rs.csv"
OUTPUT_CSV       = "output/watchlist.csv"
OUTPUT_WATCHLIST = "output/watchlist.txt"
OUTPUT_THEMES    = "output/hot_themes.csv"
OUTPUT_HOT_WL    = "output/hot_theme_watchlist.txt"
RATING_THRESHOLD = 6

GEMINI_MODEL_PHASE1 = "gemini-3-flash-preview"
GEMINI_MODEL_SCORE  = "gemini-3.1-flash-lite-preview"

STOCK_DATA_CSV    = "stock_data.csv"
INDUSTRY_RS_CSV   = "industry_rs.csv"
TICKER_IND_CSV    = "ticker_industry.csv"
MIN_AVG_VALUE_10M = 100

NEWS_SLEEP    = 1
SCORING_SLEEP = 2

TODAY      = datetime.date.today().strftime("%Y-%m-%d")
ONE_MONTH_AGO = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

_news_cache: dict = {}  # ticker -> 格式化新聞字串


# ── Finnhub API 抓公司新聞 summary ────────────────────────────────────────
def fetch_finnhub_news(ticker: str) -> str:
    """
    用 Finnhub company-news endpoint 抓一個月內新聞，
    回傳格式：每篇一行「[YYYY-MM-DD] headline: summary」
    """
    if ticker in _news_cache:
        return _news_cache[ticker]

    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        print("      [Finnhub] 未設定 FINNHUB_API_KEY，跳過 {}".format(ticker))
        _news_cache[ticker] = ""
        return ""

    print("    [Finnhub] fetching {} ...".format(ticker), flush=True)

    try:
        r = requests.get(
            "https://finnhub.io/api/v1/company-news",
            params={
                "symbol": ticker,
                "from":   ONE_MONTH_AGO,
                "to":     TODAY,
                "token":  api_key,
            },
            timeout=15,
        )
        articles = r.json()

        if not isinstance(articles, list):
            print("      [Finnhub] {} 回傳非預期格式".format(ticker))
            _news_cache[ticker] = ""
            return ""

        lines = []
        for article in articles:
            # unix timestamp -> YYYY-MM-DD
            ts = article.get("datetime", 0)
            try:
                date_str = datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            except Exception:
                date_str = "unknown"

            headline = article.get("headline", "").strip()
            summary  = article.get("summary",  "").strip()

            if not headline:
                continue

            if summary and summary != headline:
                lines.append("[{}] {}: {}".format(date_str, headline, summary))
            else:
                lines.append("[{}] {}".format(date_str, headline))

        text = "\n".join(lines)
        print("      [Finnhub] {} 拿到 {} 篇".format(ticker, len(lines)))
        _news_cache[ticker] = text
        return text

    except Exception as e:
        print("      [Finnhub error {}] {}".format(ticker, e))
        _news_cache[ticker] = ""
        return ""


# ── Phase 1：Finnhub summary → Gemini 摘要 ───────────────────────────────
def fetch_ticker_summary(client: genai.Client, ticker: str) -> str:
    news_text = fetch_finnhub_news(ticker)
    if not news_text:
        return ""

    prompt = """Today is {today}. The following are recent news headlines and summaries (past 30 days) for stock {ticker}, sourced from Finnhub.
Based on this information, summarize from an investment theme and market catalyst perspective:
- What is the company's core business?
- What is the strongest current investment theme? Why is the market paying attention?
- What recent catalysts (earnings, products, partnerships, regulations, industry trends) are driving the stock?
- In which sector or theme does it have speculative potential or scarcity value?

Return a concise bullet-point summary focused on themes and catalysts only.

News (format: [YYYY-MM-DD] headline: summary):
{news}""".format(today=TODAY, ticker=ticker, news=news_text)

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_SCORE,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0,
                    max_output_tokens=1024,
                ),
            )
            result = response.text.strip()
            print("      [summary OK] {} chars".format(len(result)))
            return result
        except Exception as e:
            err = str(e)
            print("\n  [summary error {}] {}".format(ticker, err[:200]))
            if "429" in err or "quota" in err.lower():
                time.sleep(5)
            elif attempt < 2:
                time.sleep(3)

    return ""


# ── 計算 RS95+ 且成交值 >= 100M 的清單 ──────────────────────────────────
def load_rs95_liquid(rs_csv: str, stock_data_csv: str) -> list:
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

    liquid = avg_val[avg_val["avg_value_10"] >= MIN_AVG_VALUE_10M]["ticker"].tolist()
    liquid_set = set(liquid)

    result = [(t, rs) for t, rs in rs_map.items() if t in liquid_set]
    return sorted(result, key=lambda x: -x[1])


# ── 讀取 industry RS 排行 ────────────────────────────────────────────────
def load_industry_ranking(industry_rs_csv: str, ticker_ind_csv: str) -> tuple:
    ticker_to_industry = {}
    ticker_to_exchange = {}

    if os.path.exists(ticker_ind_csv):
        df_ind = pd.read_csv(ticker_ind_csv)
        for _, row in df_ind.iterrows():
            t    = str(row.get("ticker",   "")).upper()
            ind  = str(row.get("industry", ""))
            exch = str(row.get("exchange", ""))
            if t:
                if ind and ind != "nan":
                    ticker_to_industry[t] = ind
                if exch and exch != "nan":
                    ticker_to_exchange[t] = exch

    if not os.path.exists(industry_rs_csv):
        return "", ticker_to_industry, ticker_to_exchange

    df = pd.read_csv(industry_rs_csv)
    df = df.sort_values("avg_rs", ascending=False)

    lines = []
    for _, row in df.iterrows():
        industry = row.get("industry", "")
        sector   = row.get("sector", "")
        avg_rs   = row.get("avg_rs", 0)
        count    = int(row.get("ticker_count", 0))
        lines.append("  {:5.1f}  {}（{}，{} 檔）".format(avg_rs, industry, sector, count))

    return "\n".join(lines), ticker_to_industry, ticker_to_exchange


# ── Phase 2：建立今日熱門題材 ─────────────────────────────────────────────
def fetch_hot_themes(client: genai.Client, rs95_tickers: list,
                     industry_text: str, ticker_to_industry: dict) -> tuple:
    print("  Fetching news for {} tickers via Finnhub...".format(len(rs95_tickers)))

    ticker_summaries = {}
    for i, (ticker, rs) in enumerate(rs95_tickers, 1):
        print("    [{:3d}/{}] {} RS{}".format(i, len(rs95_tickers), ticker, rs), end=" ... ", flush=True)
        summary = fetch_ticker_summary(client, ticker)
        ticker_summaries[ticker] = summary
        print("✓" if summary else "（無）")
        time.sleep(NEWS_SLEEP)

    ticker_lines = "\n".join("  RS{:>2}  {}".format(rs, t) for t, rs in rs95_tickers)

    ticker_ind_lines = "\n".join(
        "  {}: {}".format(t, ticker_to_industry.get(t, "N/A"))
        for t, rs in rs95_tickers
    )

    summaries_text = "\n\n".join(
        "[{} RS{}]\n{}".format(t, rs, ticker_summaries.get(t, "(no data)"))
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
[Source D: Recent news summaries for RS95+ stocks (sourced from Finnhub, past 30 days, format: [YYYY-MM-DD] headline: summary)]
{summaries_text}

Instructions:
- RS is a relative strength score. RS99 = top 1% of all stocks; RS95 = top 5%.
- RS weighting is LINEAR. Use these exact weights per stock:
    RS99 = 5,  RS98 = 4,  RS97 = 3,  RS96 = 2,  RS95 = 1
  A theme's score = sum of weights of all its member stocks.
  Example: 3x RS99 in a theme = score 15, which outranks 10x RS95-only stocks (score 10).
- Therefore: RS99 stocks carry significantly more weight than RS95 stocks in theme ranking.
- One stock may belong to multiple themes.
- Use your own knowledge for sector classification; Source B is only a rough reference.
- Use Source D to assess recent catalysts and market attention.
- Industry average RS (Source C) is a secondary signal only — do not weight it too heavily.

[Output format: return ONLY the following JSON. No Markdown, no explanation.]
{{
  "hot_themes": [
    {{
      "name": "主題名稱（繁體中文）",
      "desc": "說明（含代表性個股RS分數，繁體中文）",
      "tickers": ["TICKER1", "TICKER2"]
    }},
    ...
  ],
  "ticker_themes": {{
    "TICKER": ["題材A（繁體中文）", "題材B（繁體中文）"],
    ...
  }}
}}

Requirements:
- hot_themes: top 10 themes sorted by weighted heat. Each entry includes 1-2 sentence description and all related tickers (uppercase). Theme names and descriptions must be in Traditional Chinese.
- ticker_themes: tag every stock in the list with all relevant themes (multiple allowed). Theme names must be in Traditional Chinese.
""".format(
        today=TODAY,
        total=len(rs95_tickers),
        ticker_lines=ticker_lines,
        ticker_ind_lines=ticker_ind_lines,
        industry_section=industry_section,
        summaries_text=summaries_text,
    )

    for attempt in range(5):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_PHASE1,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0,
                    max_output_tokens=8192,
                ),
            )
            raw = response.text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.lower().startswith("json"):
                    raw = raw[4:]
            data = json.loads(raw.strip())

            hot_themes_list = data.get("hot_themes", [])
            ticker_themes   = data.get("ticker_themes", {})

            rs_lookup = {t: rs for t, rs in rs95_tickers}

            theme_count = {}
            for ticker_upper, themes in ticker_themes.items():
                t = ticker_upper.upper()
                for theme in themes:
                    theme_count.setdefault(theme, []).append(
                        "{}(RS{})".format(t, rs_lookup.get(t, "?"))
                    )

            lines = ["## 題材統計（RS95+ 且成交值>=100M，共 {} 檔）\n".format(len(rs95_tickers))]
            for theme, members in sorted(theme_count.items(), key=lambda x: -len(x[1])):
                members_str = ", ".join(members[:20])
                suffix = "... 等共 {} 檔".format(len(members)) if len(members) > 20 else "共 {} 檔".format(len(members))
                lines.append("{}：{}　{}".format(theme, members_str, suffix))

            lines.append("\n## 今日熱門題材（Gemini 分析）\n")
            for item in hot_themes_list:
                name    = item.get("name", "")
                desc    = item.get("desc", "")
                tickers = item.get("tickers", [])
                lines.append("{} — {}（{}）".format(name, desc, ", ".join(tickers)))

            themes_text = "\n".join(lines)
            return themes_text, hot_themes_list, rs_lookup

        except (json.JSONDecodeError, KeyError):
            time.sleep(2)
        except Exception as e:
            err = str(e)
            if "429" in err or "quota" in err.lower():
                time.sleep(5)
            elif attempt < 4:
                time.sleep(5)
            else:
                raise

    return "", [], {}


# ── 產生 hot_theme_watchlist ──────────────────────────────────────────────
def build_hot_theme_watchlist(hot_themes_list: list, rs_lookup: dict, top_n: int = 10) -> list:
    result = []
    seen   = set()
    for item in hot_themes_list[:top_n]:
        tickers_in_group = sorted(
            [t.strip().upper() for t in item.get("tickers", []) if t.strip()],
            key=lambda t: -rs_lookup.get(t, 0),
        )
        for t in tickers_in_group:
            if t not in seen:
                seen.add(t)
                result.append(t)
    return result


# ── Phase 3：評分 ─────────────────────────────────────────────────────────
def score_ticker(client: genai.Client, ticker: str, hot_themes: str) -> dict:
    # 優先用快取，沒有就重新抓
    news_text = fetch_finnhub_news(ticker)
    news_section = "\n[Recent Finnhub news for {} (past 30 days)]\n{}".format(ticker, news_text) if news_text else ""

    prompt = """You are a senior US equity analyst. Today is {today}.

Below is today's hot theme list, derived from RS scores and capital flow across the entire market:

{hot_themes}
{news_section}

Based on the above information, evaluate how well {ticker} aligns with the hot themes listed.
Score it from 1 to 10. Base your score strictly on {ticker}'s actual relevance to the themes above,
not on your own independent judgment of whether the stock is hot.

Scoring criteria:
- 10: Core holding of the current strongest theme; explosive momentum, highly concentrated capital
- 9: Leader of a strong theme; clear catalyst, extremely high market attention
- 8: Key member of a strong sector; clear theme with sustained capital inflow
- 7: Member of a hot sector but not the core, or theme still building
- 6: Theme has support but many competitors, or attention not yet focused
- 5: Flat theme, unclear sector rotation position
- 4: Theme fading, capital attention declining
- 3: Theme cooling off, capital outflow, limited fundamental support
- 2: Theme nearly gone, market has moved on
- 1: Completely abandoned by market, disconnected from current trends

Output: Return a single JSON object only. All Chinese text fields must be in Traditional Chinese.
Fields:
- "ticker": symbol (uppercase)
- "rating": integer 1-10
- "theme": matching theme name(s) from the hot theme list above (comma-separated, Traditional Chinese)
- "feature": relevance to hot themes or competitive edge (max 20 Chinese characters, Traditional Chinese)
- "reason": scoring rationale (max 20 Chinese characters, Traditional Chinese)

No Markdown, no extra explanation.""".format(
        today=TODAY,
        hot_themes=hot_themes,
        news_section=news_section,
        ticker=ticker,
    )

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_SCORE,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0,
                    max_output_tokens=512,
                ),
            )
            raw = response.text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.lower().startswith("json"):
                    raw = raw[4:]
            result = json.loads(raw.strip())
            result["ticker"] = ticker.upper()
            return result
        except json.JSONDecodeError:
            time.sleep(2)
        except Exception as e:
            err = str(e)
            if "429" in err or "quota" in err.lower():
                time.sleep(5)
            elif attempt < 2:
                time.sleep(5)

    return {"ticker": ticker.upper(), "rating": 0, "theme": "", "feature": "", "reason": ""}


# ── 主流程 ────────────────────────────────────────────────────────────────
def main():
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        raise EnvironmentError("GEMINI_API_KEY 未設定")

    client = genai.Client(api_key=gemini_key)

    with open(INPUT_TXT, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    rs_map = {}
    if os.path.exists(RS_CSV):
        with open(RS_CSV, "r", newline="") as f:
            for row in csv.DictReader(f):
                t = row["ticker"].upper()
                rs_map[t] = row.get("RS", 0)

    rs95_tickers = load_rs95_liquid(RS_CSV, STOCK_DATA_CSV)
    industry_text, ticker_to_industry, _ = load_industry_ranking(
        INDUSTRY_RS_CSV, TICKER_IND_CSV
    )

    print("📋 待分析：{} 檔 ／ RS>=95 參考股：{} 檔\n".format(len(tickers), len(rs95_tickers)))

    print("📡 Phase 1+2：抓 Finnhub 新聞 → Gemini 摘要 → 建立今日熱門題材...")
    hot_themes, hot_themes_list, rs_lookup = fetch_hot_themes(
        client, rs95_tickers, industry_text, ticker_to_industry
    )
    if not hot_themes:
        raise RuntimeError("Phase 2 失敗")

    os.makedirs("output", exist_ok=True)
    theme_rows = []
    for rank, item in enumerate(hot_themes_list, 1):
        tickers_in_theme = [t.upper() for t in item.get("tickers", [])]
        theme_rows.append({
            "rank"        : rank,
            "theme"       : item.get("name", ""),
            "desc"        : item.get("desc", ""),
            "tickers"     : ", ".join(tickers_in_theme),
            "ticker_count": len(tickers_in_theme),
        })
    pd.DataFrame(theme_rows).to_csv(OUTPUT_THEMES, index=False, encoding="utf-8-sig")
    print("✅ 題材清單已存至 {}\n".format(OUTPUT_THEMES))

    hot_wl_tickers = build_hot_theme_watchlist(hot_themes_list, rs_lookup)
    with open(OUTPUT_HOT_WL, "w", encoding="utf-8") as f:
        f.write("\n".join(hot_wl_tickers) + "\n")
    print("✅ 熱門題材 watchlist 已存至 {}（{} 檔）\n".format(OUTPUT_HOT_WL, len(hot_wl_tickers)))

    print("🔍 Phase 3：逐一評分（共 {} 檔）...\n".format(len(tickers)))
    final_rows = []

    for i, ticker in enumerate(tickers, 1):
        cached = ticker in _news_cache
        print("  [{:3d}/{}] {}{}".format(i, len(tickers), ticker, "（快取）" if cached else ""), end=" ... ", flush=True)
        result = score_ticker(client, ticker, hot_themes)
        final_rows.append({
            "ticker":  result.get("ticker", ticker),
            "RS":      rs_map.get(ticker, 0),
            "rating":  result.get("rating", 0),
            "theme":   result.get("theme", ""),
            "feature": result.get("feature", ""),
            "reason":  result.get("reason", ""),
        })
        print("rating={}".format(result.get("rating", "?")))
        time.sleep(SCORING_SLEEP)

    final_rows.sort(
        key=lambda x: (-int(x["rating"]), -float(str(x["RS"]).replace(",", "") or 0))
    )

    os.makedirs("output", exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "RS", "rating", "theme", "feature", "reason"])
        writer.writeheader()
        writer.writerows(final_rows)

    watchlist = [row["ticker"] for row in final_rows if int(row["rating"]) >= RATING_THRESHOLD]
    with open(OUTPUT_WATCHLIST, "w", encoding="utf-8") as f:
        f.write("\n".join(watchlist) + "\n")

    print("\n" + "=" * 50)
    print("✅ 完成！分析 {} 檔，watchlist {} 檔".format(len(final_rows), len(watchlist)))
    print("   {}".format(OUTPUT_CSV))
    print("   {}".format(OUTPUT_WATCHLIST))
    print("   {}".format(OUTPUT_THEMES))
    print("   {}".format(OUTPUT_HOT_WL))


if __name__ == "__main__":
    main()
