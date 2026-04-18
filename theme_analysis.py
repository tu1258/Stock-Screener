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

TODAY = datetime.date.today().strftime("%Y-%m-%d")

_news_cache: dict[str, str] = {}  # ticker -> 格式化新聞字串


# ── Serper API 抓 Google 搜尋結果（含 AI Overview）───────────────────────
def fetch_serper_news(ticker: str) -> str:
    """
    用 Serper API 搜尋 ticker，回傳：
    - answerBox / AI Overview 摘要
    - 前 10 筆搜尋結果標題 + snippet（當新聞用）
    """
    if ticker in _news_cache:
        return _news_cache[ticker]

    api_key = os.environ.get("SERPER_API_KEY", "")
    if not api_key:
        print(f"      [Serper] 未設定 SERPER_API_KEY，跳過 {ticker}")
        _news_cache[ticker] = ""
        return ""

    print(f"    [Serper] fetching {ticker} ...", flush=True)

    try:
        r = requests.post(
            "https://google.serper.dev/search",
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json",
            },
            json={
                "q": f"{ticker} stock news analysis",
                "gl": "us",
                "hl": "en",
                "num": 10,
            },
            timeout=15,
        )
        data = r.json()

        parts = []

        # AI Overview / Answer Box
        answer = data.get("answerBox", {})
        if answer.get("answer"):
            parts.append(f"[AI Overview] {answer['answer']}")
        elif answer.get("snippet"):
            parts.append(f"[AI Overview] {answer['snippet']}")
        elif answer.get("snippetHighlighted"):
            parts.append(f"[AI Overview] {' '.join(answer['snippetHighlighted'])}")

        # knowledgeGraph
        kg = data.get("knowledgeGraph", {})
        if kg.get("description"):
            parts.append(f"[Company] {kg['description']}")

        # organic 搜尋結果（標題 + snippet）
        for item in data.get("organic", [])[:10]:
            title   = item.get("title", "").strip()
            snippet = item.get("snippet", "").strip()
            if title:
                line = f"- {title}"
                if snippet:
                    line += f": {snippet}"
                parts.append(line)

        text = "\n".join(parts)
        print(f"      [Serper] {ticker} 拿到 {len(parts)} 筆")
        print(f"      [Serper] 內容預覽:\n{text[:1000]}\n") 
        _news_cache[ticker] = text
        return text

    except Exception as e:
        print(f"      [Serper error {ticker}] {e}")
        _news_cache[ticker] = ""
        return ""


# ── Phase 1：Serper 結果 → Gemini 摘要────────────────────────────────────
def fetch_ticker_summary(client: genai.Client, ticker: str) -> str:
    news_text = fetch_serper_news(ticker)
    if not news_text:
        return ""

    prompt = f"""Today is {TODAY}. The following are recent Google search results and news for stock {ticker}.
Based on this information, summarize from an investment theme and market catalyst perspective:
- What is the company's core business?
- What is the strongest current investment theme? Why is the market paying attention?
- What recent catalysts (earnings, products, partnerships, regulations, industry trends) are driving the stock?
- In which sector or theme does it have speculative potential or scarcity value?

Return a concise bullet-point summary focused on themes and catalysts only.

Search results:
{news_text}"""

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
            print(f"      [summary OK] {len(result)} chars")
            return result
        except Exception as e:
            err = str(e)
            print(f"\n  [summary error {ticker}] {err[:200]}")
            if "429" in err or "quota" in err.lower():
                time.sleep(5)
            elif attempt < 2:
                time.sleep(3)

    return ""


# ── 計算 RS95+ 且成交值 >= 100M 的清單 ──────────────────────────────────
def load_rs95_liquid(rs_csv: str, stock_data_csv: str) -> list[tuple]:
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
def load_industry_ranking(industry_rs_csv: str, ticker_ind_csv: str) -> tuple[str, dict, dict]:
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
        lines.append(f"  {avg_rs:5.1f}  {industry}（{sector}，{count} 檔）")

    return "\n".join(lines), ticker_to_industry, ticker_to_exchange


# ── Phase 2：建立今日熱門題材 ─────────────────────────────────────────────
def fetch_hot_themes(client: genai.Client, rs95_tickers: list[tuple],
                     industry_text: str, ticker_to_industry: dict) -> tuple:
    print(f"  Fetching news for {len(rs95_tickers)} tickers via Serper...")

    ticker_summaries = {}
    for i, (ticker, rs) in enumerate(rs95_tickers, 1):
        print(f"    [{i:3d}/{len(rs95_tickers)}] {ticker} RS{rs}", end=" ... ", flush=True)
        summary = fetch_ticker_summary(client, ticker)
        ticker_summaries[ticker] = summary
        print("✓" if summary else "（無）")
        time.sleep(NEWS_SLEEP)

    ticker_lines = "\n".join(f"  RS{rs:>2}  {t}" for t, rs in rs95_tickers)

    ticker_ind_lines = "\n".join(
        f"  {t}: {ticker_to_industry.get(t, 'N/A')}"
        for t, rs in rs95_tickers
    )

    summaries_text = "\n\n".join(
        f"[{t} RS{rs}]\n{ticker_summaries.get(t, '(no data)')}"
        for t, rs in rs95_tickers
    )

    industry_section = ""
    if industry_text:
        industry_section = f"""
[Source C: Industry Average RS Ranking (sorted descending by avg RS across all stocks)]
{industry_text}

Notes:
- This ranking reflects the overall strength of each industry, not individual stocks.
- A high industry average RS means strong sector-wide capital momentum, not just a few outliers.
- Cross-reference with Source A to avoid single-stock distortion of theme assessment.
"""

    prompt = f"""Today is {TODAY}. You are a senior US equity analyst. Your task is to identify today's hot investment themes.

[Source A: Stocks with RS>=95 and avg daily value>=100M USD (total {len(rs95_tickers)} stocks, sorted by RS descending)]
{ticker_lines}

[Source B: Sector classification of RS95+ stocks (use your own knowledge as primary reference; this data is coarse)]
{ticker_ind_lines}
{industry_section}
[Source D: Recent news summaries for RS95+ stocks (sourced from Google Search via Serper)]
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
"""

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

            hot_themes_list: list = data.get("hot_themes", [])
            ticker_themes: dict   = data.get("ticker_themes", {})

            rs_lookup = {t: rs for t, rs in rs95_tickers}

            theme_count: dict[str, list[str]] = {}
            for ticker_upper, themes in ticker_themes.items():
                t = ticker_upper.upper()
                for theme in themes:
                    theme_count.setdefault(theme, []).append(
                        f"{t}(RS{rs_lookup.get(t, '?')})"
                    )

            lines = [f"## 題材統計（RS95+ 且成交值>=100M，共 {len(rs95_tickers)} 檔）\n"]
            for theme, members in sorted(theme_count.items(), key=lambda x: -len(x[1])):
                members_str = ", ".join(members[:20])
                suffix = f"... 等共 {len(members)} 檔" if len(members) > 20 else f"共 {len(members)} 檔"
                lines.append(f"{theme}：{members_str}　{suffix}")

            lines.append("\n## 今日熱門題材（Gemini 分析）\n")
            for item in hot_themes_list:
                name    = item.get("name", "")
                desc    = item.get("desc", "")
                tickers = item.get("tickers", [])
                lines.append(f"{name} — {desc}（{', '.join(tickers)}）")

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
def build_hot_theme_watchlist(
    hot_themes_list: list[dict],
    rs_lookup: dict[str, int],
    top_n: int = 10,
) -> list[str]:
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
    news_text = fetch_serper_news(ticker)
    news_section = f"\n[Recent news & analysis for {ticker}]\n{news_text}" if news_text else ""

    prompt = f"""You are a senior US equity analyst. Today is {TODAY}.

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

No Markdown, no extra explanation."""

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

    print(f"📋 待分析：{len(tickers)} 檔 ／ RS>=95 參考股：{len(rs95_tickers)} 檔\n")

    print("📡 Phase 1+2：抓 Google 新聞（Serper）→ Gemini 摘要 → 建立今日熱門題材...")
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
    print(f"✅ 題材清單已存至 {OUTPUT_THEMES}\n")

    hot_wl_tickers = build_hot_theme_watchlist(hot_themes_list, rs_lookup)
    with open(OUTPUT_HOT_WL, "w", encoding="utf-8") as f:
        f.write("\n".join(hot_wl_tickers) + "\n")
    print(f"✅ 熱門題材 watchlist 已存至 {OUTPUT_HOT_WL}（{len(hot_wl_tickers)} 檔）\n")

    print(f"🔍 Phase 3：逐一評分（共 {len(tickers)} 檔）...\n")
    final_rows = []

    for i, ticker in enumerate(tickers, 1):
        cached = ticker in _news_cache
        print(f"  [{i:3d}/{len(tickers)}] {ticker}{'（快取）' if cached else ''}", end=" ... ", flush=True)
        result = score_ticker(client, ticker, hot_themes)
        final_rows.append({
            "ticker":  result.get("ticker", ticker),
            "RS":      rs_map.get(ticker, 0),
            "rating":  result.get("rating", 0),
            "theme":   result.get("theme", ""),
            "feature": result.get("feature", ""),
            "reason":  result.get("reason", ""),
        })
        print(f"rating={result.get('rating', '?')}")
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

    print(f"\n{'='*50}")
    print(f"✅ 完成！分析 {len(final_rows)} 檔，watchlist {len(watchlist)} 檔")
    print(f"   {OUTPUT_CSV}")
    print(f"   {OUTPUT_WATCHLIST}")
    print(f"   {OUTPUT_THEMES}")
    print(f"   {OUTPUT_HOT_WL}")


if __name__ == "__main__":
    main()
