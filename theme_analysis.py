import os
import csv
import json
import time
import datetime
import requests
import xml.etree.ElementTree as ET
import pandas as pd
from google import genai
from google.genai import types
from email.utils import parsedate_to_datetime
from playwright.sync_api import sync_playwright

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

NEWS_SLEEP        = 1
SCORING_SLEEP     = 2
NEWS_MAX_AGE_DAYS = 30
NEWS_MAX_ITEMS    = 20

TODAY = datetime.date.today().strftime("%Y-%m-%d")

_summary_cache: dict[str, str] = {}


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
def load_industry_ranking(industry_rs_csv: str, ticker_ind_csv: str) -> tuple[str, dict]:
    ticker_to_industry = {}
    if os.path.exists(ticker_ind_csv):
        df_ind = pd.read_csv(ticker_ind_csv)
        for _, row in df_ind.iterrows():
            t = str(row.get("ticker", "")).upper()
            ind = str(row.get("industry", ""))
            if t and ind:
                ticker_to_industry[t] = ind

    if not os.path.exists(industry_rs_csv):
        return "", ticker_to_industry

    df = pd.read_csv(industry_rs_csv)
    df = df.sort_values("avg_rs", ascending=False)

    lines = []
    for _, row in df.iterrows():
        industry = row.get("industry", "")
        sector   = row.get("sector", "")
        avg_rs   = row.get("avg_rs", 0)
        count    = int(row.get("ticker_count", 0))
        lines.append(f"  {avg_rs:5.1f}  {industry}（{sector}，{count} 檔）")

    return "\n".join(lines), ticker_to_industry


# ── Perplexity Finance scrape ────────────────────────────────────────────
def scrape_perplexity_finance(ticker: str) -> str:
    url = f"https://www.perplexity.ai/finance/{ticker}"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="networkidle", timeout=15000)
            page.wait_for_timeout(3000)
            text = page.inner_text("body")
            browser.close()
        return text.strip()
    except Exception as e:
        print(f"\n  Perplexity scrape error ({ticker}): {e}")
        return ""


# ── # RSS 相關 code（暫時停用）─────────────────────────────────────────────
# def resolve_url(google_url: str, max_retries: int = 5) -> str:
#     headers = {
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
#         "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#         "Accept-Language": "en-US,en;q=0.5",
#         "Accept-Encoding": "gzip, deflate, br",
#         "Connection": "keep-alive",
#     }
#     for attempt in range(max_retries):
#         try:
#             resp = requests.get(
#                 google_url,
#                 headers=headers,
#                 allow_redirects=True,
#                 timeout=8,
#             )
#             if resp.url != google_url and "google.com" not in resp.url:
#                 return resp.url
#         except Exception:
#             pass
#         time.sleep(1)
#     return ""
#
#
# def fetch_rss_urls(ticker: str) -> list[str]:
#     rss_url = (
#         f"https://news.google.com/rss/search"
#         f"?q={ticker}+stock&hl=en-US&gl=US&ceid=US:en"
#     )
#     cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=NEWS_MAX_AGE_DAYS)
#     items_with_date = []
#
#     try:
#         resp = requests.get(rss_url, timeout=10,
#                             headers={"User-Agent": "Mozilla/5.0"})
#         resp.raise_for_status()
#
#         if not resp.content:
#             return []
#
#         root = ET.fromstring(resp.content)
#
#         for item in root.findall(".//item"):
#             raw_link = item.findtext("link")
#             if raw_link is None:
#                 continue
#
#             link = raw_link.strip()
#             if not link:
#                 continue
#
#             pub_date_str = item.findtext("pubDate", "")
#             try:
#                 pub_dt = parsedate_to_datetime(pub_date_str)
#             except Exception:
#                 pub_dt = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
#
#             if pub_dt < cutoff:
#                 continue
#
#             items_with_date.append((pub_dt, link))
#
#     except Exception as e:
#         print(f"\n  RSS error ({ticker}): {e}")
#         return []
#
#     items_with_date.sort(key=lambda x: x[0], reverse=True)
#     target_items = items_with_date[:NEWS_MAX_ITEMS]
#
#     resolved_urls = []
#
#     if target_items:
#         print(f"\n    [ {ticker} 解析到的新聞來源 ]")
#         for _, link in target_items:
#             real_url = resolve_url(link)
#             if real_url:
#                 resolved_urls.append(real_url)
#                 print(f"{link}")
#                 print(f"{real_url}")
#                 print()
#     else:
#         print(" (無符合條件的新聞)")
#
#     return resolved_urls


# ── Perplexity 摘要（含快取） ─────────────────────────────────────────────
def fetch_ticker_summary(client: genai.Client, ticker: str) -> str:
    if ticker in _summary_cache:
        return _summary_cache[ticker]

    print(f"\n    scraping Perplexity Finance for {ticker}...", end=" ", flush=True)
    raw_text = scrape_perplexity_finance(ticker)

    if not raw_text:
        _summary_cache[ticker] = ""
        return ""

    prompt = f"""Today is {TODAY}. The following is raw text scraped from Perplexity Finance for the stock {ticker}.
Please extract and summarize from an investment theme and market catalyst perspective:
- What is the company's core business?
- What is the strongest current investment theme? Why is the market paying attention?
- What recent catalysts (earnings, products, partnerships, regulations, industry trends) are driving the stock?
- In which sector or theme does it have speculative potential or scarcity value?

Focus only on themes and catalysts. Return a concise bullet-point summary.

Raw text:
{raw_text[:8000]}"""

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
            _summary_cache[ticker] = result
            return result
        except Exception as e:
            err = str(e)
            print(f"\n  ERROR {ticker}: {err[:200]}")
            if "429" in err or "quota" in err.lower():
                time.sleep(5)
            elif attempt < 2:
                time.sleep(5)

    _summary_cache[ticker] = ""
    return ""


# ── Phase 1：建立今日熱門題材 ─────────────────────────────────────────────
def fetch_hot_themes(client: genai.Client, rs95_tickers: list[tuple],
                     industry_text: str, ticker_to_industry: dict) -> tuple:
    print(f"  Fetching news summaries for {len(rs95_tickers)} tickers via Perplexity Finance...")

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
[Source D: Recent news summaries for RS95+ stocks (sourced from Perplexity Finance)]
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
    news_summary = fetch_ticker_summary(client, ticker)
    news_section = (
        f"\n[Recent news summary for {ticker}]\n{news_summary}"
        if news_summary else ""
    )

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
    industry_text, ticker_to_industry = load_industry_ranking(INDUSTRY_RS_CSV, TICKER_IND_CSV)

    print(f"📋 待分析：{len(tickers)} 檔 ／ RS>=95 參考股：{len(rs95_tickers)} 檔\n")

    print("📡 Phase 1：建立今日熱門題材...")
    hot_themes, hot_themes_list, rs_lookup = fetch_hot_themes(
        client, rs95_tickers, industry_text, ticker_to_industry
    )
    if not hot_themes:
        raise RuntimeError("Phase 1 失敗")

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
        cached = ticker in _summary_cache
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

    os.makedirs("csv", exist_ok=True)
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
