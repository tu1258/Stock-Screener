import os
import csv
import json
import time
import datetime
from google import genai
from google.genai import types
from pydantic import BaseModel

INPUT_TXT         = "output/technical_watchlist.txt"
RS_CSV            = "stock_rs.csv"
INPUT_NEWS_CACHE  = "output/news_cache.json"
INPUT_THEMES_JSON = "output/hot_theme.json"
OUTPUT_PROGRESS   = "output/theme_progress.json"
OUTPUT_CSV        = "output/watchlist.csv"
OUTPUT_WATCHLIST  = "output/watchlist.txt"
RATING_THRESHOLD  = 4

#GEMINI_MODEL_SCORE = "gemini-3.1-flash-lite-preview"
GEMINI_MODEL_SCORE = "gemma-4-31b-it"

TODAY = datetime.date.today().strftime("%Y-%m-%d")


class ScoreResult(BaseModel):
    ticker: str
    rating: int
    theme: str
    feature: str
    reason: str


def score_ticker(client, ticker, market_summary, hot_themes_json, news_text):
    news_section = "[Recent news for {} (past 30 days)]\n{}".format(ticker, news_text) if news_text else "(no recent news)"

    prompt = """You are a senior US equity analyst focused on momentum and thematic investing. Today is {today}.

Your task: evaluate how well {ticker} fits today's hot investment themes. Consider the following:
- What is the company's core business and its role within any relevant theme?
- Which of today's hot themes does this stock belong to, and what is its role within the theme?
- What recent catalysts are driving the stock?
- What is the forward narrative and growth potential?

[Today's market overview]
{market_summary}

[Today's hot themes]
{hot_themes}

[Recent news for {ticker}]
{news_section}

Based on your assessment of catalyst strength, narrative fit, and forward potential, rate on a scale of 1-5. 5 = strongest fit, 1 = no meaningful connection.

Fields (all Chinese text must be in Traditional Chinese):
- ticker: symbol (uppercase)
- rating: integer 1-5
- theme: matched theme name(s) from the list above (comma-separated, Traditional Chinese)
- feature: this stock's role or edge within the theme (max 20 Traditional Chinese characters)
- reason: rationale for the score (max 20 Traditional Chinese characters)""".format(
        today=TODAY,
        ticker=ticker,
        market_summary=market_summary,
        hot_themes=hot_themes_json,
        news_section=news_section,
    )

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL_SCORE,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0,
                top_p=0.95,
                top_k=1,
                max_output_tokens=512,
                response_mime_type="application/json",
                response_schema=ScoreResult,
            ),
        )
        result = json.loads(response.text.strip())
        result["ticker"] = ticker.upper()
        return result
    except Exception as e:
        print("❌ {}".format(str(e)[:100]))
        return None
    finally:
        time.sleep(4)


def main():
    gemini_key = os.environ.get("GEMINI_API_KEY_SCORE")
    if not gemini_key:
        raise EnvironmentError("GEMINI_API_KEY 未設定")

    client = genai.Client(api_key=gemini_key)
    os.makedirs("output", exist_ok=True)

    if not os.path.exists(INPUT_THEMES_JSON):
        raise FileNotFoundError("找不到 {}，請先執行 hot_theme.py".format(INPUT_THEMES_JSON))
    with open(INPUT_THEMES_JSON, "r", encoding="utf-8") as f:
        themes_data = json.load(f)
    hot_themes_json  = json.dumps(themes_data["hot_themes_list"], ensure_ascii=False, indent=2)
    market_summary   = themes_data.get("market_summary", "")

    with open(INPUT_TXT, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    rs_map = {}
    if os.path.exists(RS_CSV):
        with open(RS_CSV, "r", newline="") as f:
            for row in csv.DictReader(f):
                rs_map[row["ticker"].upper()] = row.get("RS", 0)

    news_cache = {}
    if os.path.exists(INPUT_NEWS_CACHE):
        with open(INPUT_NEWS_CACHE, "r", encoding="utf-8") as f:
            news_cache = json.load(f)
        print("📂 news cache 載入：{} 檔".format(len(news_cache)))

    progress = {}
    if os.path.exists(OUTPUT_PROGRESS):
        with open(OUTPUT_PROGRESS, "r", encoding="utf-8") as f:
            progress = json.load(f)
        print("📂 斷點進度載入：已完成 {} 檔".format(len(progress)))

    remaining = [t for t in tickers if t not in progress]
    print("📋 待評分：{} 檔，已完成：{} 檔，剩餘：{} 檔\n".format(
        len(tickers), len(progress), len(remaining)))

    failed_this_run = []

    for i, ticker in enumerate(tickers, 1):
        if ticker in progress:
            print("  [{:3d}/{}] {} （已完成，跳過）".format(i, len(tickers), ticker))
            continue

        news_text = news_cache.get(ticker, "")
        print("  [{:3d}/{}] {}".format(i, len(tickers), ticker), end=" ... ", flush=True)

        result = score_ticker(client, ticker, market_summary, hot_themes_json, news_text)

        if result is None:
            print("❌ 失敗，稍後重試")
            failed_this_run.append(ticker)
            continue

        progress[ticker] = {
            "ticker":  result.get("ticker", ticker),
            "RS":      rs_map.get(ticker, 0),
            "rating":  result.get("rating", 0),
            "theme":   result.get("theme", ""),
            "feature": result.get("feature", ""),
            "reason":  result.get("reason", ""),
        }
        print("rating={}".format(result.get("rating", "?")))

        with open(OUTPUT_PROGRESS, "w", encoding="utf-8") as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)

    if failed_this_run:
        print("\n⚠️ 本次未完成 {} 檔：{}".format(len(failed_this_run), ", ".join(failed_this_run)))
        _write_output(progress, rs_map)
        raise SystemExit(1)

    _write_output(progress, rs_map)
    print("\n✅ 全部完成")


def _write_output(progress, rs_map):
    final_rows = list(progress.values())
    final_rows.sort(key=lambda x: (-int(x["rating"]), -float(str(x["RS"]).replace(",", "") or 0)))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "RS", "rating", "theme", "feature", "reason"])
        writer.writeheader()
        writer.writerows(final_rows)

    watchlist = [row["ticker"] for row in final_rows if int(row["rating"]) >= RATING_THRESHOLD]
    with open(OUTPUT_WATCHLIST, "w", encoding="utf-8") as f:
        f.write("\n".join(watchlist) + "\n")

    print("   {} 檔已寫入 {}".format(len(final_rows), OUTPUT_CSV))
    print("   watchlist {} 檔 → {}".format(len(watchlist), OUTPUT_WATCHLIST))


if __name__ == "__main__":
    main()
