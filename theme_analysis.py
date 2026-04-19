import os
import csv
import json
import time
import datetime
from google import genai
from google.genai import types

INPUT_TXT         = "output/technical_watchlist.txt"
RS_CSV            = "stock_rs.csv"
INPUT_NEWS_CACHE  = "output/news_cache.json"
INPUT_THEMES_JSON = "output/hot_themes.json"
OUTPUT_PROGRESS   = "output/theme_progress.json"
OUTPUT_CSV        = "output/watchlist.csv"
OUTPUT_WATCHLIST  = "output/watchlist.txt"
RATING_THRESHOLD  = 6
SCORING_SLEEP     = 1

GEMINI_MODEL_SCORE = "gemini-2.5-flash-lite"

TODAY = datetime.date.today().strftime("%Y-%m-%d")


def score_ticker(client, ticker, hot_themes_text, news_text):
    news_section = "[Recent news for {} (past 30 days)]\n{}".format(ticker, news_text) if news_text else "(no recent news)"

    prompt = """You are a senior US equity analyst. Today is {today}.

Your task: score how well {ticker} belongs to today's hot investment themes.

A stock scores high if it clearly belongs to one or more of the top-ranked themes.
A stock scores low if it has no meaningful connection to any of the themes.

[Today's hot theme list — use this as your scoring framework]
{hot_themes}

[Recent news for {ticker} — use this together with your own knowledge to judge whether {ticker} fits the themes above]
{news_section}

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
- "feature": relevance to hot themes or competitive edge (max 20 Traditional Chinese characters)
- "reason": scoring rationale (max 20 Traditional Chinese characters)

No Markdown, no extra explanation.""".format(
        today=TODAY,
        ticker=ticker,
        hot_themes=hot_themes_text,
        news_section=news_section,
    )

    for attempt in range(5):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_SCORE,
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0, max_output_tokens=512),
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
            time.sleep(3)
        except Exception as e:
            err = str(e)
            print("\n  [score error {}] {}".format(ticker, err[:200]))
            wait = 60 if ("429" in err or "503" in err or "quota" in err.lower()) else 10
            time.sleep(wait)

    # 5 次都失敗，回傳 None 讓外層知道這檔沒跑成功
    return None


def main():
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        raise EnvironmentError("GEMINI_API_KEY 未設定")

    client = genai.Client(api_key=gemini_key)
    os.makedirs("output", exist_ok=True)

    if not os.path.exists(INPUT_THEMES_JSON):
        raise FileNotFoundError("找不到 {}，請先執行 hot_theme.py".format(INPUT_THEMES_JSON))
    with open(INPUT_THEMES_JSON, "r", encoding="utf-8") as f:
        themes_data = json.load(f)
    hot_themes_text = themes_data["hot_themes_text"]

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

    # 斷點進度
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

        result = score_ticker(client, ticker, hot_themes_text, news_text)

        if result is None:
            # 這檔失敗，記錄下來，繼續跑其他檔
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

        time.sleep(SCORING_SLEEP)

    # 如果還有失敗的，以非零 exit code 結束，讓 workflow 知道要重跑
    if failed_this_run:
        print("\n⚠️ 本次未完成 {} 檔：{}".format(len(failed_this_run), ", ".join(failed_this_run)))

        # 輸出目前已完成的結果（部分結果）
        _write_output(progress, rs_map)
        raise SystemExit(1)

    # 全部完成
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
