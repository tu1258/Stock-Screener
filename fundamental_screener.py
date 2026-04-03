"""
fundamental_screener.py
- 讀 txt/watchlist.txt (由 screener.py 生成)
- 讀 csv/watchlist.csv 取 close 價格 (由 screener.py 生成)
- 每檔股票呼叫 Gemini AI 3次，多數決定最終評分
- 輸出 csv/fundamental_watchlist.csv 和 txt/fundamental_watchlist.txt

免費額度: Gemini 2.0 Flash → 10 RPM / 250 req/day (不需信用卡)
80檔 × 3次 = 240 req，在免費額度內
"""

import os
import json
import time
import csv
from collections import Counter
import google.generativeai as genai

# ── 路徑設定 ─────────────────────────────────────────────────────────────────
INPUT_TXT  = "txt/watchlist.txt"
INPUT_CSV  = "csv/watchlist.csv"
OUTPUT_CSV = "csv/fundamental_watchlist.csv"
OUTPUT_TXT = "txt/fundamental_watchlist.txt"

# ── AI 設定 ──────────────────────────────────────────────────────────────────
REPEAT_TIMES  = 3      # 免費版每天250 req，80檔×3=240，剛好夠
SLEEP_BETWEEN = 6.5    # 免費版 10 RPM → 每6秒1次，留buffer
MODEL         = "gemini-2.0-flash"
# ─────────────────────────────────────────────────────────────────────────────


def load_tickers() -> list[str]:
    with open(INPUT_TXT, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]


def load_prices() -> dict[str, float]:
    prices = {}
    if not os.path.exists(INPUT_CSV):
        print(f"[WARN] {INPUT_CSV} not found, prices will show as 0.0")
        return prices
    with open(INPUT_CSV, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get("ticker", "").strip().upper()
            try:
                prices[ticker] = round(float(row.get("close", 0)), 3)
            except (ValueError, TypeError):
                prices[ticker] = 0.0
    return prices


def build_prompt(ticker: str) -> str:
    return f"""You are a professional US stock analyst. Analyze **{ticker}** and reply ONLY with a valid JSON object — no markdown, no extra text, no explanation.

Required fields:
- "fundamentals": 1-2 sentences covering revenue growth, profitability, and valuation (P/E, P/S, etc.)
- "theme": 1-2 sentences on the key narrative or speculative catalyst (e.g. AI infrastructure, GLP-1, defense, reshoring, etc.)
- "rating": integer 1 to 5
  5=Exceptional  4=Strong  3=Neutral  2=Weak  1=Avoid

Example:
{{"fundamentals": "Revenue grew 35% YoY with expanding margins and reasonable P/S of 8x.", "theme": "Key AI infrastructure play benefiting from datacenter buildout cycle.", "rating": 4}}"""


def call_gemini(model, ticker: str) -> dict | None:
    try:
        response = model.generate_content(build_prompt(ticker))
        raw = response.text.strip()
        # Strip markdown fences if model accidentally adds them
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.lower().startswith("json"):
                raw = raw[4:]
        result = json.loads(raw.strip())
        # Validate
        assert isinstance(result.get("rating"), int) and 1 <= result["rating"] <= 5
        assert isinstance(result.get("fundamentals"), str) and result["fundamentals"]
        assert isinstance(result.get("theme"), str) and result["theme"]
        return result
    except Exception as e:
        print(f"      [WARN] {e}")
        return None


def majority_vote(values: list[int]) -> int:
    count = Counter(values)
    return max(count, key=lambda x: (count[x], x))


def analyze_ticker(model, ticker: str, price: float) -> dict | None:
    print(f"\n  [{ticker}]  close={price}")
    valid = []

    for i in range(REPEAT_TIMES):
        result = call_gemini(model, ticker)
        if result:
            valid.append(result)
            print(f"    round {i+1}: {'⭐' * result['rating']} ({result['rating']})")
        else:
            print(f"    round {i+1}: invalid, skipped")
        time.sleep(SLEEP_BETWEEN)

    if not valid:
        print(f"  [SKIP] {ticker} — 0 valid responses")
        return None

    all_ratings  = [r["rating"] for r in valid]
    final_rating = majority_vote(all_ratings)
    rep          = next((r for r in valid if r["rating"] == final_rating), valid[0])

    print(f"  → Final: {'⭐' * final_rating}  votes={all_ratings}")
    return {
        "rating":       final_rating,
        "ticker":       ticker,
        "close":        price,
        "fundamentals": rep["fundamentals"],
        "theme":        rep["theme"],
        "all_ratings":  ",".join(map(str, all_ratings)),
    }


def write_outputs(records: list[dict]) -> None:
    os.makedirs("csv", exist_ok=True)
    os.makedirs("txt", exist_ok=True)

    records.sort(key=lambda x: (-x["rating"], x["ticker"]))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["rating", "ticker", "close", "fundamentals", "theme", "all_ratings"],
        )
        writer.writeheader()
        writer.writerows(records)
    print(f"\n✅ CSV → {OUTPUT_CSV}")

    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        for r in records:
            f.write(r["ticker"] + "\n")
    print(f"✅ TXT → {OUTPUT_TXT}")


def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError("GEMINI_API_KEY not set")

    genai.configure(api_key=api_key)
    model   = genai.GenerativeModel(MODEL)
    tickers = load_tickers()
    prices  = load_prices()

    print(f"Loaded {len(tickers)} tickers from {INPUT_TXT}")
    print(f"Estimated time: ~{len(tickers) * REPEAT_TIMES * SLEEP_BETWEEN / 60:.1f} minutes\n")

    records = []
    for ticker in tickers:
        price  = prices.get(ticker, 0.0)
        result = analyze_ticker(model, ticker, price)
        if result:
            records.append(result)

    if not records:
        print("No records generated.")
        return

    write_outputs(records)

    print(f"\n📊 {len(records)} stocks analyzed")
    dist = Counter(r["rating"] for r in records)
    for star in range(5, 0, -1):
        bar = "█" * dist.get(star, 0)
        print(f"  {'⭐' * star}  {dist.get(star, 0):>3}  {bar}")


if __name__ == "__main__":
    main()
