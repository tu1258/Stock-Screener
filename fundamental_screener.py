"""
fundamental_screener.py
- 讀 txt/watchlist.txt (由 screener.py 生成)
- 讀 csv/watchlist.csv 取 close 價格 (由 screener.py 生成)
- 每檔股票呼叫 Claude AI 5次，多數決定最終評分
- 輸出 csv/fundamental_watchlist.csv 和 txt/fundamental_watchlist.txt
"""

import os
import json
import time
import csv
from collections import Counter
import anthropic

# ── 路徑設定 (對齊你現有的 screener.py 輸出) ─────────────────────────────────
INPUT_TXT   = "txt/watchlist.txt"
INPUT_CSV   = "csv/watchlist.csv"
OUTPUT_CSV  = "csv/fundamental_watchlist.csv"
OUTPUT_TXT  = "txt/fundamental_watchlist.txt"

# ── AI 設定 ──────────────────────────────────────────────────────────────────
REPEAT_TIMES   = 5
SLEEP_BETWEEN  = 1.0   # 避免 rate limit
MODEL          = "claude-opus-4-5"
# ─────────────────────────────────────────────────────────────────────────────


def load_tickers() -> list[str]:
    with open(INPUT_TXT, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]


def load_prices() -> dict[str, float]:
    """從 watchlist.csv 讀取 ticker -> close 對應表"""
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


def call_claude(client: anthropic.Anthropic, ticker: str) -> dict | None:
    try:
        msg = client.messages.create(
            model=MODEL,
            max_tokens=400,
            messages=[{"role": "user", "content": build_prompt(ticker)}],
        )
        raw = msg.content[0].text.strip()
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
    """最多票的評分；同票取高分"""
    count = Counter(values)
    return max(count, key=lambda x: (count[x], x))


def analyze_ticker(client: anthropic.Anthropic, ticker: str, price: float) -> dict | None:
    print(f"\n  [{ticker}]  close={price}")
    valid = []

    for i in range(REPEAT_TIMES):
        result = call_claude(client, ticker)
        if result:
            valid.append(result)
            stars = "⭐" * result["rating"]
            print(f"    round {i+1}: {stars} ({result['rating']})")
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

    # 依 rating 高→低，同 rating 依 ticker A→Z
    records.sort(key=lambda x: (-x["rating"], x["ticker"]))

    # CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["rating", "ticker", "close", "fundamentals", "theme", "all_ratings"],
        )
        writer.writeheader()
        writer.writerows(records)
    print(f"\n✅ CSV → {OUTPUT_CSV}")

    # TXT (ticker only，依 rating 排序)
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        for r in records:
            f.write(r["ticker"] + "\n")
    print(f"✅ TXT → {OUTPUT_TXT}")


def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY not set")

    client  = anthropic.Anthropic(api_key=api_key)
    tickers = load_tickers()
    prices  = load_prices()

    print(f"Loaded {len(tickers)} tickers from {INPUT_TXT}")

    records = []
    for ticker in tickers:
        price  = prices.get(ticker, 0.0)
        result = analyze_ticker(client, ticker, price)
        if result:
            records.append(result)

    if not records:
        print("No records generated.")
        return

    write_outputs(records)

    # 統計摘要
    print(f"\n📊 {len(records)} stocks analyzed")
    dist = Counter(r["rating"] for r in records)
    for star in range(5, 0, -1):
        bar = "█" * dist.get(star, 0)
        print(f"  {'⭐' * star}  {dist.get(star, 0):>3}  {bar}")


if __name__ == "__main__":
    main()
