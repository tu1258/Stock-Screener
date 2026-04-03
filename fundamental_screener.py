"""
fundamental_screener.py

流程：
1. 讀 txt/watchlist.txt → 每檔跑一次 Gemini，做基本面+題材面分析
2. 所有分析結果累積寫入 txt/watchlist_summary.txt
3. 全部跑完後，把整個 summary 丟給 Gemini 做最終評分
4. 輸出 csv/fundamental_watchlist.csv
   欄位: ticker, RS, rating, theme, 個股特色

使用 google-genai (新版 SDK): from google import genai
免費額度: Gemini 2.0 Flash → 15 RPM / 1000 req/day
"""

import os
import csv
import time
import json
from collections import Counter
from google import genai
from google.genai import types

# ── 路徑設定 ─────────────────────────────────────────────────────────────────
INPUT_TXT    = "txt/watchlist.txt"
INPUT_CSV    = "csv/watchlist.csv"
RS_CSV       = "stock_data_rs.csv"
SUMMARY_TXT  = "txt/watchlist_summary.txt"
OUTPUT_CSV   = "csv/fundamental_watchlist.csv"

# ── AI 設定 ──────────────────────────────────────────────────────────────────
MODEL = "gemini-2.0-flash"
SLEEP_BETWEEN = 10   # 15 RPM → 每4秒1次，留buffer
# ─────────────────────────────────────────────────────────────────────────────


def load_tickers() -> list[str]:
    with open(INPUT_TXT, "r") as f:
        return [line.strip().upper() for line in f if line.strip()]


def load_prices() -> dict[str, float]:
    prices = {}
    if not os.path.exists(INPUT_CSV):
        return prices
    with open(INPUT_CSV, "r", newline="") as f:
        for row in csv.DictReader(f):
            ticker = row.get("ticker", "").strip().upper()
            try:
                prices[ticker] = round(float(row.get("close", 0)), 3)
            except (ValueError, TypeError):
                prices[ticker] = 0.0
    return prices


def load_rs() -> dict[str, int]:
    rs_map = {}
    if not os.path.exists(RS_CSV):
        print(f"[WARN] {RS_CSV} not found")
        return rs_map
    with open(RS_CSV, "r", newline="") as f:
        for row in csv.DictReader(f):
            ticker = row.get("ticker", "").strip().upper()
            try:
                rs_map[ticker] = int(float(row.get("RS", 0)))
            except (ValueError, TypeError):
                rs_map[ticker] = 0
    return rs_map


# ── Step 1: 每檔分析 prompt ───────────────────────────────────────────────────

def analysis_prompt(ticker: str, price: float) -> str:
    return f"""You are a professional US stock analyst. Analyze the stock **{ticker}** (current price: ${price}).

Write a concise analysis in this exact format (plain text, no JSON, no markdown):

FUNDAMENTALS: [1-2 sentences on revenue growth, profitability, valuation metrics like P/E or P/S]
THEME: [1-2 sentences on the key narrative or catalyst driving this stock, e.g. AI infrastructure, GLP-1, defense spending, reshoring, optical networking, etc.]"""


def analyze_one(client, ticker: str, price: float) -> str | None:
    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=analysis_prompt(ticker, price),
        )
        return response.text.strip()
    except Exception as e:
        print(f"  [WARN] {ticker}: {e}")
        return None


# ── Step 2: 批次評分 prompt ───────────────────────────────────────────────────

def rating_prompt(summary_content: str) -> str:
    return f"""You are a professional US stock analyst. Below is a fundamental and thematic analysis summary for multiple stocks.

For EVERY stock listed, output a JSON array. Each object must have exactly these fields:
- "ticker": stock symbol (string)
- "rating": integer 1 to 5 (5=Exceptional, 4=Strong, 3=Neutral, 2=Weak, 1=Avoid)
- "theme": short comma-separated theme tags in mixed English/Chinese, e.g. "AI,光通訊" or "Defense,Reshoring"
- "feature": one concise English sentence describing this stock's specific role or edge

Reply ONLY with the raw JSON array. No markdown fences, no explanation, no extra text.

--- STOCK SUMMARIES ---
{summary_content}"""


def batch_rate(client, summary_content: str) -> list[dict] | None:
    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=rating_prompt(summary_content),
            config=types.GenerateContentConfig(
                max_output_tokens=8192,
            ),
        )
        raw = response.text.strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.lower().startswith("json"):
                raw = raw[4:]
        results = json.loads(raw.strip())
        assert isinstance(results, list)
        return results
    except Exception as e:
        print(f"[ERROR] Batch rating failed: {e}")
        return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise EnvironmentError("GEMINI_API_KEY not set")

    client  = genai.Client(api_key=api_key)
    tickers = load_tickers()
    prices  = load_prices()
    rs_map  = load_rs()

    os.makedirs("txt", exist_ok=True)
    os.makedirs("csv", exist_ok=True)

    print(f"Loaded {len(tickers)} tickers")
    print(f"Estimated time: ~{len(tickers) * SLEEP_BETWEEN / 60:.1f} min\n")

    # ── Step 1: 逐檔分析，邊跑邊寫入 summary ──────────────────────────────────
    summary_lines = []

    with open(SUMMARY_TXT, "w", encoding="utf-8") as f_out:
        for i, ticker in enumerate(tickers, 1):
            price = prices.get(ticker, 0.0)
            print(f"[{i}/{len(tickers)}] {ticker} (close={price})", end="  ")

            analysis = analyze_one(client, ticker, price)

            if analysis:
                block = f"=== {ticker} ===\n{analysis}"
                print("✓")
            else:
                block = f"=== {ticker} ===\nFUNDAMENTALS: Data unavailable.\nTHEME: Data unavailable."
                print("✗ fallback")

            summary_lines.append(block)
            f_out.write(block + "\n\n")
            f_out.flush()

            time.sleep(SLEEP_BETWEEN)

    print(f"\n✅ Summary → {SUMMARY_TXT}  ({len(summary_lines)} stocks)\n")

    # ── Step 2: 一次批次評分 ───────────────────────────────────────────────────
    print("Running batch rating (1 API call)...")
    summary_content = "\n\n".join(summary_lines)
    ratings = batch_rate(client, summary_content)

    if not ratings:
        print("[ERROR] Batch rating failed. CSV not generated.")
        return

    # ── Step 3: 輸出 CSV ───────────────────────────────────────────────────────
    rating_map = {r["ticker"].upper(): r for r in ratings}

    rows = []
    for ticker in tickers:
        r = rating_map.get(ticker, {})
        rows.append({
            "ticker":  ticker,
            "RS":      rs_map.get(ticker, ""),
            "rating":  r.get("rating", ""),
            "theme":   r.get("theme", ""),
            "個股特色":  r.get("feature", ""),
        })
        if not r:
            print(f"  [WARN] {ticker} missing from batch rating output")

    # 依 rating 高→低，RS 高→低
    rows.sort(key=lambda x: (
        -int(x["rating"]) if str(x["rating"]).isdigit() else 0,
        -int(x["RS"])     if str(x["RS"]).isdigit()     else 0,
    ))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "RS", "rating", "theme", "個股特色"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ CSV → {OUTPUT_CSV}\n")

    dist = Counter(str(r["rating"]) for r in rows if str(r["rating"]).isdigit())
    print(f"📊 {len(rows)} stocks rated")
    for star in range(5, 0, -1):
        count = dist.get(str(star), 0)
        print(f"  {'⭐' * star}  {count:>3}  {'█' * count}")


if __name__ == "__main__":
    main()
