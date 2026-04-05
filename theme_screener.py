import os
import csv
import json
import time
from google import genai
from google.genai import types

# ── 設定 ──────────────────────────────────────────────────────────────────
INPUT_TXT       = "txt/technical_watchlist.txt"
RS_CSV          = "stock_data_rs.csv"
THEME_TXT       = "txt/theme_watchlist.txt"       # 個股搜尋結果暫存
OUTPUT_CSV      = "csv/watchlist_summary.csv"
OUTPUT_TXT      = "txt/watchlist_summary.txt"
MODEL           = "gemini-3.1-flash-lite-preview"
SLEEP_BETWEEN   = 5   # 每次 API call 間隔（避免 RPM 超限）

def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key, http_options={'timeout': 600000})

    # 讀取 Tickers
    with open(INPUT_TXT, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    # 讀取 RS
    rs_map = {}
    if os.path.exists(RS_CSV):
        with open(RS_CSV, "r", newline="") as f:
            for row in csv.DictReader(f):
                rs_map[row['ticker'].upper()] = row.get('RS', 0)

    os.makedirs("txt", exist_ok=True)
    os.makedirs("csv", exist_ok=True)

    # ══════════════════════════════════════════════════════════════════
    # Step 1：逐檔搜尋，寫入 theme_watchlist.txt
    # ══════════════════════════════════════════════════════════════════
    print(f"Step 1：逐檔搜尋 (共 {len(tickers)} 檔)...")

    with open(THEME_TXT, "w", encoding="utf-8") as f_theme:
        for i, ticker in enumerate(tickers, 1):
            print(f"  [{i}/{len(tickers)}] {ticker}", end="  ")

            prompt_search = f"""你是一位資深美股研究員。請搜尋以下股票的最新資訊：
股票代號：{ticker}

請搜尋並整理以下內容（純文字，不需要 JSON）：
1. 所屬產業與細分領域
2. 當前最主要的題材與市場敘事
3. 近期重要催化劑（財報、產品、合約、政策等）
4. 與當前熱門市場趨勢的相關性
5. 任何值得注意的風險或負面因素

格式：
=== {ticker} ===
產業：...
題材：...
催化劑：...
趨勢相關性：...
風險：..."""

            result_text = ""
            for attempt in range(3):
                try:
                    response = client.models.generate_content(
                        model=MODEL,
                        contents=prompt_search,
                        config=types.GenerateContentConfig(
                            max_output_tokens=2048,
                            temperature=0
                        )
                    )
                    result_text = response.text.strip()
                    print("✓")
                    break
                except Exception as e:
                    err = str(e)
                    if "400" in err or "INVALID_ARGUMENT" in err:
                        print(f"✗ 參數錯誤: {e}")
                        break
                    print(f"⚠️ 第{attempt+1}次失敗，重試...")
                    time.sleep(5)
            else:
                result_text = f"=== {ticker} ===\n資料獲取失敗。"
                print("✗")

            f_theme.write(result_text + "\n\n")
            f_theme.flush()

            time.sleep(SLEEP_BETWEEN)

    print(f"\n✅ Step 1 完成 → {THEME_TXT}\n")

    # ══════════════════════════════════════════════════════════════════
    # Step 2：讀取 theme_watchlist.txt，做全局評分
    # ══════════════════════════════════════════════════════════════════
    print("Step 2：全局評分分析...")

    with open(THEME_TXT, "r", encoding="utf-8") as f:
        theme_content = f.read()

    prompt_rating = f"""你是一位資深美股分析師。以下是 {len(tickers)} 檔股票的個別研究資料：

{theme_content}

---
請根據以上資料，進行「全局對比分析」並評分。

評分標準（1-10分）：
- 10分：處於當前市場最強主題的核心標的，題材具爆發性且資金高度集中。
- 9分：強勢主題的領頭羊，具備明確催化劑且市場關注度極高。
- 8分：強勢板塊的重要成員，題材清晰且有持續性資金流入。
- 7分：屬於熱門板塊，但非核心標的，或題材仍在醞釀階段。
- 6分：題材有一定支撐，但競爭者多或市場關注度尚未聚焦。
- 5分：中性，題材平淡或板塊輪動位置不明確。
- 4分：題材略顯老化，資金關注度下降，短期缺乏催化劑。
- 3分：題材退燒或處於資金流出板塊，基本面支撐有限。
- 2分：題材幾乎消失，市場已轉移焦點，個股邊緣化明顯。
- 1分：基本面惡化或完全被市場拋棄，題材與當前趨勢完全脫節。

請輸出繁體中文 JSON 陣列，每個物件嚴格包含：
- "ticker": 代號（大寫字串）
- "rating": 1-10 整數
- "theme": 與該股相關的所有題材標籤（逗號分隔）
- "feature": 與當前熱門題材的相關性或獨特競爭優勢（20字左右）
- "reason": 此評分的核心理由（20字左右）

**禁令**：僅回傳原始 JSON 陣列，禁止任何 Markdown 標籤、解釋文字或補充說明。"""

    results = None
    for attempt in range(10):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=prompt_rating,
                config=types.GenerateContentConfig(
                    response_mime_type='application/json',
                    max_output_tokens=65536,
                    temperature=0
                )
            )
            results = json.loads(response.text)
            print("✅ 全局評分完成")
            break
        except Exception as e:
            err = str(e)
            print(f"⚠️ 第 {attempt + 1} 次失敗: {e}")
            if "400" in err or "INVALID_ARGUMENT" in err:
                print("❌ 參數錯誤，程式終止。")
                return
            if "429" in err or "RESOURCE_EXHAUSTED" in err:
                print("❌ 額度已用完，程式終止。")
                return
            if attempt < 9:
                time.sleep(5)
            else:
                print("❌ 已達最大重試次數，程式終止。")
                return

    if not results:
        return

    # ══════════════════════════════════════════════════════════════════
    # Step 3：輸出
    # ══════════════════════════════════════════════════════════════════
    final_rows = []
    rating_map = {item['ticker'].upper(): item for item in results}

    for ticker in tickers:
        item = rating_map.get(ticker, {})
        if not item:
            print(f"  [WARN] {ticker} 不在評分結果中")
        final_rows.append({
            "ticker":  ticker,
            "RS":      rs_map.get(ticker, 0),
            "rating":  item.get("rating", 0),
            "theme":   item.get("theme", ""),
            "feature": item.get("feature", ""),
            "reason":  item.get("reason", ""),
        })

    final_rows.sort(key=lambda x: (-int(x["rating"]), -int(x["RS"])))

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "RS", "rating", "theme", "feature", "reason"])
        writer.writeheader()
        writer.writerows(final_rows)

    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        for row in final_rows:
            f.write(f"{row['ticker']}\n")

    print(f"✅ 完成！")
    print(f"   {OUTPUT_CSV}")
    print(f"   {OUTPUT_TXT}")

if __name__ == "__main__":
    main()
