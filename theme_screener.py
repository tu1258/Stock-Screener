import os
import csv
import json
import time
import anthropic

# ── 設定 ──────────────────────────────────────────────────────────────────
INPUT_TXT  = "txt/technical_watchlist.txt"
RS_CSV     = "stock_data_rs.csv"
OUTPUT_CSV = "csv/theme_watchlist.csv"
OUTPUT_TXT = "txt/theme_watchlist.txt"
MODEL      = "claude-haiku-4-5"   # 最便宜；換成 claude-sonnet-4-6 可提升品質

def main():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY 未設定")

    client = anthropic.Anthropic(api_key=api_key)

    # 讀取 Tickers
    with open(INPUT_TXT, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    # 讀取 RS
    rs_map = {}
    if os.path.exists(RS_CSV):
        with open(RS_CSV, "r", newline="") as f:
            for row in csv.DictReader(f):
                rs_map[row['ticker'].upper()] = row.get('RS', 0)

    print(f"正在執行全量分析 (共 {len(tickers)} 檔)... 請稍候")

    prompt = f"""你是一位資深美股分析師。請針對以下 {len(tickers)} 檔股票進行「全局對比分析」：
清單：{', '.join(tickers)}

任務說明：
1. **趨勢偵測**：利用 web search 檢索「當前」全球美股市場的資金流向與熱門板塊趨勢。
2. **個股定位**：針對清單中的每檔個股，查明其所屬細分產業，並與當前市場趨勢進行比對。
3. **題材權重評分 (1-10)**：
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
4. 輸出繁體中文 JSON 陣列，每個物件必須嚴格包含以下欄位：
   - "ticker": 代號 (大寫)
   - "rating": 1-10 整數
   - "theme": 列出所有與該股票相關的題材
   - "feature": 與當前熱門題材相關性，或是獨特競爭優勢(20字左右)
   - "reason": 評分的理由(20字左右)

**禁令**：僅回傳原始 JSON 陣列，禁止包含任何 Markdown 標籤（如 ```json）、解釋文字或補充說明。"""

    # ── API 調用與重試邏輯 ──
    results = None
    for attempt in range(10):
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=8192,
                tools=[{
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "max_uses": 10,
                }],
                messages=[{"role": "user", "content": prompt}],
            )

            # Anthropic 回傳多個 content block，取最後一個 text block
            text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    text = block.text

            # Strip markdown fences 以防萬一
            raw = text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.lower().startswith("json"):
                    raw = raw[4:]
            results = json.loads(raw.strip())
            break

        except Exception as e:
            err = str(e)
            print(f"⚠️ API 第 {attempt + 1} 次調用失敗: {e}")

            if "400" in err or "invalid_request" in err.lower():
                print("❌ 請求參數錯誤，程式終止。")
                return
            if "401" in err or "authentication" in err.lower():
                print("❌ API Key 無效，程式終止。")
                return
            if "529" in err or "credit" in err.lower() or "balance" in err.lower():
                print("❌ 餘額不足，請至 platform.claude.com 儲值。")
                return
            if "429" in err:
                wait = 60
                print(f"  Rate limit，等待 {wait} 秒...")
                time.sleep(wait)
                continue

            if attempt < 9:
                time.sleep(5)
            else:
                print("❌ 已達到最大重試次數，程式終止。")
                return

    if not results:
        print("❌ 未取得有效結果。")
        return

    # 整合數據
    final_rows = []
    for item in results:
        t = item['ticker'].upper()
        final_rows.append({
            "ticker":  t,
            "RS":      rs_map.get(t, 0),
            "rating":  item.get("rating", 0),
            "theme":   item.get("theme", ""),
            "feature": item.get("feature", ""),
            "reason":  item.get("reason", ""),
        })

    # 排序：Rating 高 -> RS 高
    final_rows.sort(key=lambda x: (-int(x["rating"]), -int(x["RS"])))

    # 輸出 CSV
    os.makedirs("csv", exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "RS", "rating", "theme", "feature", "reason"])
        writer.writeheader()
        writer.writerows(final_rows)

    # 輸出 TXT
    os.makedirs("txt", exist_ok=True)
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f_txt:
        for row in final_rows:
            f_txt.write(f"{row['ticker']}\n")

    print(f"✅ 完成！已產出報表與 Ticker 清單。")

if __name__ == "__main__":
    main()
