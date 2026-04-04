import os
import csv
import json
import time
from google import genai
from google.genai import types

# ── 設定 ──────────────────────────────────────────────────────────────────
INPUT_TXT      = "txt/technical_watchlist.txt"
RS_CSV         = "stock_data_rs.csv"
OUTPUT_CSV     = "csv/fundamental_watchlist.csv"
OUTPUT_TXT     = "txt/fundamental_watchlist.txt"
MODEL          = "gemini-3-flash-preview"

def main():
    api_key = os.environ.get("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)
    
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

    # 核心 Prompt
    prompt = f"""你是一位資深美股分析師。請針對以下 {len(tickers)} 檔股票進行「全局對比分析」：
清單：{', '.join(tickers)}

任務說明：
1. **深度檢索**：針對每檔股票檢索其當前市場題材。
2. **全局對比評分**：進行橫向對比，給予 1-10 分的評分 (10分為最推薦)。
3. **詳盡分析**：針對每一檔股票提供重點的描述，字數應於20字以內。
4. 輸出繁體中文 JSON 陣列，每個物件必須嚴格包含以下欄位：
   - "ticker": 代號 (大寫)
   - "rating": 1-10 整數
   - "theme": 相關題材
   - "feature": 核心競爭優勢或題材相關性
   - "reason": 評分的理由
   
**禁令**：僅回傳原始 JSON 陣列，禁止包含任何 Markdown 標籤（如 ```json）、解釋文字或補充說明。"""

    # ── API 調用與重試邏輯 ──
    results = None
    for attempt in range(10): # 最多重試 10 次
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type='application/json',
                    max_output_tokens=65536,
                    temperature=0.25
                )
            )
            results = json.loads(response.text)
            break # 成功拿到資料就跳出重試
        except Exception as e:
            print(f"⚠️ API 第 {attempt + 1} 次調用失敗: {e}")
            if attempt < 9:
                time.sleep(5) # 等待 5 秒後重試
            else:
                print("❌ 已達到最大重試次數，程式終止。")
                return

    # 整合數據
    final_rows = []
    for item in results:
        t = item['ticker'].upper()
        final_rows.append({
            "ticker": t,
            "RS": rs_map.get(t, 0),
            "rating": item.get("rating", 0),
            "theme": item.get("theme", ""),
            "feature": item.get("feature", ""),
            "reason": item.get("reason", "")
        })

    # 排序：Rating 高 -> RS 高
    final_rows.sort(key=lambda x: (-int(x["rating"]), -int(x["RS"])))

    # 輸出 CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["ticker", "RS", "rating", "theme", "feature", "reason"])
        writer.writeheader()
        writer.writerows(final_rows)

    # 輸出 TXT (僅輸出 Ticker)
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f_txt:
        for row in final_rows:
            f_txt.write(f"{row['ticker']}\n")
        
    print(f"✅ 完成！已產出報表與 Ticker 清單。")

if __name__ == "__main__":
    main()
