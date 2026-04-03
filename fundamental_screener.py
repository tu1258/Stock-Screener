import os
import csv
import json
from google import genai
from google.genai import types

# ── 設定 ──────────────────────────────────────────────────────────────────
INPUT_TXT     = "txt/technical_watchlist.txt"
RS_CSV        = "stock_data_rs.csv"
OUTPUT_CSV    = "csv/fundamental_watchlist.csv"
MODEL         = "gemini-2.5-flash"

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

    # 核心 Prompt：要求 AI 一次性完成所有邏輯
    prompt = f"""你是一位資深美股分析師。請針對以下 {len(tickers)} 檔股票進行「全局對比分析」：
清單：{', '.join(tickers)}

任務說明：
1. 針對每檔股票檢索其基本面指標與當前市場題材。
2. 進行橫向對比，給予 1-5 分的評分 (5分為最推薦)。請基於這整個清單的強弱分配評分，確保評分具有區別性。
3. **分配公平性要求**：請確保每一檔股票的分析深度一致，不要因為清單較長而簡略或跳過後方的股票內容。
4. 輸出繁體中文 JSON 陣列，每個物件必須嚴格包含以下欄位：
   - "ticker": 代號 (大寫)
   - "rating": 1-5 整數
   - "theme": 題材關鍵字 (例如：AI 伺服器, 能源)
   - "fundamental": 一句話基本面簡述 (包含營收或獲利亮點)
   - "feature": 一句話核心特色 (該股在產業中的獨特優勢)

**禁令**：僅回傳原始 JSON 陣列，禁止包含任何 Markdown 標籤（如 ```json）、解釋文字或補充說明。"""

    try:
        response = client.models.generate_content(
            model=MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type='application/json',
                max_output_tokens=12288,  # 支撐 100 檔的輸出空間
                temperature=0.2          # 降低隨機性，確保長輸出的格式穩定
            )
        )
        
        results = json.loads(response.text)
        
        # 整合數據
        final_rows = []
        for item in results:
            t = item['ticker'].upper()
            final_rows.append({
                "ticker": t,
                "RS": rs_map.get(t, 0),
                "rating": item.get("rating", 0),
                "theme": item.get("theme", ""),
                "fundamental": item.get("fundamental", ""),
                "feature": item.get("feature", "")
            })

        # 排序：Rating 高 -> RS 高
        final_rows.sort(key=lambda x: (-int(x["rating"]), -int(x["RS"])))

        # 輸出 CSV
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=["ticker", "RS", "rating", "theme", "fundamental", "feature"])
            writer.writeheader()
            writer.writerows(final_rows)

        print(f"✅ 完成！已產出評分報表：{OUTPUT_CSV}")

    except Exception as e:
        print(f"❌ 執行失敗: {e}")

if __name__ == "__main__":
    main()
