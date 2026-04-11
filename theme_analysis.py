import os
import csv
import json
import time
import datetime
import requests
from google import genai
from google.genai import types

# ── 設定 ──────────────────────────────────────────────────────────────────
INPUT_TXT        = "txt/technical_watchlist.txt"
RS_CSV           = "stock_data_rs.csv"
OUTPUT_CSV       = "csv/watchlist_summary.csv"
OUTPUT_WATCHLIST = "txt/watchlist.txt"
OUTPUT_THEMES    = "txt/hot_themes.txt"   # Phase 1 題材清單，方便人工檢查
RATING_THRESHOLD = 6

GEMINI_MODEL_PHASE1 = "gemini-3-flash-preview"
GEMINI_MODEL_PHASE3 = "gemini-3.1-flash-lite-preview"

SERPER_NEWS_MAX        = 10      # 每檔個股抓幾則新聞
SERPER_TIME_RANGE      = "qdr:w" # 過去一周；改 qdr:d 為過去 24 小時
TODAY      = datetime.date.today().strftime("%Y-%m-%d")
THIS_MONTH = datetime.date.today().strftime("%B %Y")

SERPER_THEMES_KEYWORDS = [       # Phase 1 大盤題材搜尋關鍵字，每個上限10則
    f"US stock market hot sectors themes momentum {THIS_MONTH}",
    f"top performing stock sectors this week {THIS_MONTH}",
    f"best performing industry groups stocks {THIS_MONTH}",
    f"stock market sector rotation leaders {THIS_MONTH}",
    f"high relative strength stocks sector catalyst {THIS_MONTH}",
]


# ── Serper 通用搜尋（回傳 snippet 文字）────────────────────────────────────
def serper_search(query: str, serper_key: str, num: int = 10, news: bool = True) -> str:
    endpoint = "https://google.serper.dev/news" if news else "https://google.serper.dev/search"
    try:
        resp = requests.post(
            endpoint,
            headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
            json={"q": query, "num": num, "tbs": SERPER_TIME_RANGE, "hl": "en", "gl": "us"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("news", data.get("organic", []))
        if not items:
            return ""
        return "\n".join(
            f"[{i.get('date', i.get('position', ''))}] {i.get('title', '')} — {i.get('snippet', '')}"
            for i in items
        )
    except Exception:
        return ""


# ── Phase 1：建立今日熱門題材 ─────────────────────────────────────────────
def fetch_hot_themes(client: genai.Client, rs95_tickers: list[tuple], serper_key: str) -> str:
    # 1-A：Serper 多角度搜尋大盤題材新聞（每個關鍵字最多10則，共最多50則）
    market_news_parts = []
    for kw in SERPER_THEMES_KEYWORDS:
        text = serper_search(kw, serper_key, num=10)
        if text:
            market_news_parts.append(text)
    market_news = "\n\n".join(market_news_parts) or "（無搜尋結果）"

    # 1-B：RS>=95 ticker 列表，附帶 RS 分數，依 RS 降序排列
    ticker_lines = "\n".join(f"  RS{rs:>2}  {t}" for t, rs in rs95_tickers)

    prompt = f"""今天是 {TODAY}。你是一位資深美股分析師，任務是建立今日市場熱門題材清單。

【資料來源 A：RS>=95 強勢股清單（共 {len(rs95_tickers)} 檔，依 RS 分數降序）】
{ticker_lines}

分析說明：
- RS 為相對強度分數，RS99 代表全市場前 1% 最強勢，RS95 代表前 5%
- 請依 RS 分數加權判斷題材熱度：RS99 的權重遠高於 RS95，RS98/97 次之
- 多檔 RS99 股票集中同一板塊，代表該題材資金極度集中，應列為頂級熱門
- 請對清單進行題材分類，計算各板塊的加權強度（高 RS 貢獻更多權重）

【資料來源 B：即時市場新聞（來自 Serper/Google News，多角度搜尋）】
{market_news}

【輸出要求】
綜合 A（RS 加權題材分析）和 B（即時新聞）各佔約 80%/20% 權重，整理出今日最熱門的 10 個投資題材。
格式：繁體中文，純文字條列，每行一個題材，附帶 1-2 句說明（含代表性個股的 RS 分數與板塊強度）。
題材排序應反映加權後的熱度，最強題材排最前。
禁止輸出 Markdown 標題或多餘格式。"""

    for attempt in range(5):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_PHASE1,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0,
                    max_output_tokens=2048,
                ),
            )
            return response.text.strip()
        except Exception as e:
            err = str(e)
            if "429" in err or "quota" in err.lower():
                time.sleep(60)
            elif attempt < 4:
                time.sleep(5)
            else:
                raise
    return ""


# ── Phase 2：Serper 搜尋個股新聞 ─────────────────────────────────────────
def fetch_stock_news(ticker: str, serper_key: str) -> str:
    return serper_search(
        f"{ticker} stock catalyst theme {THIS_MONTH}",
        serper_key,
        num=SERPER_NEWS_MAX,
        news=True,
    ) or "（無最新新聞）"


# ── Phase 3：Gemini Flash-Lite 評分 ──────────────────────────────────────
def score_ticker(client: genai.Client, ticker: str, news_text: str, hot_themes: str) -> dict:
    prompt = f"""你是一位資深美股分析師。今天是 {TODAY}。

## 今日市場熱門題材
{hot_themes}

## 個股：{ticker}
以下是過去一週的最新新聞摘要（來源：Google News via Serper）：
{news_text}

## 任務
判斷 {ticker} 與今日熱門題材的契合度，給予 1-10 評分。

評分標準：
- 10：當前最強主題核心標的，題材爆發性強，資金高度集中
- 9：強勢主題領頭羊，有明確催化劑，市場關注度極高
- 8：強勢板塊重要成員，題材清晰且有持續性資金流入
- 7：熱門板塊成員，但非核心，或題材仍在醞釀
- 6：題材有支撐，但競爭者多或關注度尚未聚焦
- 5：題材平淡，板塊輪動位置不明確
- 4：題材略顯老化，資金關注度下降
- 3：題材退燒，資金流出，基本面支撐有限
- 2：題材幾乎消失，市場已轉移焦點
- 1：完全被市場拋棄，與當前趨勢脫節

原則：新聞資訊優先於訓練知識；若新聞顯示該股活躍交易，忽略任何已下市/收購的舊知識；禁止自行聯網搜尋。

輸出：僅回傳單一 JSON 物件，繁體中文，欄位：
- "ticker": 代號（大寫）
- "rating": 1-10 整數
- "theme": 所有相關題材（逗號分隔）
- "feature": 與熱門題材關聯或競爭優勢（20字內）
- "reason": 評分理由（20字內）

禁止任何 Markdown 或額外說明。"""

    for attempt in range(5):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_PHASE3,
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
            time.sleep(2)
        except Exception as e:
            err = str(e)
            if "429" in err or "quota" in err.lower():
                time.sleep(60)
            elif attempt < 4:
                time.sleep(3)
            else:
                return {"ticker": ticker.upper(), "rating": 0, "theme": "", "feature": "", "reason": ""}
    return {"ticker": ticker.upper(), "rating": 0, "theme": "", "feature": "", "reason": ""}


# ── 主流程 ────────────────────────────────────────────────────────────────
def main():
    gemini_key = os.environ.get("GEMINI_API_KEY")
    serper_key = os.environ.get("SERPER_API_KEY")

    if not gemini_key:
        raise EnvironmentError("GEMINI_API_KEY 未設定")
    if not serper_key:
        raise EnvironmentError("SERPER_API_KEY 未設定")

    client = genai.Client(api_key=gemini_key)

    with open(INPUT_TXT, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    rs_map = {}
    rs95_tickers = []   # list of (ticker, rs_int), sorted by RS desc
    if os.path.exists(RS_CSV):
        with open(RS_CSV, "r", newline="") as f:
            for row in csv.DictReader(f):
                t = row["ticker"].upper()
                rs_val = row.get("RS", 0)
                rs_map[t] = rs_val
                try:
                    rs_int = int(float(rs_val))
                    if rs_int >= 95:
                        rs95_tickers.append((t, rs_int))
                except (ValueError, KeyError):
                    pass
    rs95_tickers.sort(key=lambda x: -x[1])   # RS 高 → 低

    print(f"📋 待分析：{len(tickers)} 檔 ／ RS>=95 參考股：{len(rs95_tickers)} 檔\n")

    # Phase 1
    print("📡 Phase 1：建立今日熱門題材...")
    hot_themes = fetch_hot_themes(client, rs95_tickers, serper_key)
    if not hot_themes:
        raise RuntimeError("Phase 1 失敗")

    os.makedirs("txt", exist_ok=True)
    with open(OUTPUT_THEMES, "w", encoding="utf-8") as f:
        f.write(f"# {TODAY}\n\n{hot_themes}\n")
    print(f"✅ 題材清單已存至 {OUTPUT_THEMES}\n")

    # Phase 2 + 3
    print(f"🔍 Phase 2+3：逐一處理（共 {len(tickers)} 檔）...\n")
    final_rows = []

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i:3d}/{len(tickers)}] {ticker}", end=" ... ", flush=True)
        news_text = fetch_stock_news(ticker, serper_key)
        result = score_ticker(client, ticker, news_text, hot_themes)
        final_rows.append({
            "ticker":  result.get("ticker", ticker),
            "RS":      rs_map.get(ticker, 0),
            "rating":  result.get("rating", 0),
            "theme":   result.get("theme", ""),
            "feature": result.get("feature", ""),
            "reason":  result.get("reason", ""),
        })
        print(f"rating={result.get('rating', '?')}")
        time.sleep(0.5)

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


if __name__ == "__main__":
    main()
