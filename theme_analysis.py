import os
import csv
import json
import time
import datetime
import pandas as pd
import yfinance as yf
from google import genai
from google.genai import types

# ── 設定 ──────────────────────────────────────────────────────────────────
INPUT_TXT        = "txt/technical_watchlist.txt"
RS_CSV           = "stock_rs.csv"
OUTPUT_CSV       = "csv/watchlist_summary.csv"
OUTPUT_WATCHLIST = "txt/watchlist.txt"
OUTPUT_THEMES    = "txt/hot_themes.txt"
OUTPUT_HOT_WL    = "txt/hot_theme_watchlist.txt"
RATING_THRESHOLD = 6

GEMINI_MODEL_PHASE1    = "gemini-3-flash-preview"
GEMINI_MODEL_GROUNDING = "gemini-3.1-flash-lite-preview"

STOCK_DATA_CSV    = "stock_data.csv"
INDUSTRY_RS_CSV   = "industry_rs.csv"
TICKER_IND_CSV    = "ticker_industry.csv"
MIN_AVG_VALUE_10M = 100

GROUNDING_SLEEP = 5

TODAY = datetime.date.today().strftime("%Y-%m-%d")


# ── 計算 RS95+ 且成交值 >= 100M 的清單 ──────────────────────────────────
def load_rs95_liquid(rs_csv: str, stock_data_csv: str) -> list[tuple]:
    rs_map = {}
    with open(rs_csv, newline="") as f:
        for row in csv.DictReader(f):
            try:
                rs_int = int(float(row.get("RS", 0)))
                if rs_int >= 95:
                    rs_map[row["ticker"].upper()] = rs_int
            except (ValueError, KeyError):
                pass

    if not os.path.exists(stock_data_csv) or not rs_map:
        return sorted(rs_map.items(), key=lambda x: -x[1])

    price_df = pd.read_csv(stock_data_csv, parse_dates=["date"],
                           usecols=["ticker", "date", "close", "volume"])
    price_df = price_df[price_df["ticker"].str.upper().isin(rs_map)]
    price_df = price_df.sort_values(["ticker", "date"])

    price_df["daily_value"] = price_df["close"] * price_df["volume"] / 1_000_000
    avg_val = (
        price_df.groupby("ticker")["daily_value"]
        .apply(lambda x: x.tail(10).mean())
        .reset_index()
        .rename(columns={"daily_value": "avg_value_10"})
    )
    avg_val["ticker"] = avg_val["ticker"].str.upper()

    liquid = avg_val[avg_val["avg_value_10"] >= MIN_AVG_VALUE_10M]["ticker"].tolist()
    liquid_set = set(liquid)

    result = [(t, rs) for t, rs in rs_map.items() if t in liquid_set]
    return sorted(result, key=lambda x: -x[1])


# ── 讀取 industry RS 排行 ────────────────────────────────────────────────
def load_industry_ranking(industry_rs_csv: str, ticker_ind_csv: str) -> tuple[str, dict]:
    ticker_to_industry = {}
    if os.path.exists(ticker_ind_csv):
        df_ind = pd.read_csv(ticker_ind_csv)
        for _, row in df_ind.iterrows():
            t = str(row.get("ticker", "")).upper()
            ind = str(row.get("industry", ""))
            if t and ind:
                ticker_to_industry[t] = ind

    if not os.path.exists(industry_rs_csv):
        return "", ticker_to_industry

    df = pd.read_csv(industry_rs_csv)
    df = df.sort_values("avg_rs", ascending=False)

    lines = []
    for _, row in df.iterrows():
        industry = row.get("industry", "")
        sector   = row.get("sector", "")
        avg_rs   = row.get("avg_rs", 0)
        count    = int(row.get("ticker_count", 0))
        lines.append(f"  {avg_rs:5.1f}  {industry}（{sector}，{count} 檔）")

    return "\n".join(lines), ticker_to_industry


# ── Search Grounding 個股摘要 ────────────────────────────────────────────
def fetch_ticker_summary(client: genai.Client, ticker: str) -> str:
    prompt = f"""今天是 {TODAY}。請搜尋 {ticker} 股票的近期資訊，並從投資題材與市場炒作的角度進行分析：
- 這支股票的主要業務是什麼？
- 這支股票目前最強的投資題材是什麼？為什麼市場在關注它？
- 近期有哪些催化劑（財報、產品、合作、法規、產業趨勢）推動股價？
- 它在哪個板塊或題材中具備炒作潛力或稀缺性？

請條列整理，聚焦在題材面與炒作邏輯，不需要列流水帳新聞。"""

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_GROUNDING,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0,
                    max_output_tokens=1024,
                ),
            )
            return response.text.strip()
        except Exception as e:
            err = str(e)
            if "429" in err or "quota" in err.lower():
                time.sleep(60)
            elif attempt < 2:
                time.sleep(5)
    return ""


# ── Phase 1：建立今日熱門題材 ─────────────────────────────────────────────
def fetch_hot_themes(client: genai.Client, rs95_tickers: list[tuple],
                     industry_text: str, ticker_to_industry: dict) -> tuple:
    print(f"  用 search grounding 抓取 {len(rs95_tickers)} 檔個股摘要...")

    ticker_summaries = {}
    for i, (ticker, rs) in enumerate(rs95_tickers, 1):
        print(f"    [{i:3d}/{len(rs95_tickers)}] {ticker} RS{rs}", end=" ... ", flush=True)
        summary = fetch_ticker_summary(client, ticker)
        ticker_summaries[ticker] = summary
        print("✓" if summary else "（無）")
        time.sleep(GROUNDING_SLEEP)

    ticker_lines = "\n".join(f"  RS{rs:>2}  {t}" for t, rs in rs95_tickers)

    ticker_ind_lines = "\n".join(
        f"  {t}：{ticker_to_industry.get(t, 'N/A')}"
        for t, rs in rs95_tickers
    )

    summaries_text = "\n\n".join(
        f"[{t} RS{rs}]\n{ticker_summaries.get(t, '（無資料）')}"
        for t, rs in rs95_tickers
    )

    industry_section = ""
    if industry_text:
        industry_section = f"""
【資料來源 C：各 Industry 平均 RS 排行（依全市場個股 RS 分數計算，降序）】
{industry_text}

分析說明：
- 此排行反映整體 industry 的強弱，而非個別股票
- Industry 平均 RS 高，代表該板塊整體資金動能強，不只是少數個股拉抬
- 請將此數據與資料來源 A 的個股 RS 交叉比對，避免單一暴漲股扭曲題材判斷
"""

    prompt = f"""今天是 {TODAY}。你是一位資深美股分析師，任務是建立今日市場熱門題材清單。

【資料來源 A：RS>=95 且成交值>=100M 強勢股清單（共 {len(rs95_tickers)} 檔，依 RS 分數降序）】
{ticker_lines}

【資料來源 B：RS95+ 個股所屬 Sector（僅供參考，請以你對各公司業務的專業知識為主進行分類）】
{ticker_ind_lines}
{industry_section}
【資料來源 D：RS95+ 個股近期新聞摘要（來源：Google Search Grounding）】
{summaries_text}

分析說明：
- RS 為相對強度分數，RS99 代表全市場前 1% 最強勢，RS95 代表前 5%
- 請依 RS 分數加權判斷題材熱度：RS越高權重越高
- 多檔 RS99 股票集中同一板塊，代表該題材資金極度集中，應列為頂級熱門
- 一檔個股可歸屬多個題材
- 公司業務分類請優先使用你對各公司的專業知識，資料來源 B 的 industry 分類較粗糙僅供輔助參考
- 資料來源 D 的摘要用於判斷近期催化劑與市場關注度
- 請同時參考 Industry 平均 RS，若某題材個股 RS 高但 Industry 整體 RS 偏低，熱度評分應適度保守

【輸出格式：嚴格回傳以下 JSON，禁止任何 Markdown 或額外說明】
{{
  "hot_themes": [
    {{
      "name": "題材名稱",
      "desc": "說明（含代表性個股RS分數）",
      "tickers": ["TICKER1", "TICKER2"]
    }},
    ...
  ],
  "ticker_themes": {{
    "TICKER": ["題材A", "題材B"],
    ...
  }}
}}

要求：
- hot_themes：依加權熱度排序的前 10 大題材，每條附 1-2 句說明，tickers 列出該題材所有相關個股（大寫）
- ticker_themes：對清單中每一檔股票標注所有相關題材（可多個），分類請基於你對公司業務的專業知識
- 題材用繁體中文，力求精準"""

    for attempt in range(5):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_PHASE1,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0,
                    max_output_tokens=8192,
                ),
            )
            raw = response.text.strip()
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.lower().startswith("json"):
                    raw = raw[4:]
            data = json.loads(raw.strip())

            hot_themes_list: list = data.get("hot_themes", [])
            ticker_themes: dict   = data.get("ticker_themes", {})

            rs_lookup = {t: rs for t, rs in rs95_tickers}

            theme_count: dict[str, list[str]] = {}
            for ticker_upper, themes in ticker_themes.items():
                t = ticker_upper.upper()
                for theme in themes:
                    theme_count.setdefault(theme, []).append(
                        f"{t}(RS{rs_lookup.get(t, '?')})"
                    )

            lines = [f"## 題材統計（RS95+ 且成交值>=100M，共 {len(rs95_tickers)} 檔）\n"]
            for theme, members in sorted(theme_count.items(), key=lambda x: -len(x[1])):
                members_str = ", ".join(members[:20])
                suffix = f"... 等共 {len(members)} 檔" if len(members) > 20 else f"共 {len(members)} 檔"
                lines.append(f"{theme}：{members_str}　{suffix}")

            lines.append("\n## 今日熱門題材（Gemini 分析）\n")
            for item in hot_themes_list:
                name    = item.get("name", "")
                desc    = item.get("desc", "")
                tickers = item.get("tickers", [])
                lines.append(f"{name} — {desc}（{', '.join(tickers)}）")

            themes_text = "\n".join(lines)

            return themes_text, hot_themes_list, rs_lookup

        except (json.JSONDecodeError, KeyError):
            time.sleep(2)
        except Exception as e:
            err = str(e)
            if "429" in err or "quota" in err.lower():
                time.sleep(60)
            elif attempt < 4:
                time.sleep(5)
            else:
                raise

    return "", [], {}


# ── 產生 hot_theme_watchlist ──────────────────────────────────────────────
def build_hot_theme_watchlist(
    hot_themes_list: list[dict],
    rs_lookup: dict[str, int],
    top_n: int = 5,
) -> list[str]:
    result = []
    seen   = set()
    for item in hot_themes_list[:top_n]:
        tickers_in_group = sorted(
            [t.upper() for t in item.get("tickers", [])],
            key=lambda t: -rs_lookup.get(t, 0),
        )
        for t in tickers_in_group:
            if t not in seen:
                seen.add(t)
                result.append(t)
    return result


# ── Phase 3：Search Grounding 評分 ───────────────────────────────────────
def score_ticker(client: genai.Client, ticker: str, hot_themes: str) -> dict:
    prompt = f"""你是一位資深美股分析師。今天是 {TODAY}。

以下是今日市場熱門題材清單，這是根據全市場 RS 分數與資金流向計算得出的結果：

{hot_themes}

請搜尋 {ticker} 最近的新聞與業務動向，然後判斷它與上述熱門題材清單的契合度，給予 1-10 評分。
評分依據是 {ticker} 與上述題材清單的實際關聯程度，而非你自行判斷該股是否熱門。

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

輸出：僅回傳單一 JSON 物件，繁體中文，欄位：
- "ticker": 代號（大寫）
- "rating": 1-10 整數
- "theme": 對應到上述熱門題材清單中的題材名稱（逗號分隔）
- "feature": 與熱門題材關聯或競爭優勢（20字內）
- "reason": 評分理由（20字內）

禁止任何 Markdown 或額外說明。"""

    for attempt in range(3):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL_GROUNDING,
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0,
                    max_output_tokens=512,
                ),
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
            elif attempt < 2:
                time.sleep(5)
    return {"ticker": ticker.upper(), "rating": 0, "theme": "", "feature": "", "reason": ""}


# ── 主流程 ────────────────────────────────────────────────────────────────
def main():
    gemini_key = os.environ.get("GEMINI_API_KEY")
    if not gemini_key:
        raise EnvironmentError("GEMINI_API_KEY 未設定")

    client = genai.Client(api_key=gemini_key)

    with open(INPUT_TXT, "r") as f:
        tickers = [line.strip().upper() for line in f if line.strip()]

    rs_map = {}
    if os.path.exists(RS_CSV):
        with open(RS_CSV, "r", newline="") as f:
            for row in csv.DictReader(f):
                t = row["ticker"].upper()
                rs_map[t] = row.get("RS", 0)

    rs95_tickers = load_rs95_liquid(RS_CSV, STOCK_DATA_CSV)
    industry_text, ticker_to_industry = load_industry_ranking(INDUSTRY_RS_CSV, TICKER_IND_CSV)

    print(f"📋 待分析：{len(tickers)} 檔 ／ RS>=95 參考股：{len(rs95_tickers)} 檔\n")

    print("📡 Phase 1：建立今日熱門題材...")
    hot_themes, hot_themes_list, rs_lookup = fetch_hot_themes(
        client, rs95_tickers, industry_text, ticker_to_industry
    )
    if not hot_themes:
        raise RuntimeError("Phase 1 失敗")

    os.makedirs("txt", exist_ok=True)
    with open(OUTPUT_THEMES, "w", encoding="utf-8") as f:
        f.write(f"# {TODAY}\n\n{hot_themes}\n")
    print(f"✅ 題材清單已存至 {OUTPUT_THEMES}\n")

    hot_wl_tickers = build_hot_theme_watchlist(hot_themes_list, rs_lookup)
    with open(OUTPUT_HOT_WL, "w", encoding="utf-8") as f:
        f.write("\n".join(hot_wl_tickers) + "\n")
    print(f"✅ 熱門題材 watchlist 已存至 {OUTPUT_HOT_WL}（{len(hot_wl_tickers)} 檔）\n")

    print(f"🔍 Phase 3：逐一評分（共 {len(tickers)} 檔）...\n")
    final_rows = []

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i:3d}/{len(tickers)}] {ticker}", end=" ... ", flush=True)
        result = score_ticker(client, ticker, hot_themes)
        final_rows.append({
            "ticker":  result.get("ticker", ticker),
            "RS":      rs_map.get(ticker, 0),
            "rating":  result.get("rating", 0),
            "theme":   result.get("theme", ""),
            "feature": result.get("feature", ""),
            "reason":  result.get("reason", ""),
        })
        print(f"rating={result.get('rating', '?')}")
        time.sleep(GROUNDING_SLEEP)

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
    print(f"   {OUTPUT_HOT_WL}")


if __name__ == "__main__":
    main()
