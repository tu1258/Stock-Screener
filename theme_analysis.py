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
RS_CSV           = "stock_data_rs.csv"
OUTPUT_CSV       = "csv/watchlist_summary.csv"
OUTPUT_WATCHLIST = "txt/watchlist.txt"
OUTPUT_THEMES    = "txt/hot_themes.txt"
OUTPUT_HOT_WL    = "txt/hot_theme_watchlist.txt"
RATING_THRESHOLD = 6

GEMINI_MODEL_PHASE1 = "gemini-3-flash-preview"
GEMINI_MODEL_PHASE3 = "gemini-3.1-flash-lite-preview"

STOCK_DATA_CSV = "stock_data.csv"
MIN_AVG_VALUE_10M = 100

TODAY = datetime.date.today().strftime("%Y-%m-%d")


# ── 計算 RS95+ 且成交值 >= 100M 的清單 ──────────────────────────────────
def load_rs95_liquid(rs_csv: str, stock_data_csv: str) -> list[tuple]:
    """回傳 (ticker, rs_int) list，僅保留 RS>=95 且 10日均成交值 >= 100M"""
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


# ── Yahoo Finance 新聞抓取 ────────────────────────────────────────────────
def fetch_yahoo_news(ticker: str) -> list[str]:
    lines = []
    try:
        items = yf.Ticker(ticker).news or []
        for item in items:
            pub_dt = datetime.datetime.fromtimestamp(
                item.get("providerPublishTime", 0), tz=datetime.timezone.utc
            )
            date_str = pub_dt.strftime("%Y-%m-%d")
            title   = item.get("title", "")
            summary = item.get("summary", item.get("publisher", ""))
            if title:
                lines.append(f"[{date_str}] {title} — {summary}")
    except Exception:
        pass
    return lines


# ── Phase 1：建立今日熱門題材 ─────────────────────────────────────────────
def fetch_hot_themes(client: genai.Client, rs95_tickers: list[tuple]) -> tuple:
    print(f"  抓取 {len(rs95_tickers)} 檔 RS95+ 個股的 Yahoo Finance 新聞...")

    all_news_parts = []
    for i, (ticker, rs) in enumerate(rs95_tickers, 1):
        news_lines = fetch_yahoo_news(ticker)
        if news_lines:
            block = f"[{ticker} RS{rs}]\n" + "\n".join(news_lines)
            all_news_parts.append(block)
        if i % 50 == 0:
            time.sleep(1)

    all_news = "\n\n".join(all_news_parts) or "（無新聞）"
    ticker_lines = "\n".join(f"  RS{rs:>2}  {t}" for t, rs in rs95_tickers)

    prompt = f"""今天是 {TODAY}。你是一位資深美股分析師，任務是建立今日市場熱門題材清單。

【資料來源 A：RS>=95 且成交值>=100M 強勢股清單（共 {len(rs95_tickers)} 檔，依 RS 分數降序）】
{ticker_lines}

分析說明：
- RS 為相對強度分數，RS99 代表全市場前 1% 最強勢，RS95 代表前 5%
- 請依 RS 分數加權判斷題材熱度：RS越高權重越高
- 多檔 RS99 股票集中同一板塊，代表該題材資金極度集中，應列為頂級熱門
- 一檔個股可歸屬多個題材

【資料來源 B：RS95+ 個股 Yahoo Finance 新聞】
{all_news}

【輸出格式：嚴格回傳以下 JSON，禁止任何 Markdown 或額外說明】
{{
  "ticker_themes": {{
    "TICKER": ["題材A", "題材B"],
    ...
  }},
  "hot_themes": [
    "題材名稱 — 說明（含代表性個股RS分數）",
    ...
  ]
}}

要求：
- ticker_themes：對清單中每一檔股票標注所有相關題材（可多個）
- hot_themes：依加權熱度排序的前 8 大題材，每條附 1-2 句說明
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

            ticker_themes: dict = data.get("ticker_themes", {})
            hot_themes_list: list = data.get("hot_themes", [])

            hot_theme_names_ordered = [item.split("—")[0].strip() for item in hot_themes_list]

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
            lines.extend(hot_themes_list)

            themes_text = "\n".join(lines)

            return themes_text, ticker_themes, hot_theme_names_ordered, rs_lookup

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

    return "", {}, [], {}


# ── 產生 hot_theme_watchlist ──────────────────────────────────────────────
def build_hot_theme_watchlist(
    ticker_themes: dict,
    hot_theme_names_ordered: list[str],
    rs_lookup: dict[str, int],
) -> list[str]:
    """依熱門題材順序分組，同組照 RS 降序，每個 ticker 只出現一次。"""
    theme_rank = {name: i for i, name in enumerate(hot_theme_names_ordered)}

    ticker_best_theme: dict[str, str] = {}
    for ticker_raw, themes in ticker_themes.items():
        t = ticker_raw.upper()
        if not themes:
            continue
        best = min(
            (th for th in themes if th in theme_rank),
            key=lambda th: theme_rank[th],
            default=None,
        )
        if best:
            ticker_best_theme[t] = best

    groups: dict[str, list[str]] = {name: [] for name in hot_theme_names_ordered}
    for t, best_theme in ticker_best_theme.items():
        groups[best_theme].append(t)

    result = []
    seen = set()
    for theme_name in hot_theme_names_ordered:
        tickers_in_group = sorted(
            groups.get(theme_name, []),
            key=lambda t: -rs_lookup.get(t, 0),
        )
        for t in tickers_in_group:
            if t not in seen:
                seen.add(t)
                result.append(t)

    return result


# ── Phase 2：Yahoo Finance 個股新聞 ──────────────────────────────────────
def fetch_stock_news(ticker: str) -> str:
    lines = fetch_yahoo_news(ticker)
    return "\n".join(lines) or "（無最新新聞）"


# ── Phase 3：Gemini Flash-Lite 評分 ──────────────────────────────────────
def score_ticker(client: genai.Client, ticker: str, news_text: str, hot_themes: str) -> dict:
    prompt = f"""你是一位資深美股分析師。今天是 {TODAY}。

## 今日市場熱門題材
{hot_themes}

## 個股：{ticker}
以下是過去 7 天的最新新聞摘要（來源：Yahoo Finance）：
{news_text}

## 任務
判斷 {ticker} 與今日熱門題材的契合度，給予 1-10 評分。
評分只看題材契合度與炒作潛力，與個股 RS 分數完全無關。

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

    print(f"📋 待分析：{len(tickers)} 檔 ／ RS>=95 參考股：{len(rs95_tickers)} 檔\n")

    print("📡 Phase 1：建立今日熱門題材...")
    hot_themes, ticker_themes, hot_theme_names_ordered, rs_lookup = fetch_hot_themes(client, rs95_tickers)
    if not hot_themes:
        raise RuntimeError("Phase 1 失敗")

    os.makedirs("txt", exist_ok=True)
    with open(OUTPUT_THEMES, "w", encoding="utf-8") as f:
        f.write(f"# {TODAY}\n\n{hot_themes}\n")
    print(f"✅ 題材清單已存至 {OUTPUT_THEMES}\n")

    hot_wl_tickers = build_hot_theme_watchlist(ticker_themes, hot_theme_names_ordered, rs_lookup)
    with open(OUTPUT_HOT_WL, "w", encoding="utf-8") as f:
        f.write("\n".join(hot_wl_tickers) + "\n")
    print(f"✅ 熱門題材 watchlist 已存至 {OUTPUT_HOT_WL}（{len(hot_wl_tickers)} 檔）\n")

    print(f"🔍 Phase 2+3：逐一處理（共 {len(tickers)} 檔）...\n")
    final_rows = []

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i:3d}/{len(tickers)}] {ticker}", end=" ... ", flush=True)
        news_text = fetch_stock_news(ticker)
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
        time.sleep(4)

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
