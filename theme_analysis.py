def fetch_hot_themes(client: genai.Client, rs95_tickers: list[tuple],
                     industry_text: str, ticker_to_industry: dict) -> tuple:
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

    ticker_ind_lines = "\n".join(
        f"  {t}：{ticker_to_industry.get(t, 'N/A')}"
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
【資料來源 D：RS95+ 個股 Yahoo Finance 新聞（用於判斷近期催化劑與市場關注度）】
{all_news}

分析說明：
- RS 為相對強度分數，RS99 代表全市場前 1% 最強勢，RS95 代表前 5%
- 請依 RS 分數加權判斷題材熱度：RS越高權重越高
- 多檔 RS99 股票集中同一板塊，代表該題材資金極度集中，應列為頂級熱門
- 一檔個股可歸屬多個題材
- 公司業務分類請優先使用你對各公司的專業知識，資料來源 B 的 industry 分類較粗糙僅供輔助參考
- 新聞資料用於判斷近期是否有催化劑，不作為公司業務分類的依據
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
