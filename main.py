from src.rs import calculate_total_rs, calculate_rs_ranking, get_stock_data, get_spx_data
import pandas as pd

# 1️⃣ 股票清單，可以用 S&P500 成分股
tickers = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]['Symbol'].tolist()

# 2️⃣ 下載 SP500 收盤價
spx_close = get_spx_data()

# 3️⃣ 計算所有股票 total RS score
rs_scores = {}
for t in tickers:
    try:
        data = get_stock_data(t)
        rs_scores[t] = calculate_total_rs(data, spx_close)
    except Exception as e:
        print(f"{t} error:", e)

rs_scores = pd.Series(rs_scores)

# 4️⃣ 計算 RS Ranking
rs_ranking = calculate_rs_ranking(rs_scores)

# 5️⃣ 過濾 RS Ranking > 80
screener_result = rs_ranking[rs_ranking >= 80]

# 6️⃣ 輸出 CSV
screener_result.to_csv("rs_screener_result.csv", header=['RS Ranking'])
print(screener_result)
