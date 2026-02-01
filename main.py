import pandas as pd
from rs import get_stock_data, get_spx_data, calculate_total_rs, calculate_rs_ranking

# 先固定股票清單
tickers = ["AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN", 
           "NFLX", "SPOT", "CRWD", "CRWV", "SNDK", "MU", "FCX"]

# 取得 SPX 收盤價
spx_close = get_spx_data()

# 記錄每檔股票的 RS score
rs_scores = {}

for ticker in tickers:
    print(f"Downloading {ticker} data...")
    close_prices = get_stock_data(ticker)
    if len(close_prices) < 252:  # 確保有足夠資料
        print(f"{ticker} 資料不足，跳過")
        continue
    rs_score = calculate_total_rs(close_prices, spx_close)
    rs_scores[ticker] = rs_score

# 將 RS score 轉為 Series
rs_series = pd.Series(rs_scores)

# 計算 RS ranking
rs_ranking = calculate_rs_ranking(rs_series)

# 組成 dataframe
df = pd.DataFrame({
    "Ticker": rs_series.index,
    "RS_Score": rs_series.values,
    "RS_Rank": rs_ranking.values
})

# 排序、顯示 top
df = df.sort_values("RS_Rank", ascending=False)
print(df)

# 可存檔
df.to_csv("output/rs_ranking.csv", index=False)
