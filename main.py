import pandas as pd
import yfinance as yf
import sys
sys.path.append("src")
from rs import get_stock_data, get_spx_data, calculate_total_rs, calculate_rs_ranking

# 先固定股票清單
tickers = ["AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN", 
           "NFLX", "SPOT", "CRWD", "CRWV", "SNDK", "MU", "FCX"]

# 取得 SPX 收盤價
spx_close = get_spx_data()

# 記錄每檔股票的 RS score
rs_scores = {}


# 2. 計算每檔股票的 RS score
for ticker in tickers:
    data = yf.download(ticker, period="400d", interval="1d", auto_adjust=True)['Close']
    if len(data) < 30:
        print(f"{ticker} 資料少於30天，跳過")
        continue
    rs = calculate_total_rs(data, spx_close)
    rs_scores[ticker] = rs

# 3. 轉成 DataFrame
df = pd.DataFrame.from_dict(rs_scores, orient='index', columns=['RS'])
df.index.name = 'Ticker'

# 4. 計算 RS Ranking（百分位 1~99）
df['RS_Rank'] = pd.qcut(df['RS'], 100, labels=False, duplicates='drop')
df['RS_Rank'] = df['RS_Rank'] + 1  # qcut labels 從0開始，改成1~99

# 5. 排序
df = df.sort_values('RS', ascending=False)

print(df)
# 可存檔
df.to_csv("output/rs_ranking.csv", index=False)
