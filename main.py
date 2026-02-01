import pandas as pd
import os
import sys
sys.path.append("src")
from rs import get_stock_data, get_spx_data, calculate_total_rs, calculate_rs_ranking

# 建立 output 資料夾
if not os.path.exists("output"):
    os.makedirs("output")

# 股票清單
tickers = ["AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN", "NFLX", "SPOT", "CRWD", "CRWV", "SNDK", "MU", "FCX"]

# 抓 S&P 500 指數
spx_data = get_spx_data()

rs_list = []
for ticker in tickers:
    data = get_stock_data(ticker)
    print(ticker, len(data))  # 印出每檔資料長度
    if len(data) >= 30:  # 至少30天才算
        rs_score = calculate_total_rs(data, spx_data)
        rs_list.append((ticker, rs_score))
    else:
        print(f"{ticker} 天數不足，跳過")

# 如果沒股票符合，也生成空的 CSV 避免 workflow fail
if rs_list:
    df = pd.DataFrame(rs_list, columns=["Ticker", "RS"])
    df["RS_Rank"] = calculate_rs_ranking(df["RS"])
else:
    df = pd.DataFrame(columns=["Ticker", "RS", "RS_Rank"])

# 存檔
df.to_csv("output/rs_stocks.csv", index=False)
print(df)
