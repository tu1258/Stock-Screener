import yfinance as yf

spx = yf.download("^GSPC", period="5d", interval="1d")

last_close = spx["Close"].iloc[-1]

# 如果是 Series，就取第一個值
if hasattr(last_close, "iloc"):
    last_close = last_close.iloc[0]

print("SP500 last close:", float(last_close))
