import pandas as pd

RS_CSV = "stock_data_rs.csv"
OUTPUT_CSV = "watchlist.csv"

def main():
    # 讀取 RS CSV
    rs_df = pd.read_csv(RS_CSV)

    # 篩選 RS rank > 90
    rs_filtered = rs_df[rs_df["RS"] > 90].copy()

    # 輸出結果
    rs_filtered.to_csv(OUTPUT_CSV, index=False)


if __name__ == "__main__":
    main()
