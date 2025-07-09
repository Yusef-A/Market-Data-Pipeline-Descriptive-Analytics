import argparse 
import os
from data_utils import (
    load_tickers, 
    get_ticker_data, 
    clean_data, 
    store_clean_paraquets, 
    align_data, 
    store_merged
)
def main(symbols_path, out_clean, out_merged, start, end): 
    symbols = load_tickers("symbols.txt")

    raw_dfs= [get_ticker_data(sym) for sym in symbols]
    clean_dfs= [clean_data(df) for df in raw_dfs]

    store_clean_paraquets(dict(zip(symbols,clean_dfs)))

    prices = align_data(clean_dfs,symbols)

    store_merged(prices, "data/prices.paraquet")
if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Ingest, clean, and store OHLCV for a universe of tickers"
    )
    p.add_argument("--symbols",    default="symbols.txt",
                   help="one-per-line ticker list")
    p.add_argument("--out-clean",  default="data/clean",
                   help="folder for per-symbol Parquets")
    p.add_argument("--out-merged", default="data/prices.parquet",
                   help="path for merged prices Parquet")
    p.add_argument("--start",      default="2015-01-01",
                   help="start date YYYY-MM-DD")
    p.add_argument("--end",        default=None,
                   help="end date YYYY-MM-DD (defaults to today)")
    args = p.parse_args()
    os.makedirs(os.path.dirname(args.out_merged) or ".", exist_ok=True)
    main(args.symbols, args.out_clean, args.out_merged, args.start, args.end)