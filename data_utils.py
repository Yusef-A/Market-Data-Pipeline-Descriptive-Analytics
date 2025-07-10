import time
import logging  
from joblib import Memory 
import yfinance as yf 
import pandas as pd 
import os
from typing import List, Dict, Optional


logging.basicConfig(
    filename="data_utils.long",
    level=logging.INFO,
    format= "%(asctime)s %(levelname)s: %(message)s"
)
cache = Memory(".cache")

def load_tickers(path: str = "symbols.txt") ->list: 
    with open(path, "r") as f: 
        return [line.strip() for line in f if line.strip()] # return a list of cleaned tickers
    
@cache.cache 

def get_ticker_data(
    ticker: str, 
    start: str = "2015-01-01",
    end: str | None = None
)-> pd.DataFrame: 
    if end is None: 
        end = pd.Timestamp.today().strftime("%Y-%m-%d")
    for attempt in range(6): 
        try: 
            df = yf.download(
                ticker, 
                start = start, 
                end = end, 
                auto_adjust=True, # gives prices correctd to corporate actions whatever that means.. 
                progress=False #download bar
            )
            df.index = pd.to_datetime(df.index)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.droplevel(1)
            df.columns = df.columns.str.lower().str.replace(" ","_")
            if "adj_close" in df.columns:
                df = df.rename(columns={"adj_close": "close"})
            return df
        except Exception as e: 
            if attempt == 5:
                logging.error(f"Failed to download {ticker} after {attempt} attempts: {e}")
            time.sleep(2**(attempt -1))
    empty = pd.DataFrame()
    empty.index = pd.DatetimeIndex([]) 
    return empty
def clean_data(
        df: pd.DataFrame,
        max_gap : int = 2,
        anomaly_thresh: float = 0.2
) -> pd.DataFrame: 
    df = df.asfreq("B").ffill(limit=max_gap)
    
    jumps = df["close"].pct_change().abs() #calcs % delta in the close price from one rw to the next
    for date, pct in jumps[jumps > anomaly_thresh].items():
        logging.warning(f"Anomaly on {date.date()}: {pct:.1%} jump in Close") #recording big moves in market
    
    nan_dates = df[df["close"].isna()].index
    for date in nan_dates: 
        logging.info(f"Unfilled gap (> {max_gap} days) at {date.date()}") 
    return df
def align_data(dfs: list[pd.DataFrame], symbols: list[str]) -> pd.DataFrame: 
    map = {
        sym: df["close"]
        for sym, df in zip(symbols, dfs)
        }
    return pd.concat(map, axis=1, join="inner")
def store_clean_parquets(
        dfs: dict[str, pd.DataFrame],
        outdir: str = "data/clean"
)->None: 
    os.makedirs(outdir, exist_ok=True)
    for sym,df in dfs.items():
        fname = f"{sym.lower()}.parquet"
        tmp = os.path.join(outdir,fname +".tmp")
        final = os.path.join(outdir,fname)
        df.to_parquet(tmp)
        os.replace(tmp,final)
def store_merged(df, out_path):
    """
    Save the merged price‐table DataFrame to a Parquet file.
    """
    # make sure the directory exists
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_parquet(out_path)