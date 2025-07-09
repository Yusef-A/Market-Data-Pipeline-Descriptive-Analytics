import pandas as pd
from data_utils import load_tickers, clean_data

def test_load_tickers(tmp_path):
    # Create a temporary symbols.txt with sample tickers and blank lines
    p = tmp_path / "symbols.txt"
    p.write_text("AAPL\nGOOG\n\nMSFT\n")
    result = load_tickers(str(p))
    assert result == ["AAPL", "GOOG", "MSFT"]

def test_clean_data_forward_fill():
    # Build a small DataFrame with a 2-day gap
    idx = pd.bdate_range("2021-01-01", periods=4)  # business days
    df  = pd.DataFrame({"close": [1, None, None, 4]}, index=idx)
    out = clean_data(df, max_gap=2)
    # After forward-filling up to 2 days, there should be no NaNs
    assert out["close"].isna().sum() == 0
