
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'nifty_options_data',
    'user': 'prana',
}

CLICKHOUSE_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'username': 'default',
    'password': '',
    'database': 'default',
}

import datetime

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30), name='IST')

def load_risk_free_rate(date_str=None, use_open=True):
    """
    Load risk-free rate from rate.csv for a specific date (YYYY-MM-DD).
    If no date is provided, defaults to today.
    Uses 'Open' column if use_open is True, otherwise 'Price' (Close).
    Returns rate as a decimal (e.g., 0.0696 for 6.96%).
    """
    import pandas as pd
    from pathlib import Path
    
    csv_path = Path(__file__).parent / "rate.csv"
    if not csv_path.exists():
        return 0.07
        
    try:
        df = pd.read_csv(csv_path)
        df['ParsedDate'] = pd.to_datetime(df['Date'], format='%d-%m-%Y').dt.date
        df = df.sort_values('ParsedDate')
        
        if date_str is None:
            target_date = datetime.date.today()
        else:
            if isinstance(date_str, str):
                target_date = pd.to_datetime(date_str).date()
            elif isinstance(date_str, datetime.datetime):
                target_date = date_str.date()
            else:
                target_date = date_str
                
        # Find exact match or the closest previous date
        match = df[df['ParsedDate'] <= target_date]
        if match.empty:
            row = df.iloc[0]
        else:
            row = match.iloc[-1]
            
        col = 'Open' if use_open else 'Price'
        rate_val = float(row[col])
        return rate_val / 100.0
    except Exception:
        return 0.07

