# src/extract.py
import pandas as pd
from pathlib import Path

# bu dosyanın konumundan data/raw'a git
RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

def extract_orders():
    orders_path = RAW_DIR / "data.csv"   # sende bu vardı
    # encoding ekledik, gerekirse sep de ekleriz
    df_orders = pd.read_csv(orders_path, encoding="latin1")
    return df_orders

def extract_customers():
    customers_path = RAW_DIR / "customers.csv"
    if customers_path.exists():
        return pd.read_csv(customers_path, encoding="latin1")
    else:
        # şimdilik boş dön
        return pd.DataFrame()
