# src/transform.py
import pandas as pd

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    # kolonları küçült
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # tarih: "invoicedate"
    df["invoicedate"] = pd.to_datetime(df["invoicedate"], errors="coerce", dayfirst=True)

    # customerid boşları at
    df = df.dropna(subset=["customerid"])

    # revenue = quantity * unitprice
    df["revenue"] = df["quantity"] * df["unitprice"]

    # yıl / ay kolonları
    df["invoice_year"] = df["invoicedate"].dt.year
    df["invoice_month"] = df["invoicedate"].dt.month

    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


def build_monthly_country_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    yıl + ay + ülke bazında revenue ve sipariş sayısı
    """
    if df.empty:
        return df

    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    grouped = (
        df.groupby(["invoice_year", "invoice_month", "country"], dropna=True)
          .agg(
              total_revenue=("revenue", "sum"),
              order_count=("invoiceno", "nunique")
          )
          .reset_index()
    )
    return grouped
