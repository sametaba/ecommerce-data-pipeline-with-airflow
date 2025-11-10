# src/load.py
from sqlalchemy import create_engine

# docker'daki postgres'e bağlan
DB_USER = "deuser"
DB_PASS = "depass"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce"

engine_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(engine_str)

def load_orders(df):
    df.to_sql("stg_orders", engine, if_exists="replace", index=False)
    print("stg_orders tablosu Postgres'e yazıldı.")

def load_customers(df):
    df.to_sql("stg_customers", engine, if_exists="replace", index=False)
    print("stg_customers tablosu Postgres'e yazıldı.")

def load_monthly_country_sales(df):
    # airflow ilk çalıştığında df boş olabilir, o durumda yazmayalım
    if df is None or df.empty:
        print("fact_monthly_country_sales için veri yok, tablo yazılmadı.")
        return
    df.to_sql("fact_monthly_country_sales", engine, if_exists="replace", index=False)
    print("fact_monthly_country_sales tablosu Postgres'e yazıldı.")
