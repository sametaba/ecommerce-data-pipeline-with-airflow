# E-commerce Data Engineering Pipeline

Bu projede Kaggle’daki Online Retail veri setini alıp Python ile işliyoruz, Docker içinde çalışan PostgreSQL’e yüklüyoruz ve aynı adımları Apache Airflow ile orkestre edilebilir hale getiriyoruz. 

---

## 1. Mimarinin Özeti

```text
Kaggle CSV (data/raw/data.csv)
           |
           v
Python ETL (src/extract.py, src/transform.py, src/load.py)
           |
           v
Postgres (Docker, tablo: stg_orders)
           |
           v
Analytics tablo (fact_monthly_country_sales)
           |
           v
Airflow DAG (dags/ecommerce_etl_dag.py)


Kurulum:
python -m venv venv
venv\Scripts\activate
pip install pandas sqlalchemy psycopg2-binary

Docker Servisleri: docker compose up -d

Airflow Arayüzü: http://localhost:8080
kullanıcı: admin
şifre: admin