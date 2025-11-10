# run_etl.py
import src.extract as extract
import src.transform as transform
import src.load as load

def main():
    orders_df = extract.extract_orders()
    customers_df = extract.extract_customers()

    orders_clean = transform.clean_orders(orders_df)
    customers_clean = transform.clean_customers(customers_df)

    # orders her zaman yazılsın
    load.load_orders(orders_clean)

    # customers boş değilse yaz
    if not customers_clean.empty:
        load.load_customers(customers_clean)
    else:
        print("customers boş geldi, tablo oluşturulmadı.")

if __name__ == "__main__":
    main()
