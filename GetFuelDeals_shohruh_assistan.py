import json
import subprocess
import pandas as pd
import pyodbc
from datetime import datetime

# === CONFIGURATION ===
BEGINDATE = "2024-01-01"
ENDDATE = datetime.today().strftime("%Y-%m-%d")
API_URL = f"http://172.16.14.21:4041/GetFuelDeals/{BEGINDATE}/{ENDDATE}"
AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"
CSV_FILENAME = "GetFuelDeals.csv"

# === DATABASE CONNECTION ===
conn_str = (
    "DRIVER={SQL Server};"
    "SERVER=192.168.111.14;"
    "DATABASE=Birja;"
    "UID=sa;"
    "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
    "TrustServerCertificate=yes;"
)

REQUIRED_COLUMNS = [
    "deal_number", "deal_date", "deal_type", "contract_number", "seller_name",
    "seller_tin", "seller_region", "seller_district", "product_name", "deal_amount",
    "amount_unit", "deal_price", "deal_cost", "deal_currency", "buyer_tin",
    "buyer_name", "buyer_region", "register_id", "deal_url", "tnved"
]


def fetch_data_with_curl(api_url, auth_token):
    try:
        command = f'curl -X GET "{api_url}" -H "accept: text/plain" -H "Authorization: {auth_token}"'
        process = subprocess.run(command, shell=True, capture_output=True, text=True, encoding="utf-8")
        if process.returncode == 0:
            return process.stdout
        else:
            print(f"API error: {process.stderr}")
            return None
    except Exception as e:
        print(f"API error: {e}")
        return None


def save_to_csv(data, filename):
    if data:
        try:
            parsed = json.loads(data)
            if isinstance(parsed, list):
                df = pd.DataFrame(parsed)
                df.to_csv(filename, index=False, encoding="utf-8-sig")
                print(f"CSV saved: {filename}")
                return df
            else:
                print("JSON format is incorrect.")
                return None
        except json.JSONDecodeError:
            print("JSON decode error.")
            return None
    else:
        print("No data received from API.")
        return None


def prepare_staging_table(cursor):
    cursor.execute("""
    IF OBJECT_ID('dbo.FuelDeals_Staging', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.FuelDeals_Staging (
            deal_number INT,
            deal_date DATETIME,
            deal_type INT,
            contract_number NVARCHAR(500),
            seller_name NVARCHAR(500),
            seller_tin NVARCHAR(500),
            seller_region NVARCHAR(500),
            seller_district NVARCHAR(500),
            product_name NVARCHAR(500),
            deal_amount FLOAT,
            amount_unit NVARCHAR(500),
            deal_price FLOAT,
            deal_cost FLOAT,
            deal_currency INT,
            buyer_tin NVARCHAR(500),
            buyer_name NVARCHAR(500),
            buyer_region NVARCHAR(500),
            register_id INT,
            deal_url NVARCHAR(500),
            tnved NVARCHAR(500)
        );
    END
    ELSE
        TRUNCATE TABLE dbo.FuelDeals_Staging;
    """)


def prepare_test_table(cursor):
    cursor.execute("""
    IF OBJECT_ID('dbo.test_FuelDeals', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.test_FuelDeals (
            deal_number INT,
            deal_date DATETIME,
            deal_type INT,
            contract_number NVARCHAR(500),
            seller_name NVARCHAR(500),
            seller_tin NVARCHAR(500),
            seller_region NVARCHAR(500),
            seller_district NVARCHAR(500),
            product_name NVARCHAR(500),
            deal_amount FLOAT,
            amount_unit NVARCHAR(500),
            deal_price FLOAT,
            deal_cost FLOAT,
            deal_currency INT,
            buyer_tin NVARCHAR(500),
            buyer_name NVARCHAR(500),
            buyer_region NVARCHAR(500),
            register_id INT,
            deal_url NVARCHAR(500),
            tnved NVARCHAR(500)
        );
    END
    """)


def insert_into_staging(cursor, df):
    df = df.fillna("")
    df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")

    # Ensure required columns are present
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    df = df[REQUIRED_COLUMNS]

    numeric_cols = [
        "deal_number", "deal_type", "deal_amount", "deal_price",
        "deal_cost", "deal_currency", "register_id"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    print(f"Inserting {len(df)} rows with columns: {df.columns.tolist()}")

    records = df.values.tolist()
    insert_sql = f"""
        INSERT INTO dbo.FuelDeals_Staging (
            {', '.join(REQUIRED_COLUMNS)}
        ) VALUES ({', '.join(['?'] * len(REQUIRED_COLUMNS))})
    """
    cursor.fast_executemany = True
    cursor.executemany(insert_sql, records)


# merge_to_main_table stays unchanged

def merge_to_main_table(cursor):
    merge_sql = """
    MERGE dbo.test_FuelDeals WITH (HOLDLOCK) AS Target
    USING dbo.FuelDeals_Staging AS Src
    ON Target.deal_number = Src.deal_number AND Target.deal_date = Src.deal_date
    WHEN MATCHED THEN
        UPDATE SET
            Target.deal_type = Src.deal_type,
            Target.contract_number = Src.contract_number,
            Target.seller_name = Src.seller_name,
            Target.seller_tin = Src.seller_tin,
            Target.seller_region = Src.seller_region,
            Target.seller_district = Src.seller_district,
            Target.product_name = Src.product_name,
            Target.deal_amount = Src.deal_amount,
            Target.amount_unit = Src.amount_unit,
            Target.deal_price = Src.deal_price,
            Target.deal_cost = Src.deal_cost,
            Target.deal_currency = Src.deal_currency,
            Target.buyer_tin = Src.buyer_tin,
            Target.buyer_name = Src.buyer_name,
            Target.buyer_region = Src.buyer_region,
            Target.register_id = Src.register_id,
            Target.deal_url = Src.deal_url,
            Target.tnved = Src.tnved
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            deal_number, deal_date, deal_type, contract_number, seller_name,
            seller_tin, seller_region, seller_district, product_name, deal_amount,
            amount_unit, deal_price, deal_cost, deal_currency, buyer_tin,
            buyer_name, buyer_region, register_id, deal_url, tnved
        ) VALUES (
            Src.deal_number, Src.deal_date, Src.deal_type, Src.contract_number, Src.seller_name,
            Src.seller_tin, Src.seller_region, Src.seller_district, Src.product_name, Src.deal_amount,
            Src.amount_unit, Src.deal_price, Src.deal_cost, Src.deal_currency, Src.buyer_tin,
            Src.buyer_name, Src.buyer_region, Src.register_id, Src.deal_url, Src.tnved
        );
    """
    cursor.execute(merge_sql)


if __name__ == "__main__":
    raw_data = fetch_data_with_curl(API_URL, AUTH_TOKEN)
    df = save_to_csv(raw_data, CSV_FILENAME)

    if df is not None and not df.empty:
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            prepare_staging_table(cursor)
            prepare_test_table(cursor)
            insert_into_staging(cursor, df)
            conn.commit()

            merge_to_main_table(cursor)
            conn.commit()

            print("FuelDeals loaded into test_FuelDeals safely.")
        except Exception as e:
            print(f"Error occurred: {e}")
        finally:
            cursor.close()
            conn.close()
    else:
        print("Data is empty or CSV issue occurred.")
