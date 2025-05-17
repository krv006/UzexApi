import json
import subprocess
import pandas as pd
import pyodbc
from datetime import datetime


def fetch_data_with_curl(api_url, auth_token):
    try:
        command = f'curl -X GET "{api_url}" -H "accept: text/plain" -H "Authorization: {auth_token}"'
        process = subprocess.run(command, shell=True, capture_output=True, text=True, encoding="utf-8")
        if process.returncode == 0:
            return process.stdout
        else:
            print(f"❌ API xatolik: {process.stderr}")
            return None
    except Exception as e:
        print(f"❌ API xatolik: {e}")
        return None


def save_to_csv(data, filename="GetSpotCottonDeals.csv"):
    if data:
        try:
            if isinstance(data, str):
                data = json.loads(data)
            if isinstance(data, list):
                df = pd.DataFrame(data)
                df.to_csv(filename, index=False, encoding="utf-8-sig")
                print(f"📁 CSV saqlandi: {filename}")
                return df
            else:
                print("❌ JSON noto‘g‘ri formatda.")
                return None
        except json.JSONDecodeError:
            print("❌ JSON decode error.")
            return None
    else:
        print("🚫 API'dan hech qanday ma'lumot kelmadi.")
        return None


def insert_spot_deals_to_db(df, batch_size=500):
    try:
        conn = pyodbc.connect(
            "DRIVER={SQL Server};"
            "SERVER=192.168.111.14;"
            "DATABASE=Birja;"
            "UID=sa;"
            "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
            "TrustServerCertificate=yes;"
        )
        cursor = conn.cursor()

        # CREATE TABLE IF NOT EXISTS
        create_query = """
        IF OBJECT_ID('dbo.SpotCottonDeals', 'U') IS NULL
        CREATE TABLE dbo.SpotCottonDeals (
            deal_number INT,
            deal_date DATETIME,
            contract_number NVARCHAR(500),
            seller_name NVARCHAR(500),
            seller_tin NVARCHAR(500),
            seller_region INT,
            seller_district INT,
            product_name NVARCHAR(500),
            deal_amount FLOAT,
            deal_price FLOAT,
            deal_cost FLOAT,
            deal_currency INT,
            buyer_name NVARCHAR(500),
            buyer_tin NVARCHAR(500),
            buyer_region INT,
            buyer_district INT
        )
        """
        cursor.execute(create_query)
        conn.commit()

        df = df.fillna("")
        df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")

        int_cols = ["deal_number", "seller_region", "seller_district", "deal_currency", "buyer_region", "buyer_district"]
        float_cols = ["deal_amount", "deal_price", "deal_cost"]
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        records = df.values.tolist()
        cursor.fast_executemany = True

        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                cursor.executemany("""
                    INSERT INTO dbo.SpotCottonDeals (
                        deal_number, deal_date, contract_number, seller_name, seller_tin,
                        seller_region, seller_district, product_name, deal_amount, deal_price,
                        deal_cost, deal_currency, buyer_name, buyer_tin, buyer_region, buyer_district
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                conn.commit()
                total += len(batch)
                print(f"📥 {total} qator qo‘shildi...")
            except Exception as batch_error:
                print(f"⚠️ Batch skip qilindi: {batch_error}")

        print(f"✅ Jami {total} qator SpotCottonDeals ga yozildi.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ SpotCottonDeals DB xatolik: {e}")


if __name__ == "__main__":
    BEGINDATE = "2024-01-01"
    ENDDATE = datetime.today().strftime("%Y-%m-%d")
    API_URL = f"http://172.16.14.21:4041/GetSpotCottonDeals/{BEGINDATE}/{ENDDATE}"
    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    data = fetch_data_with_curl(API_URL, AUTH_TOKEN)
    df = save_to_csv(data)

    if df is not None and not df.empty:
        insert_spot_deals_to_db(df)
    else:
        print("⚠️ Ma’lumot bo‘sh yoki CSV xatolik.")
