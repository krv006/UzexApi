import json
import subprocess
from datetime import datetime

import pandas as pd
import pyodbc


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


def save_to_csv(data, filename="GetCottonDeals.csv"):
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


def insert_cotton_deals_to_db(df, batch_size=500):
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

        create_query = """
        IF OBJECT_ID('dbo.CottonDeals', 'U') IS NULL
        CREATE TABLE dbo.CottonDeals (
            deal_number INT PRIMARY KEY,
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
            deal_url NVARCHAR(500)
        )
        """
        cursor.execute(create_query)
        conn.commit()

        df = df.fillna("")
        df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")

        numeric_cols = [
            "deal_number", "deal_type", "deal_amount", "deal_price",
            "deal_cost", "deal_currency", "register_id"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        df["deal_number"] = df["deal_number"].astype(int)

        df = df.drop_duplicates(subset=['deal_number'])

        existing_deals = pd.read_sql("SELECT deal_number FROM dbo.CottonDeals", conn)
        existing_deals_set = set(existing_deals["deal_number"].astype(int))

        df = df[~df["deal_number"].isin(existing_deals_set)]

        if df.empty:
            print("📭 Yangi yoziladigan deal_number yo‘q, barcha ma’lumotlar DBda mavjud.")
            cursor.close()
            conn.close()
            return

        records = df.values.tolist()
        cursor.fast_executemany = True

        total_inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                cursor.executemany("""
                    INSERT INTO dbo.CottonDeals (
                        deal_number, deal_date, deal_type, contract_number, seller_name,
                        seller_tin, seller_region, seller_district, product_name, deal_amount,
                        amount_unit, deal_price, deal_cost, deal_currency, buyer_tin,
                        buyer_name, buyer_region, register_id, deal_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                conn.commit()
                total_inserted += len(batch)
                print(f"📥 {total_inserted} qator qo‘shildi...")
            except Exception as batch_error:
                print(f"⚠️ Batch skip qilindi: {batch_error}")

        print(f"✅ Jami {total_inserted} qator CottonDeals ga yozildi.")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ CottonDeals DB xatolik: {e}")


if __name__ == "__main__":
    BEGINDATE = "2024-01-01"
    ENDDATE = datetime.today().strftime("%Y-%m-%d")
    API_URL = f"http://172.16.14.21:4041/GetCottonDeals/{BEGINDATE}/{ENDDATE}"
    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    data = fetch_data_with_curl(API_URL, AUTH_TOKEN)
    df = save_to_csv(data)

    if df is not None and not df.empty:
        insert_cotton_deals_to_db(df)
    else:
        print("⚠️ Ma’lumot bo‘sh yoki CSV xatolik.")
