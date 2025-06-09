import json
import os
import subprocess
from datetime import datetime, timedelta

import pandas as pd
import pyodbc

CSV_FILENAME = "GetMinUdobDeals.csv"


def fetch_data_with_curl(api_url, auth_token):
    print(f"🌐 So‘rov yuborilmoqda: {api_url}")
    try:
        command = f'curl -X GET "{api_url}" -H "accept: text/plain" -H "Authorization: {auth_token}"'
        process = subprocess.run(command, shell=True, capture_output=True, text=True, encoding="utf-8")
        if process.returncode == 0:
            print("✅ API dan javob olindi.")
            return process.stdout
        else:
            print(f"❌ API xatolik: {process.stderr}")
            return None
    except Exception as e:
        print(f"❌ API xatolik: {e}")
        return None


def save_to_csv(df):
    if df is not None and not df.empty:
        file_exists = os.path.isfile(CSV_FILENAME)
        df.to_csv(CSV_FILENAME, mode='a', index=False, encoding="utf-8-sig", header=not file_exists)
        print(f"📁 Ma'lumotlar CSV faylga saqlandi: {CSV_FILENAME}")
    else:
        print("⚠️ Saqlash uchun ma'lumotlar mavjud emas.")


def save_to_dataframe(data):
    if data:
        try:
            if isinstance(data, str):
                data = json.loads(data)
            if isinstance(data, list):
                df = pd.DataFrame(data)
                print(f"📊 Ma'lumotlar soni: {len(df)} qator")
                return df
            else:
                print("❌ JSON noto‘g‘ri format.")
                return None
        except json.JSONDecodeError:
            print("❌ JSON decode error.")
            return None
    else:
        print("🚫 Ma’lumot yo‘q.")
        return None


def insert_to_db(df, batch_size=500):
    print("🗃️ SQL Serverga yozish jarayoni boshlandi...")
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
        IF OBJECT_ID('dbo.GetMinUdobDealsFull', 'U') IS NULL
        CREATE TABLE dbo.GetMinUdobDealsFull (
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
            deal_url NVARCHAR(500),
            amount FLOAT,
            startingpricefrombill FLOAT,
            productamountbycoefficient FLOAT,
            segmentgruppa NVARCHAR(500),
            productunit NVARCHAR(500),
            productgroup NVARCHAR(500),
            productsubgroup NVARCHAR(500),
            statline NVARCHAR(500),
            tnved NVARCHAR(500)
        )
        """
        cursor.execute(create_query)
        conn.commit()

        existing_ids = pd.read_sql("SELECT deal_number FROM dbo.GetMinUdobDealsFull", conn)
        df = df[~df["deal_number"].isin(existing_ids["deal_number"])]

        if df.empty:
            print("📭 Yangi yoziladigan qator yo‘q.")
            cursor.close()
            conn.close()
            return

        df = df.fillna("")
        df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")

        numeric_cols = [
            "deal_number", "deal_type", "deal_amount", "deal_price", "deal_cost",
            "deal_currency", "register_id", "amount", "startingpricefrombill", "productamountbycoefficient"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        expected_columns = [
            'deal_number', 'deal_date', 'deal_type', 'contract_number', 'seller_name',
            'seller_tin', 'seller_region', 'seller_district', 'product_name', 'deal_amount',
            'amount_unit', 'deal_price', 'deal_cost', 'deal_currency', 'buyer_tin',
            'buyer_name', 'buyer_region', 'register_id', 'deal_url', 'amount',
            'startingpricefrombill', 'productamountbycoefficient', 'segmentgruppa',
            'productunit', 'productgroup', 'productsubgroup', 'statline', 'tnved'
        ]
        for col in expected_columns:
            if col not in df.columns:
                df[col] = ""

        records = df[expected_columns].values.tolist()
        cursor.fast_executemany = True

        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany(f"""
                INSERT INTO dbo.GetMinUdobDealsFull (
                    {', '.join(expected_columns)}
                ) VALUES ({', '.join(['?'] * len(expected_columns))})
            """, batch)
            conn.commit()
            total += len(batch)

        print(f"✅ {total} ta yangi qator DB ga yozildi.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ DB xatolik: {e}")


def month_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        start = current.replace(day=1)
        if current.month == 12:
            end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end = current.replace(month=current.month + 1, day=1) - timedelta(days=1)
        yield start, end
        current = end + timedelta(days=1)


if __name__ == "__main__":
    print("🚀 Oylik yuklash jarayoni boshlandi...")

    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"
    start_date = datetime(2024, 1, 1)
    end_date = datetime.today()

    for beg, end in month_range(start_date, end_date):
        beg_str = beg.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        print(f"\n📅 Ma'lumot yuklanmoqda: {beg_str} → {end_str}")

        API_URL = f"http://172.16.14.21:4041/GetMinUdobDeals/{beg_str}/{end_str}"
        data = fetch_data_with_curl(API_URL, AUTH_TOKEN)
        df = save_to_dataframe(data)

        if df is not None and not df.empty:
            save_to_csv(df)
            insert_to_db(df)
        else:
            print("⚠️ Bu oyda hech qanday qator topilmadi.")
