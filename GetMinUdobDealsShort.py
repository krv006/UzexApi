from datetime import datetime, timedelta

import pandas as pd
import pyodbc
import requests

AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"
CSV_FILENAME = "GetMinUdobDealsShort.csv"


def fetch_data(api_url, auth_token):
    headers = {
        "accept": "application/json",
        "Authorization": auth_token
    }
    try:
        response = requests.get(api_url, headers=headers, timeout=180)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ API xatolik: {e}")
        return None


def save_to_csv(data):
    if data:
        try:
            if isinstance(data, dict):
                data = [data]
            df = pd.DataFrame(data)
            df.to_csv(CSV_FILENAME, index=False, encoding="utf-8-sig")
            print(f"📁 CSV saqlandi: {CSV_FILENAME}")
            return df
        except Exception as e:
            print(f"❌ CSV yozishda xatolik: {e}")
            return None
    else:
        print("🚫 Ma’lumot bo‘sh.")
        return None


def insert_udob_deals_to_db(df, batch_size=500):
    try:
        conn = pyodbc.connect(
            "DRIVER={SQL Server};"
            "SERVER=192.168.111.14;"
            "DATABASE=Birja;"
            "UID=sa;"
            "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
            "TrustServerCertificate=yes;",
            autocommit=True
        )
        cursor = conn.cursor()

        # Jadvalni yaratish (agar yo'q bo'lsa)
        create_query = """
        IF OBJECT_ID('dbo.GetMinUdobDealsShort', 'U') IS NULL
        CREATE TABLE dbo.GetMinUdobDealsShort (
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
            tnved NVARCHAR(500)
        )
        """
        cursor.execute(create_query)

        # Ma'lumotni tayyorlash
        df = df.fillna("")
        df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")

        numeric_cols = [
            "deal_number", "deal_type", "deal_amount", "deal_price", "deal_cost",
            "deal_currency", "register_id"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "tnved" not in df.columns:
            df["tnved"] = ""

        # Eskilarni olib tashlash
        existing_ids = pd.read_sql("SELECT deal_number FROM dbo.GetMinUdobDealsShort", conn)
        df = df[~df["deal_number"].isin(existing_ids["deal_number"])]
        if df.empty:
            print("📭 Yangi yoziladigan qator yo‘q.")
            return

        # Yozish
        records = df[[
            "deal_number", "deal_date", "deal_type", "contract_number", "seller_name",
            "seller_tin", "seller_region", "seller_district", "product_name", "deal_amount",
            "amount_unit", "deal_price", "deal_cost", "deal_currency", "buyer_tin",
            "buyer_name", "buyer_region", "register_id", "deal_url", "tnved"
        ]].values.tolist()

        cursor.fast_executemany = True
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany("""
                INSERT INTO dbo.GetMinUdobDealsShort (
                    deal_number, deal_date, deal_type, contract_number, seller_name,
                    seller_tin, seller_region, seller_district, product_name, deal_amount,
                    amount_unit, deal_price, deal_cost, deal_currency, buyer_tin,
                    buyer_name, buyer_region, register_id, deal_url, tnved
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            total += len(batch)
            print(f"📥 {total} qator yozildi...")

        cursor.close()
        conn.close()
        print(f"✅ Jami {total} qator DB ga qo‘shildi.")
    except Exception as e:
        print(f"❌ DB xatolik: {e}")


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


if __name__ == "__main__":
    BEGINDATE = datetime.strptime("2024-01-01", "%Y-%m-%d")
    ENDDATE = datetime.today()

    all_data = []
    for single_date in daterange(BEGINDATE, ENDDATE):
        start_str = single_date.strftime("%Y-%m-%d")
        api_url = f"http://172.16.14.21:4041/GetMinUdobDealsShort/{start_str}/{start_str}"
        print(f"🔄 {start_str} dan ma’lumot olinmoqda...")
        data = fetch_data(api_url, AUTH_TOKEN)
        if data:
            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)

    if all_data:
        df = save_to_csv(all_data)
        if df is not None and not df.empty:
            insert_udob_deals_to_db(df)
        else:
            print("⚠️ Ma’lumot bo‘sh yoki CSV xatolik.")
    else:
        print("🚫 Umumiy ma’lumot kelmadi.")
