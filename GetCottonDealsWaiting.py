from datetime import datetime

import pandas as pd
import pyodbc
import requests


def fetch_data(api_url, auth_token):
    try:
        headers = {
            "accept": "text/plain",
            "Authorization": auth_token
        }
        response = requests.get(api_url, headers=headers, timeout=60)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ API xato kodi: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ API da xatolik: {e}")
        return None


def save_to_csv(df, filename="GetCottonDealsWaiting.csv"):
    if df is not None and not df.empty:
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"📁 CSV saqlandi: {filename}")
    else:
        print("❌ CSV saqlab bo‘lmadi. Data yo‘q.")


def insert_to_db(df, batch_size=500):
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
        IF OBJECT_ID('dbo.CottonDealsWaiting', 'U') IS NULL
        CREATE TABLE dbo.CottonDealsWaiting (
            deal_number INT PRIMARY KEY,
            deal_date DATETIME,
            deal_type INT,
            contract_number NVARCHAR(500),
            seller_name NVARCHAR(500),
            seller_tin NVARCHAR(500),
            seller_region INT,
            seller_district INT,
            product_name NVARCHAR(500),
            deal_amount FLOAT,
            amount_unit NVARCHAR(500),
            deal_price FLOAT,
            deal_cost FLOAT,
            deal_currency INT,
            buyer_tin NVARCHAR(500),
            buyer_name NVARCHAR(500),
            buyer_region INT,
            register_id INT,
            deal_url NVARCHAR(500)
        )
        """
        cursor.execute(create_query)
        conn.commit()

        df = df.fillna("")
        df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")
        df = df[df["deal_date"].notna()]

        # Tiplarni to'g'ri o'rnatish
        int_cols = [
            "deal_number", "deal_type", "seller_region", "seller_district",
            "deal_currency", "buyer_region", "register_id"
        ]
        float_cols = ["deal_amount", "deal_price", "deal_cost"]
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        df["deal_number"] = df["deal_number"].astype(int)

        # Ichki dublikatsiyalarni olib tashlash
        df = df.drop_duplicates(subset=['deal_number'])

        # Bazadagi mavjud deal_number larni olish
        existing_deals = pd.read_sql("SELECT deal_number FROM dbo.CottonDealsWaiting", conn)
        existing_deals_set = set(existing_deals["deal_number"].astype(int))

        # Yangi faqat noyob deal_number larni qoldirish
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
                    INSERT INTO dbo.CottonDealsWaiting (
                        deal_number, deal_date, deal_type, contract_number, seller_name,
                        seller_tin, seller_region, seller_district, product_name, deal_amount,
                        amount_unit, deal_price, deal_cost, deal_currency, buyer_tin,
                        buyer_name, buyer_region, register_id, deal_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                conn.commit()
                total_inserted += len(batch)
                print(f"📥 {total_inserted} qator yozildi...")
            except Exception as batch_error:
                print(f"⚠️ Batch skip qilindi: {batch_error}")

        print(f"✅ Jami {total_inserted} qator CottonDealsWaiting ga yozildi.")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ DB xatolik: {e}")


if __name__ == "__main__":
    BEGINDATE = "2024-01-01"
    ENDDATE = datetime.today().strftime("%Y-%m-%d")
    API_URL = f"http://172.16.14.21:4041/GetCottonDealsWaiting/{BEGINDATE}/{ENDDATE}"
    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    data = fetch_data(API_URL, AUTH_TOKEN)
    if data:
        df = pd.DataFrame(data)
        save_to_csv(df)
        insert_to_db(df)
    else:
        print("🚫 API javobi bo‘sh.")
