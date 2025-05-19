import json
import pandas as pd
import requests
import pyodbc
from datetime import datetime

def fetch_data(api_url, auth_token):
    try:
        headers = {
            "accept": "text/plain",
            "Authorization": auth_token
        }
        response = requests.get(api_url, headers=headers, timeout=30)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ API xato kodi: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ API da xatolik: {e}")
        return None

def save_to_csv(df, filename="GetSpotCottonDeals.csv"):
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

        # Jadvalni yaratish agar mavjud bo'lmasa
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

        # NaN ni tozalash va turi o‘zgartirish
        df = df.fillna("")
        df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")

        # ❗ Noto‘g‘ri deal_date satrlarini olib tashlash
        df = df[df["deal_date"].notna()]

        # Agar noto‘g‘ri sana bor bo‘lsa, chiqarish
        bad_dates = df[df["deal_date"].isna()]
        if not bad_dates.empty:
            print("⚠️ Sanasi noto‘g‘ri bo‘lgan satrlar:")
            print(bad_dates[["deal_number", "deal_date"]])

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
            cursor.executemany("""
                INSERT INTO dbo.SpotCottonDeals (
                    deal_number, deal_date, contract_number, seller_name, seller_tin,
                    seller_region, seller_district, product_name, deal_amount, deal_price,
                    deal_cost, deal_currency, buyer_name, buyer_tin, buyer_region, buyer_district
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            total += len(batch)
            print(f"📥 {total} qator yozildi...")

        cursor.close()
        conn.close()
        print(f"✅ Jami {total} qator SpotCottonDeals ga yozildi.")
    except Exception as e:
        print(f"❌ DB xatolik: {e}")

if __name__ == "__main__":
    BEGINDATE = "2024-01-01"
    ENDDATE = datetime.today().strftime("%Y-%m-%d")
    API_URL = f"http://172.16.14.21:4041/GetSpotCottonDeals?begindate={BEGINDATE}&enddate={ENDDATE}"
    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    data = fetch_data(API_URL, AUTH_TOKEN)
    if data:
        df = pd.DataFrame(data)
        save_to_csv(df)
        insert_to_db(df)
    else:
        print("🚫 API javobi bo‘sh.")
