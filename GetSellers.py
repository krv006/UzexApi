import json
import subprocess
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


def save_to_csv(data, filename="GetSellers.csv"):
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


def insert_sellers_to_db(df, batch_size=500):  # 🚀 Yangi: batching
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

        # 🏗️ Jadval yaratish (agar mavjud bo'lmasa)
        create_query = """
        IF OBJECT_ID('dbo.GetSellers', 'U') IS NULL
        CREATE TABLE dbo.GetSellers (
            farmer_name NVARCHAR(500),
            farmer_tin NVARCHAR(500),
            farmer_region INT,
            farmer_district INT,
            norm_amount FLOAT,
            amount_unit NVARCHAR(500),
            extra_amount INT,
            crop_id NVARCHAR(500),
            hectare FLOAT,
            productivity FLOAT,
            contract_id INT,
            harvest_year INT,
            volume_type INT
        )
        """
        cursor.execute(create_query)
        conn.commit()

        # 🧼 Clean data
        df["farmer_name"] = df["farmer_name"].astype(str)
        df["farmer_tin"] = df["farmer_tin"].astype(str)
        df["farmer_region"] = pd.to_numeric(df["farmer_region"], errors="coerce").fillna(0).astype(int)
        df["farmer_district"] = pd.to_numeric(df["farmer_district"], errors="coerce").fillna(0).astype(int)
        df["norm_amount"] = pd.to_numeric(df["norm_amount"], errors="coerce").fillna(0).astype(float)
        df["amount_unit"] = df["amount_unit"].astype(str)
        df["extra_amount"] = pd.to_numeric(df["extra_amount"], errors="coerce").fillna(0).astype(int)
        df["crop_id"] = df["crop_id"].astype(str)
        df["hectare"] = pd.to_numeric(df["hectare"], errors="coerce").fillna(0).astype(float)
        df["productivity"] = pd.to_numeric(df["productivity"], errors="coerce").fillna(0).astype(float)
        df["contract_id"] = pd.to_numeric(df["contract_id"], errors="coerce").fillna(0).astype(int)
        df["harvest_year"] = pd.to_numeric(df["harvest_year"], errors="coerce").fillna(0).astype(int)
        df["volume_type"] = pd.to_numeric(df["volume_type"], errors="coerce").fillna(0).astype(int)

        df = df.fillna("")
        records = df.values.tolist()

        # 🚀 Bulk insert in batches
        total_inserted = 0
        cursor.fast_executemany = True
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany("""
                INSERT INTO dbo.GetSellers (
                    farmer_name, farmer_tin, farmer_region, farmer_district, norm_amount,
                    amount_unit, extra_amount, crop_id, hectare, productivity,
                    contract_id, harvest_year, volume_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            total_inserted += len(batch)
            print(f"📥 {total_inserted} qator qo‘shildi...")

        print(f"✅ Jami {total_inserted} qator Sellers jadvaliga qo‘shildi.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Sellers DB xatolik: {e}")


if __name__ == "__main__":
    API_URL = "http://172.16.14.21:4041/GetSellers"
    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    data = fetch_data_with_curl(API_URL, AUTH_TOKEN)
    df = save_to_csv(data)

    if df is not None and not df.empty:
        insert_sellers_to_db(df)
    else:
        print("⚠️ Ma’lumot bo‘sh yoki CSV xatolik.")
