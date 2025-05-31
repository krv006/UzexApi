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


def save_to_csv(data, filename="GetBuyers.csv"):
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


def insert_buyers_to_db(df, batch_size=500):
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

        # Jadvalni yaratish, cluster_tin ustuni PRIMARY KEY bo'lib
        create_query = """
        IF OBJECT_ID('dbo.GetBuyers', 'U') IS NULL
        CREATE TABLE dbo.GetBuyers (
            cluster_name NVARCHAR(255),
            cluster_tin NVARCHAR(20) PRIMARY KEY,
            cluster_type INT,
            crop_id NVARCHAR(50),
            cluster_id INT,
            cluster_region1 INT,
            cluster_region2 INT,
            cluster_region3 INT,
            cluster_region4 INT,
            cluster_region5 INT,
            cluster_region6 INT,
            cluster_region7 INT,
            cluster_region8 INT,
            cluster_region9 INT,
            cluster_region10 INT,
            cluster_region11 INT,
            cluster_region12 INT,
            cluster_region13 INT
        )
        """
        cursor.execute(create_query)
        conn.commit()

        # Tozalash va tiplash
        df["cluster_name"] = df["cluster_name"].astype(str)
        df["cluster_tin"] = df["cluster_tin"].astype(str)
        df["cluster_type"] = pd.to_numeric(df["cluster_type"], errors="coerce").fillna(0).astype(int)
        df["crop_id"] = df["crop_id"].astype(str)
        df["cluster_id"] = pd.to_numeric(df["cluster_id"], errors="coerce").fillna(0).astype(int)

        for i in range(1, 14):
            col = f"cluster_region{i}"
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        df = df.fillna("")

        # Duplicatlarni df ichida olib tashlash
        df = df.drop_duplicates(subset=['cluster_tin'])

        # Bazadagi mavjud cluster_tin larni olish
        existing_tins = pd.read_sql("SELECT cluster_tin FROM dbo.GetBuyers", conn)
        existing_tins_set = set(existing_tins["cluster_tin"].astype(str))

        # Faqat yangi cluster_tin larni qoldirish
        df = df[~df["cluster_tin"].isin(existing_tins_set)]

        if df.empty:
            print("📭 Yangi yoziladigan cluster_tin yo‘q, barcha ma’lumotlar DBda mavjud.")
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
                    INSERT INTO dbo.GetBuyers (
                        cluster_name, cluster_tin, cluster_type, crop_id, cluster_id,
                        cluster_region1, cluster_region2, cluster_region3, cluster_region4, cluster_region5,
                        cluster_region6, cluster_region7, cluster_region8, cluster_region9, cluster_region10,
                        cluster_region11, cluster_region12, cluster_region13
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                conn.commit()
                total_inserted += len(batch)
                print(f"📥 {total_inserted} qator qo‘shildi...")
            except Exception as batch_error:
                print(f"⚠️ Batch o'tkazib yuborildi: {batch_error}")

        print(f"✅ Jami {total_inserted} qator Buyers jadvaliga qo‘shildi.")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Buyers DB xatolik: {e}")


if __name__ == "__main__":
    API_URL = "http://172.16.14.21:4041/GetBuyers"
    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    data = fetch_data_with_curl(API_URL, AUTH_TOKEN)
    df = save_to_csv(data)

    if df is not None and not df.empty:
        insert_buyers_to_db(df)
    else:
        print("⚠️ Ma’lumot bo‘sh yoki CSV xatolik.")
