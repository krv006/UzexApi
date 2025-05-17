import json
import subprocess
import time

import pandas as pd
import pyodbc


def connect_vpn(server, username, password):
    try:
        subprocess.run('rasdial "MyVPN" /disconnect', shell=True, capture_output=True, text=True)
        time.sleep(2)
        connect_command = f'rasdial "MyVPN" {username} {password}'
        process = subprocess.run(connect_command, shell=True, capture_output=True, text=True, encoding="utf-8")
        if "connected" in process.stdout.lower():
            print("✅ VPN ulanishi muvaffaqiyatli!")
        else:
            print(f"❌ VPN ulanishda xatolik: {process.stdout or process.stderr}")
    except Exception as e:
        print(f"❌ VPN xatolik: {e}")


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


def save_to_csv(data, filename="GetAgroSellersTin.csv"):
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


def insert_to_db(df):
    try:
        conn = pyodbc.connect(
            "DRIVER={SQL Server};"  # <-- universal & xavfsiz driver
            "SERVER=192.168.111.14;"
            "DATABASE=Birja;"
            "UID=sa;"
            "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
            "TrustServerCertificate=yes;"
        )
        cursor = conn.cursor()

        # Table yaratish agar mavjud bo'lmasa
        create_table_query = """
        IF OBJECT_ID('dbo.AgroSellersTin', 'U') IS NULL
        CREATE TABLE dbo.AgroSellersTin (
            tin NVARCHAR(20),
            Year FLOAT
        )
        """
        cursor.execute(create_table_query)
        conn.commit()

        # 🔥 Eski malumotlarni o‘chirish
        cursor.execute("TRUNCATE TABLE dbo.AgroSellersTin")
        conn.commit()
        print("🧹 Eski ma’lumotlar tozalandi.")

        # 🧼 Clean data
        df = df[["tin", "Year"]].copy()
        df["tin"] = df["tin"].astype(str)
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
        df = df.dropna(subset=["tin", "Year"])

        records = df.values.tolist()
        cursor.fast_executemany = True
        cursor.executemany("INSERT INTO dbo.AgroSellersTin (tin, Year) VALUES (?, ?)", records)
        conn.commit()

        print(f"✅ {len(records)} qator DB ga qo‘shildi.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ DB xatolik: {e}")


if __name__ == "__main__":
    VPN_SERVER = "kvpnn.uzex.uz"
    VPN_USERNAME = "n.jumabayev"
    VPN_PASSWORD = "bgtyhn@123"
    API_URL = "http://172.16.14.21:4041/GetAgroSellersTin"
    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    connect_vpn(VPN_SERVER, VPN_USERNAME, VPN_PASSWORD)

    data = fetch_data_with_curl(API_URL, AUTH_TOKEN)

    df = save_to_csv(data)

    if df is not None and not df.empty:
        insert_to_db(df)
    else:
        print("⚠️ Ma’lumot bo‘sh yoki CSV xatolik.")
