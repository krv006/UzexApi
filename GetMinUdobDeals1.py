import json
import subprocess
import pandas as pd
import pyodbc
from datetime import datetime, timedelta


def daterange_months(start_date, end_date):
    current = start_date.replace(day=1)
    while current <= end_date:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield current, min(next_month - timedelta(days=1), end_date)
        current = next_month


def fetch_data(api_url, auth_token):
    print(f"📡 So‘rov: {api_url}")
    try:
        command = f'curl -X GET "{api_url}" -H "accept: text/plain" -H "Authorization: {auth_token}"'
        process = subprocess.run(command, shell=True, capture_output=True, text=True, encoding="utf-8")
        if process.returncode == 0:
            print("✅ API javobi olindi")
            return process.stdout
        else:
            print(f"❌ API xatolik: {process.stderr}")
            return None
    except Exception as e:
        print(f"❌ API istisnosi: {e}")
        return None


def save_and_insert_to_db(data):
    if not data:
        print("⚠️ Ma’lumot yo‘q. O‘tkazib yuborildi.")
        return

    try:
        json_data = json.loads(data)
        if not isinstance(json_data, list) or not json_data:
            print("⚠️ JSON bo‘sh yoki noto‘g‘ri formatda.")
            return

        df = pd.DataFrame(json_data)
        print(f"📊 Qatorlar: {len(df)}")
        df = df.fillna("")
        df["deal_date"] = pd.to_datetime(df["deal_date"], errors="coerce")

        numeric_cols = ["deal_number", "deal_type", "deal_amount", "deal_price", "deal_cost",
                        "deal_currency", "register_id", "amount", "startingpricefrombill"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        conn = pyodbc.connect(
            "DRIVER={SQL Server};"
            "SERVER=192.168.111.14;"
            "DATABASE=Birja;"
            "UID=sa;"
            "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
            "TrustServerCertificate=yes;"
        )
        cursor = conn.cursor()
        cursor.fast_executemany = True

        records = df.values.tolist()
        total = 0
        for i in range(0, len(records), 500):
            batch = records[i:i + 500]
            cursor.executemany("""
                INSERT INTO dbo.GetMinUdobDeals (
                    deal_number, deal_date, deal_type, contract_number, seller_name,
                    seller_tin, seller_region, seller_district, product_name, deal_amount,
                    amount_unit, deal_price, deal_cost, deal_currency, buyer_tin,
                    buyer_name, buyer_region, register_id, deal_url, amount, startingpricefrombill
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            total += len(batch)
            print(f"✅ {total} ta yozuv DB ga qo‘shildi")

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ DB xatolik: {e}")


if __name__ == "__main__":
    print("🚀 Skript ishga tushdi")
    START_DATE = datetime(2024, 1, 1)
    END_DATE = datetime.today()
    AUTH_TOKEN = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    for begin, end in daterange_months(START_DATE, END_DATE):
        b_str = begin.strftime("%Y-%m-%d")
        e_str = end.strftime("%Y-%m-%d")
        print(f"\n📥 Yuklanmoqda: {b_str} → {e_str}")
        url = f"http://172.16.14.21:4041/GetMinUdobDeals/{b_str}/{e_str}"
        data = fetch_data(url, AUTH_TOKEN)
        save_and_insert_to_db(data)

    print("\n🎉 YUKLAB OLISH TUGADI")
