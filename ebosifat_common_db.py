import traceback

import pandas as pd
import pyodbc
import requests

URL = 'https://ebiosifat.uz/api/v1/common/regions/'
TOKEN = 'dbb9da12883a37250c4e2fec15591b8de0d2cea0'

HEADERS = {
    'Authorization': f'Token {TOKEN}'
}


def fetch_regions(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Xatolik yuz berdi: {response.status_code} - {response.text}")
        return None


def prepare_dataframe(data):
    df = pd.DataFrame(data)
    return df[['id', 'nameUzLatn', 'soato']]


def insert_to_db(df):
    print("🗃️ SQL Serverga yozish jarayoni boshlandi...")
    conn = None
    cursor = None
    try:
        conn = pyodbc.connect(
            "DRIVER={SQL Server};"
            "SERVER=192.168.111.14;"
            "DATABASE=karantindb;"
            "UID=sa;"
            "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
            "TrustServerCertificate=yes;"
        )
        cursor = conn.cursor()

        create_query = """
        IF OBJECT_ID('dbo.Common', 'U') IS NULL
        CREATE TABLE dbo.Common (
            id INT PRIMARY KEY,
            nameUzLatn NVARCHAR(500),
            soato NVARCHAR(50)
        )
        """
        cursor.execute(create_query)
        conn.commit()

        for idx, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO dbo.Common (id, nameUzLatn, soato)
                    VALUES (?, ?, ?)
                """, row['id'], row['nameUzLatn'], row['soato'])
                conn.commit()
                print(f"✅ id={row['id']} yozildi")
            except Exception as e:
                print(f"❌ id={row['id']} yozishda xato: {e}")

        print(f"✅ {len(df)} ta qator DB ga yozildi.")
    except Exception:
        print("❌ Umumiy DB xatolik:")
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def main():
    data = fetch_regions(URL, HEADERS)
    if data:
        df = prepare_dataframe(data)
        print(df.head())
        insert_to_db(df)


if __name__ == "__main__":
    main()
