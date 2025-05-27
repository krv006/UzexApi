import requests
import pandas as pd
import pyodbc
import time
import traceback

BASE_URL = "https://ebiosifat.uz/api/v1/company/list/"
TOKEN = "dbb9da12883a37250c4e2fec15591b8de0d2cea0"

HEADERS = {
    "Authorization": f"Token {TOKEN}"
}

def fetch_all_data(base_url, headers):
    all_companies = []
    url = base_url
    page_num = 1

    while url:
        print(f"⬇️ Yuklanmoqda: {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        all_companies.extend(data.get("results", []))
        url = data.get("next")
        page_num += 1

        time.sleep(0.5)

    print(f"✅ Jami {len(all_companies)} ta kompaniya olindi.")
    return all_companies

def prepare_dataframe(data):
    rows = []
    for item in data:
        products = item.get("products", [])
        if products:
            for prod in products:
                rows.append({
                    "company_id": item.get("id"),
                    "update_id": item.get("update_id"),
                    "company_name": item.get("name"),
                    "tin_number": item.get("tin_number"),
                    "director_name": item.get("director_profile", {}).get("full_name") if item.get("director_profile") else None,
                    "director_phone": item.get("director_profile", {}).get("phone_number") if item.get("director_profile") else None,
                    "certificate": item.get("certificate"),
                    "technical_passport": item.get("technical_passport"),
                    "product_id": prod.get("id"),
                    "product_name": prod.get("name"),
                    "product_code": prod.get("code"),
                    "product_description": prod.get("description"),
                    "product_amount": prod.get("amount"),
                    "product_unit": prod.get("unit"),
                    "product_costs": prod.get("costs"),
                })
        else:
            rows.append({
                "company_id": item.get("id"),
                "update_id": item.get("update_id"),
                "company_name": item.get("name"),
                "tin_number": item.get("tin_number"),
                "director_name": item.get("director_profile", {}).get("full_name") if item.get("director_profile") else None,
                "director_phone": item.get("director_profile", {}).get("phone_number") if item.get("director_profile") else None,
                "certificate": item.get("certificate"),
                "technical_passport": item.get("technical_passport"),
                "product_id": None,
                "product_name": None,
                "product_code": None,
                "product_description": None,
                "product_amount": None,
                "product_unit": None,
                "product_costs": None,
            })

    df = pd.DataFrame(rows)
    return df

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

        # Jadvalni yaratamiz agar mavjud bo'lmasa
        create_query = """
        IF OBJECT_ID('dbo.Companies', 'U') IS NULL
        CREATE TABLE dbo.Companies (
            company_id INT,
            update_id INT,
            company_name NVARCHAR(MAX),
            tin_number NVARCHAR(MAX),
            director_name NVARCHAR(MAX),
            director_phone NVARCHAR(MAX),
            certificate NVARCHAR(MAX),
            technical_passport NVARCHAR(MAX),
            product_id INT,
            product_name NVARCHAR(MAX),
            product_code NVARCHAR(MAX),
            product_description NVARCHAR(MAX),
            product_amount BIGINT,
            product_unit NVARCHAR(MAX),
            product_costs NVARCHAR(MAX),
            CONSTRAINT PK_Companies PRIMARY KEY (company_id, product_id)
        )
        """
        cursor.execute(create_query)
        conn.commit()

        expected_columns = [
            "company_id", "update_id", "company_name", "tin_number", "director_name", "director_phone",
            "certificate", "technical_passport", "product_id", "product_name", "product_code",
            "product_description", "product_amount", "product_unit", "product_costs"
        ]

        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        df = df.fillna("")

        numeric_cols = ["company_id", "update_id", "product_id", "product_amount"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        total_processed = 0
        for idx, row in df.iterrows():
            try:
                cursor.execute("""
                    MERGE dbo.Companies AS target
                    USING (SELECT ? AS company_id, ? AS update_id, ? AS company_name, ? AS tin_number, ? AS director_name,
                                  ? AS director_phone, ? AS certificate, ? AS technical_passport, ? AS product_id,
                                  ? AS product_name, ? AS product_code, ? AS product_description, ? AS product_amount,
                                  ? AS product_unit, ? AS product_costs) AS source
                    ON target.company_id = source.company_id AND
                       ((target.product_id = source.product_id) OR (target.product_id IS NULL AND source.product_id IS NULL))
                    WHEN MATCHED THEN
                        UPDATE SET
                            update_id = source.update_id,
                            company_name = source.company_name,
                            tin_number = source.tin_number,
                            director_name = source.director_name,
                            director_phone = source.director_phone,
                            certificate = source.certificate,
                            technical_passport = source.technical_passport,
                            product_name = source.product_name,
                            product_code = source.product_code,
                            product_description = source.product_description,
                            product_amount = source.product_amount,
                            product_unit = source.product_unit,
                            product_costs = source.product_costs
                    WHEN NOT MATCHED THEN
                        INSERT (company_id, update_id, company_name, tin_number, director_name, director_phone, certificate, technical_passport,
                                product_id, product_name, product_code, product_description, product_amount, product_unit, product_costs)
                        VALUES (source.company_id, source.update_id, source.company_name, source.tin_number, source.director_name, source.director_phone, source.certificate, source.technical_passport,
                                source.product_id, source.product_name, source.product_code, source.product_description, source.product_amount, source.product_unit, source.product_costs);
                """,
                row["company_id"], row["update_id"], row["company_name"], row["tin_number"], row["director_name"], row["director_phone"],
                row["certificate"], row["technical_passport"], row["product_id"], row["product_name"], row["product_code"],
                row["product_description"], row["product_amount"], row["product_unit"], row["product_costs"])
                conn.commit()
                total_processed += 1
            except Exception as e:
                print(f"❌ Xato qator id={row['company_id']} product_id={row['product_id']}: {e}")

        print(f"✅ {total_processed} ta qator DB ga yozildi yoki yangilandi.")
    except Exception:
        print("❌ Umumiy DB xatolik:")
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    data = fetch_all_data(BASE_URL, HEADERS)
    df = prepare_dataframe(data)
    print(df.head())
    insert_to_db(df)

if __name__ == "__main__":
    main()
