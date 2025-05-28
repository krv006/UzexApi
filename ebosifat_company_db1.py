import requests
import pandas as pd
import pyodbc

# 1. SQL ulanish
conn = pyodbc.connect(
    'DRIVER={SQL Server};SERVER=192.168.111.14;DATABASE=karantindb;UID=sa;PWD=AX8wFfMQrR6b9qdhHt2eYS'
)
cursor = conn.cursor()

# 2. Jadval yaratiladi (agar mavjud bo'lmasa)
cursor.execute("""
IF OBJECT_ID('dbo.Companies', 'U') IS NULL
CREATE TABLE dbo.Companies (
    company_id INT,
    update_id INT,
    company_name NVARCHAR(MAX),
    tin_number NVARCHAR(MAX),
    region_name NVARCHAR(MAX),
    region_id INT,
    district_name NVARCHAR(MAX),
    district_id INT,
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
""")
conn.commit()

# 3. API dan ma’lumotni olish
def get_api_data():
    response = requests.get("https://ebiosifat.uz/api/company/get-companies")
    if response.status_code == 200:
        data = response.json()
        return pd.DataFrame(data)
    else:
        raise Exception(f"API Error: {response.status_code}")

# 4. Ma'lumotlarni SQL Server`ga yozish
def save_to_db(df):
    for _, row in df.iterrows():
        cursor.execute("""
            MERGE dbo.Companies AS target
            USING (SELECT ? AS company_id, ? AS update_id, ? AS company_name, ? AS tin_number,
                          ? AS region_name, ? AS region_id, ? AS district_name, ? AS district_id,
                          ? AS director_name, ? AS director_phone, ? AS certificate, ? AS technical_passport,
                          ? AS product_id, ? AS product_name, ? AS product_code, ? AS product_description,
                          ? AS product_amount, ? AS product_unit, ? AS product_costs) AS source
            ON target.company_id = source.company_id AND
               ((target.product_id = source.product_id) OR (target.product_id IS NULL AND source.product_id IS NULL))
            WHEN MATCHED THEN
                UPDATE SET
                    update_id = source.update_id,
                    company_name = source.company_name,
                    tin_number = source.tin_number,
                    region_name = source.region_name,
                    region_id = source.region_id,
                    district_name = source.district_name,
                    district_id = source.district_id,
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
                INSERT (company_id, update_id, company_name, tin_number,
                        region_name, region_id, district_name, district_id,
                        director_name, director_phone, certificate, technical_passport,
                        product_id, product_name, product_code, product_description,
                        product_amount, product_unit, product_costs)
                VALUES (source.company_id, source.update_id, source.company_name, source.tin_number,
                        source.region_name, source.region_id, source.district_name, source.district_id,
                        source.director_name, source.director_phone, source.certificate, source.technical_passport,
                        source.product_id, source.product_name, source.product_code, source.product_description,
                        source.product_amount, source.product_unit, source.product_costs);
        """, row["company_id"], row["update_id"], row["company_name"], row["tin_number"],
             row["region_name"], row["region_id"], row["district_name"], row["district_id"],
             row["director_name"], row["director_phone"], row["certificate"], row["technical_passport"],
             row["product_id"], row["product_name"], row["product_code"], row["product_description"],
             row["product_amount"], row["product_unit"], row["product_costs"])
    conn.commit()

# 5. Ishga tushirish
if __name__ == "__main__":
    df = get_api_data()
    save_to_db(df)
    print("✅ API'dan to'g'ridan-to'g'ri DB ga yozildi!")
