import pandas as pd
import pyodbc
import requests
import logging
import numpy as np

# Logging sozlamalari
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- API konfiguratsiyasi ---
BASE_URL = "https://ebiosifat.uz/api/v1/company/list/"
TOKEN = "dbb9da12883a37250c4e2fec15591b8de0d2cea0"

HEADERS = {
    "Authorization": f"Token {TOKEN}"
}

# --- SQL Server konfiguratsiyasi ---
try:
    conn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=192.168.111.14;DATABASE=karantindb;UID=sa;PWD=AX8wFfMQrR6b9qdhHt2eYS'
    )
    cursor = conn.cursor()
    logger.info("Ma'lumotlar bazasiga muvaffaqiyatli ulanildi")
except Exception as e:
    logger.error(f"Ma'lumotlar bazasiga ulanishda xato: {e}")
    raise

# Jadval yaratish yoki yangilash
cursor.execute("""
    IF OBJECT_ID('dbo.Companies_test', 'U') IS NULL
    BEGIN
        CREATE TABLE dbo.Companies_test (
            company_id INT NOT NULL,
            update_id INT NULL,
            company_name NVARCHAR(MAX) NULL,
            tin_number NVARCHAR(MAX) NULL,
            region_name NVARCHAR(MAX) NULL,
            region_soato INT NULL,
            district_name NVARCHAR(MAX) NULL,
            district_id INT NULL,
            district_soato NVARCHAR(MAX) NULL,
            district_region NVARCHAR(MAX) NULL,
            director_name NVARCHAR(MAX) NULL,
            director_phone NVARCHAR(MAX) NULL,
            certificate NVARCHAR(MAX) NULL,
            technical_passport NVARCHAR(MAX) NULL,
            product_id INT NULL,  -- product_id NULL sifatida saqlanadi
            product_name NVARCHAR(MAX) NULL,
            product_code NVARCHAR(MAX) NULL,
            product_description NVARCHAR(MAX) NULL,
            product_amount BIGINT NULL,
            product_unit NVARCHAR(MAX) NULL,
            product_costs NVARCHAR(MAX) NULL,
            CONSTRAINT PK_Companies PRIMARY KEY (company_id)
        )
    END
    ELSE
    BEGIN
        -- region_soato ustunini qo'shish
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'region_soato' AND Object_ID = Object_ID(N'dbo.Companies_test'))
        BEGIN
            ALTER TABLE dbo.Companies_test ADD region_soato INT NULL
        END
        -- district_soato ustunini qo'shish
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'district_soato' AND Object_ID = Object_ID(N'dbo.Companies_test'))
        BEGIN
            ALTER TABLE dbo.Companies_test ADD district_soato NVARCHAR(MAX) NULL
        END
        -- district_region ustunini qo'shish
        IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'district_region' AND Object_ID = Object_ID(N'dbo.Companies_test'))
        BEGIN
            ALTER TABLE dbo.Companies_test ADD district_region NVARCHAR(MAX) NULL
        END
        -- product_id ustunini NULL ga aylantirish va PRIMARY KEY ni o'zgartirish
        IF EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'product_id' AND Object_ID = Object_ID(N'dbo.Companies_test'))
        BEGIN
            -- Avval PRIMARY KEY cheklovini o'chirish
            IF EXISTS (SELECT 1 FROM sys.objects WHERE name = 'PK_Companies' AND type = 'PK')
            BEGIN
                ALTER TABLE dbo.Companies_test DROP CONSTRAINT PK_Companies
            END
            -- product_id ni NULL ga aylantirish
            IF EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'product_id' AND is_nullable = 0 AND Object_ID = Object_ID(N'dbo.Companies_test'))
            BEGIN
                ALTER TABLE dbo.Companies_test ALTER COLUMN product_id INT NULL
            END
            -- Yangi PRIMARY KEY qo'shish
            ALTER TABLE dbo.Companies_test ADD CONSTRAINT PK_Companies PRIMARY KEY (company_id)
        END
    END
    """)
conn.commit()
logger.info("Jadval tuzilishi muvaffaqiyatli yaratildi/yangilandi")


def get_api_data():
    all_data = []
    url = BASE_URL

    try:
        while url:
            logger.info(f"Ma'lumotlar olinmoqda: {url}")
            response = requests.get(url, headers=HEADERS)
            if response.status_code != 200:
                raise Exception(f"API Error: {response.status_code} - {response.text}")

            data = response.json()
            companies = data.get("results", [])
            if not companies:
                logger.warning("API'dan hech qanday ma'lumot olinmadi")
                break

            all_data.extend(companies)
            logger.info(f"{len(companies)} ta kompaniya ma'lumoti olindi (joriy sahifa)")

            url = data.get("next")
            if not url:
                logger.info("Barcha sahifalar olindi")
                break

        if not all_data:
            logger.warning("Umumiy ma'lumotlar topilmadi")
            return pd.DataFrame()

        df = pd.json_normalize(all_data)
        logger.info(f"Jami {len(df)} ta kompaniya ma'lumoti olindi")

        # Direktor ma'lumotlari
        df["director_name"] = df.get("director_profile.full_name", pd.Series([None] * len(df)))
        df["director_phone"] = df.get("director_profile.phone_number", pd.Series([None] * len(df)))

        # Hududiy ma'lumotlar
        df["region_name"] = df.get("didox_region.nameUzLatn", pd.Series([None] * len(df)))
        df["region_soato"] = df.get("didox_region.soato", pd.Series([None] * len(df)))

        # Tuman ma'lumotlari
        df["district_name"] = df.get("didox_city.nameUzLatn", pd.Series([None] * len(df)))
        df["district_soato"] = df.get("didox_city.soato", pd.Series([None] * len(df)))
        df["district_region"] = df.get("didox_city.region", pd.Series([None] * len(df)))
        df["district_id"] = df.get("didox_city.id", pd.Series([None] * len(df)))

        # Mahsulotlar ro'yxatini explode qilish
        df = df.explode("products").reset_index(drop=True)
        df["products"] = df["products"].apply(lambda x: x if isinstance(x, dict) else {})

        products_df = pd.json_normalize(df["products"])
        products_df = products_df.add_prefix("product_")

        df = pd.concat([df, products_df], axis=1)

        # Keraksiz ustunlarni olib tashlash (product_id ni o'z ichiga oladi)
        drop_cols = [
            "director_profile.full_name", "director_profile.phone_number",
            "didox_region.nameUzLatn", "didox_region.id",
            "didox_city.nameUzLatn", "didox_city.id",
            "director_profile", "didox_region", "didox_city", "products",
            "product_id"  # product_id ni olib tashlash
        ]
        df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True)

        # Ustun nomlarini o'zgartirish
        df.rename(columns={"id": "company_id", "name": "company_name"}, inplace=True)

        # INT va BIGINT ustunlar uchun ma'lumotlarni tozalash
        int_columns = ["company_id", "update_id", "region_soato", "district_id"]
        for col in int_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')  # NaN -> None

        # BIGINT uchun alohida tozalash (product_amount)
        df["product_amount"] = pd.to_numeric(df["product_amount"], errors='coerce').astype('Int64')

        # product_costs ni satr sifatida saqlash, nan ni "" ga aylantirish
        df["product_costs"] = df["product_costs"].apply(lambda x: "" if pd.isna(x) else str(x))

        # Kerakli ustunlar ro'yxati
        expected_columns = [
            "company_id", "update_id", "company_name", "tin_number",
            "region_name", "region_soato", "district_name", "district_id",
            "district_soato", "district_region",
            "director_name", "director_phone", "certificate", "technical_passport",
            "product_name", "product_code", "product_description",
            "product_amount", "product_unit", "product_costs"
        ]

        # Mavjud bo'lmagan ustunlarni qo'shish
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None

        # Ma'lumotlarni tekshirish
        logger.info(f"DataFrame ustunlari: {df.columns.tolist()}")
        logger.info(f"Birinchi qator namunasi: {df.iloc[0].to_dict() if not df.empty else 'DataFrame bo\'sh'}")
        logger.info("product_id ma'lumotlar bazasiga yozishda e'tiborsiz qoldirildi")
        return df[expected_columns]
    except Exception as e:
        logger.error(f"API ma'lumotlarini olishda xato: {e}")
        raise


def save_to_db(df):
    if df.empty:
        logger.warning("DataFrame bo'sh, ma'lumotlar bazasiga yoziladigan ma'lumot yo'q")
        return

    sql = """
            MERGE dbo.Companies_test AS target
            USING (SELECT ? AS company_id, ? AS update_id, ? AS company_name, ? AS tin_number,
                          ? AS region_name, ? AS region_soato, ? AS district_name, ? AS district_id,
                          ? AS district_soato, ? AS district_region,
                          ? AS director_name, ? AS director_phone, ? AS certificate, ? AS technical_passport,
                          ? AS product_name, ? AS product_code, ? AS product_description,
                          ? AS product_amount, ? AS product_unit, ? AS product_costs) AS source
            ON target.company_id = source.company_id
            WHEN MATCHED THEN
                UPDATE SET
                    update_id = source.update_id,
                    company_name = source.company_name,
                    tin_number = source.tin_number,
                    region_name = source.region_name,
                    region_soato = source.region_soato,
                    district_name = source.district_name,
                    district_id = source.district_id,
                    district_soato = source.district_soato,
                    district_region = source.district_region,
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
                        region_name, region_soato, district_name, district_id,
                        district_soato, district_region,
                        director_name, director_phone, certificate, technical_passport,
                        product_name, product_code, product_description,
                        product_amount, product_unit, product_costs)
                VALUES (source.company_id, source.update_id, source.company_name, source.tin_number,
                        source.region_name, source.region_soato, source.district_name, source.district_id,
                        source.district_soato, source.district_region,
                        source.director_name, source.director_phone, source.certificate, source.technical_passport,
                        source.product_name, source.product_code, source.product_description,
                        source.product_amount, source.product_unit, source.product_costs);
        """

    try:
        for index, row in df.iterrows():
            try:
                params = [
                    row["company_id"] if pd.notna(row["company_id"]) else None,  # 1: INT
                    row["update_id"] if pd.notna(row["update_id"]) else None,  # 2: INT
                    row["company_name"] if pd.notna(row["company_name"]) else None,  # 3: NVARCHAR
                    row["tin_number"] if pd.notna(row["tin_number"]) else None,  # 4: NVARCHAR
                    row["region_name"] if pd.notna(row["region_name"]) else None,  # 5: NVARCHAR
                    row["region_soato"] if pd.notna(row["region_soato"]) else None,  # 6: INT
                    row["district_name"] if pd.notna(row["district_name"]) else None,  # 7: NVARCHAR
                    row["district_id"] if pd.notna(row["district_id"]) else None,  # 8: INT
                    row["district_soato"] if pd.notna(row["district_soato"]) else None,  # 9: NVARCHAR
                    row["district_region"] if pd.notna(row["district_region"]) else None,  # 10: NVARCHAR
                    row["director_name"] if pd.notna(row["director_name"]) else None,  # 11: NVARCHAR
                    row["director_phone"] if pd.notna(row["director_phone"]) else None,  # 12: NVARCHAR
                    row["certificate"] if pd.notna(row["certificate"]) else None,  # 13: NVARCHAR
                    row["technical_passport"] if pd.notna(row["technical_passport"]) else None,  # 14: NVARCHAR
                    row["product_name"] if pd.notna(row["product_name"]) else None,  # 15: NVARCHAR
                    row["product_code"] if pd.notna(row["product_code"]) else None,  # 16: NVARCHAR
                    row["product_description"] if pd.notna(row["product_description"]) else None,  # 17: NVARCHAR
                    row["product_amount"] if pd.notna(row["product_amount"]) else None,  # 18: BIGINT
                    row["product_unit"] if pd.notna(row["product_unit"]) else None,  # 19: NVARCHAR
                    row["product_costs"] if pd.notna(row["product_costs"]) else None  # 20: NVARCHAR
                ]
                cursor.execute(sql, params)
            except Exception as e:
                logger.error(f"Xato {index}-qatorda: {row.to_dict()}\nXato xabari: {e}")
                raise
        conn.commit()
        logger.info(f"{len(df)} ta yozuv ma'lumotlar bazasiga muvaffaqiyatli yozildi")
    except Exception as e:
        logger.error(f"Ma'lumotlar bazasiga yozishda xato: {e}")
        conn.rollback()
        raise


if __name__ == "__main__":
    try:
        df = get_api_data()
        save_to_db(df)
        print("✅ API'dan to'g'ridan-to'g'ri DB ga yozildi!")
    except Exception as e:
        logger.error(f"Umumiy xato: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info("Ma'lumotlar bazasi ulanishi yopildi")
