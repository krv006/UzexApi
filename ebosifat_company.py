import pandas as pd
import requests

TOKEN = "dbb9da12883a37250c4e2fec15591b8de0d2cea0"
BASE_URL = "https://ebiosifat.uz/api/v1/company/list/"

def extract_company_products(item):
    director = item.get("director_profile") or {}

    didox_region = item.get("didox_region") or {}
    didox_city = item.get("didox_city") or {}

    company_info = {
        "company_id": item.get("id"),
        "update_id": item.get("update_id"),
        "company_name": item.get("name"),
        "tin_number": item.get("tin_number"),
        "didox_region": didox_region.get("nameUzLatn"),
        "didox_region_code": didox_region.get("soato"),
        "didox_city": didox_city.get("nameUzLatn"),
        "didox_city_code": didox_city.get("soato"),
        "didox_city_region": didox_city.get("region"),
        "director_name": director.get("full_name"),
        "director_phone": director.get("phone_number"),
        "certificate": item.get("certificate"),
        "technical_passport": item.get("technical_passport"),
    }

    products = item.get("products", [])
    if not products:
        return [company_info | {
            "product_id": None,
            "product_name": None,
            "product_code": None,
            "product_description": None,
            "product_amount": None,
            "product_unit": None,
            "product_costs": None,
        }]

    product_rows = []
    for p in products:
        row = company_info | {
            "product_id": p.get("id"),
            "product_name": p.get("name"),
            "product_code": p.get("code"),
            "product_description": p.get("description"),
            "product_amount": p.get("amount"),
            "product_unit": p.get("unit"),
            "product_costs": p.get("costs"),
        }
        product_rows.append(row)
    return product_rows


def fetch_all_data(url, headers):
    all_data = []
    while url:
        print(f"⬇️ Yuklanmoqda: {url}")
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            for item in results:
                company_products = extract_company_products(item)
                all_data.extend(company_products)
            url = data.get("next")  # Keyingi sahifa
        else:
            print(f"❌ Xatolik: {response.status_code} - {response.text}")
            break
    return all_data


def main():
    headers = {
        "Authorization": f"Token {TOKEN}"
    }

    all_rows = fetch_all_data(BASE_URL, headers)

    df = pd.DataFrame(all_rows)
    df.to_csv("company_with_products.csv", index=False, encoding="utf-8-sig")
    print("✅ CSV faylga yozildi: company_with_products.csv")


if __name__ == "__main__":
    main()
