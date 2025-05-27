import pandas as pd
import requests

url = 'https://ebiosifat.uz/api/v1/common/regions/'

headers = {
    'Authorization': 'Token dbb9da12883a37250c4e2fec15591b8de0d2cea0'
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()

    df = pd.DataFrame(data)
    df.to_csv('common.csv', index=False, encoding='utf-8-sig')

    print("✅ Ma'lumotlar muvaffaqiyatli CSV faylga yozildi.")
else:
    print(f"❌ Xatolik yuz berdi: {response.status_code} - {response.text}")
