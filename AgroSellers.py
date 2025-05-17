import json
import subprocess
import pandas as pd
import pyodbc


def fetch_data_with_curl(api_url, auth_token):
    """API dan curl orqali ma'lumot olish."""
    try:
        command = f'curl -X GET "{api_url}" -H "accept: text/plain" -H "Authorization: {auth_token}"'
        process = subprocess.run(command, shell=True, capture_output=True, text=True, encoding="utf-8", errors="ignore")

        if process.returncode == 0:
            response = process.stdout.strip()

            # 🔎 API dan kelgan javobni tekshirish uchun faylga yozamiz
            with open("api_debug.txt", "w", encoding="utf-8") as f:
                f.write(response)

            if response:
                print("✅ API dan ma'lumot olindi.")
                return response
            else:
                print("❌ API javobi bo‘sh!")
                return None
        else:
            print(f"❌ API xatolik: {process.stderr}")
            return None
    except Exception as e:
        print(f"❌ API istisno: {e}")
        return None


def save_to_csv(data, filename="GetAgroSellersTin.csv"):
    """CSV faylga saqlash."""
    if data:
        try:
            json_data = json.loads(data)
            if isinstance(json_data, list):
                df = pd.DataFrame(json_data)
                df.to_csv(filename, index=False, encoding="utf-8-sig")
                print(f"✅ CSV saqlandi: {filename}")
                return df
            else:
                print("❌ JSON format noto‘g‘ri (list emas)")
                return None
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode xatolik: {e}")
            return None
    else:
        print("❌ Ma'lumot yo‘q")
        return None


def save_to_db(df, table_name="AgroSellers"):
    """SQL Server bazasiga saqlash."""
    if df is None or df.empty:
        print("❌ DB uchun ma'lumot mavjud emas.")
        return

    try:
        df = df.dropna(subset=["tin", "Year"])
        df = df[~df["Year"].isin([float("inf"), float("-inf")])]
        df["Year"] = df["Year"].astype(int)

        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=192.168.111.14;"
            "DATABASE=weather;"
            "UID=sa;"
            "PWD=AX8wFfMQrR6b9qdhHt2eYS;"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )

        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute(f"""
                IF NOT EXISTS (SELECT 1 FROM {table_name} WHERE tin = ?)
                INSERT INTO {table_name} (tin, Year) VALUES (?, ?)
            """, row["tin"], row["tin"], row["Year"])

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Ma'lumotlar SQL Serverga saqlandi!")
    except Exception as e:
        print(f"❌ DB saqlashda xatolik: {e}")


# === MAIN ===
if __name__ == "__main__":
    api_url = "http://172.16.14.21:4041/GetAgroSellersTin"
    auth_token = "Credential Y3VzdG9tc1VzZXI6Q3UkdDBtc1BAdGh3b3Jk"

    response = fetch_data_with_curl(api_url, auth_token)
    df = save_to_csv(response)
    save_to_db(df)
