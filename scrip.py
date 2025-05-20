from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import pyodbc
import time
from datetime import datetime

# Selenium WebDriver sozlash
options = Options()
options.add_argument("--headless")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Sahifani ochish
url = "https://www.investing.com/commodities/us-cotton-no.2-historical-data"
driver.get(url)
time.sleep(5)

# Cookie popupni yopish
try:
    driver.find_element(By.ID, "onetrust-accept-btn-handler").click()
    time.sleep(2)
except:
    pass

# Scroll pastga
driver.execute_script("window.scrollTo(0, 800)")
time.sleep(2)

# Sahifa HTML source'ni olish
html = driver.page_source
driver.quit()

# Pandas orqali table'ni olish
tables = pd.read_html(html)
df = tables[0]  # Birinchi jadval (asosiy historical table)

# CSV faylga saqlash
df.to_csv('us_cotton_historical_data.csv', index=False, encoding='utf-8-sig')
print("📁 CSV saqlandi: us_cotton_historical_data.csv")

# Faqat 1-qatorni olish va ustun nomlarini standartlashtirish
df.columns = ['Date', 'Price', 'Open', 'High', 'Low', 'Volume', 'ChangePercent']
row = df.iloc[0]

# Sana formatlash
try:
    parsed_date = datetime.strptime(row['Date'], '%b %d, %Y')
    formatted_date = parsed_date.strftime('%m/%d/%Y')
except Exception as e:
    print(f"❌ Sana formati xato: {e}")
    formatted_date = row['Date']

# SQL Serverga yozish
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=192.168.111.14,1433;'
    'DATABASE=QXV;'
    'UID=Shokhjahon;'
    'PWD=#Shohjahon03;'
)
cursor = conn.cursor()

# Sana mavjudligini tekshirish
cursor.execute("SELECT COUNT(*) FROM StockPrices WHERE [Date] = ?", formatted_date)
exists = cursor.fetchone()[0]

if exists == 0:
    cursor.execute("""
        INSERT INTO StockPrices ([Date], [Price], [Open], [High], [Low], [Volume], [ChangePercent])
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, formatted_date, row['Price'], row['Open'], row['High'], row['Low'], row['Volume'], row['ChangePercent'])
    conn.commit()
    print(f"✅ Sana {formatted_date} qo‘shildi.")
else:
    print(f"⚠️ Sana {formatted_date} allaqachon mavjud.")

cursor.close()
conn.close()
