from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import pyodbc
import time
from datetime import datetime

# Setup Chrome WebDriver using webdriver-manager
options = Options()
options.add_argument("--headless")  # Fon rejimida ishlaydi, brauzer ko‘rinmaydi
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Ochiladigan URL
url = "https://www.investing.com/commodities/us-cotton-no.2-historical-data"
driver.get(url)
time.sleep(5)

# Cookie popupni yopish
try:
    accept = driver.find_element(By.ID, "onetrust-accept-btn-handler")
    accept.click()
    time.sleep(2)
except:
    pass

# Scroll pastga
driver.execute_script("window.scrollTo(0, 800)")
time.sleep(2)

# Sahifani o‘qish
soup = BeautifulSoup(driver.page_source, 'html.parser')
table = soup.find('table', {'class': 'freeze-column-w-1'})
print(table, "table")

# Headerlar
headers = [header.text for header in table.find_all('th')]

# Qatorlarni yig‘ish
rows = []
for tr in table.find_all('tr')[1:]:
    cells = tr.find_all('td')
    row = [cell.text.strip() for cell in cells]
    if row:
        rows.append(row)

# CSV saqlash
df = pd.DataFrame(rows, columns=headers)
df.to_csv('us_cotton_historical_data.csv', index=False)
print("📁 CSV saqlandi: us_cotton_historical_data.csv")

# Brauzerni yopish
driver.quit()

# CSV fayldan faqat 1-qatorni olish
df = pd.read_csv('us_cotton_historical_data.csv', nrows=1)
df.columns = ['Date', 'Price', 'Open', 'High', 'Low', 'Volume', 'ChangePercent']
row = df.iloc[0]

# Sanani formatlash
try:
    parsed_date = datetime.strptime(row['Date'], '%b %d, %Y')
    formatted_date = parsed_date.strftime('%m/%d/%Y')
except Exception as e:
    print(f"Date formatting failed: {e}")
    formatted_date = row['Date']

# SQL Serverga ulanish
conn = pyodbc.connect(
    'DRIVER={SQL Server};'
    'SERVER=192.168.111.14,1433;'
    'DATABASE=QXV;'
    'UID=Shokhjahon;'
    'PWD=#Shohjahon03;'
)
cursor = conn.cursor()

# Ushbu sana mavjudmi?
cursor.execute("SELECT COUNT(*) FROM StockPrices WHERE [Date] = ?", formatted_date)
exists = cursor.fetchone()[0]

# Yo‘qligini tekshirib yozish
if exists == 0:
    cursor.execute("""
        INSERT INTO StockPrices ([Date], [Price], [Open], [High], [Low], [Volume], [ChangePercent])
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, formatted_date, row['Price'], row['Open'], row['High'], row['Low'], row['Volume'], row['ChangePercent'])
    conn.commit()
    print(f"✅ Yangi sana qo‘shildi: {formatted_date}")
else:
    print(f"⚠️ Sana {formatted_date} allaqachon mavjud. Qo‘shilmadi.")

cursor.close()
conn.close()
