import os
import io
import time
import shutil
import requests
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import gspread
from oauth2client.service_account import ServiceAccountCredentials

########################################
# 1. Chrome設定
########################################

user_data_dir = "/tmp/chrome-user-data"
if os.path.exists(user_data_dir):
    shutil.rmtree(user_data_dir)
os.makedirs(user_data_dir, exist_ok=True)

CUSTOM_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument(f"--user-data-dir={user_data_dir}")
options.add_argument(f"user-agent={CUSTOM_USER_AGENT}")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

########################################
# 2. affi-plusログイン
########################################

MAIL = os.getenv("BPX_MAIL")
PASSWORD = os.getenv("BPX_PASSWORD")
if not MAIL or not PASSWORD:
    raise ValueError("BPX_MAIL / BPX_PASSWORD が未設定")

driver.get("https://affi-plus.com/contents.php?c=user_login")
time.sleep(3)

driver.find_element(By.NAME, "mail").send_keys(MAIL)
driver.find_element(By.NAME, "pass").send_keys(PASSWORD)
driver.find_element(By.XPATH, "//input[@type='submit']").click()
time.sleep(5)

########################################
# 3. Seleniumのcookieをrequestsへ移植
########################################

session = requests.Session()

for cookie in driver.get_cookies():
    session.cookies.set(cookie["name"], cookie["value"])

session.headers.update({
    "User-Agent": CUSTOM_USER_AGENT
})

########################################
# 4. CSVをPOSTで直接ダウンロード（過去30日）
########################################

# 検索条件付きでエクスポートページにアクセス（セッションに条件をセット）
export_page_url = (
    "https://affi-plus.com/page.php?p=action_log_rawExport&tab_type=3"
    "&regist_unix_type=-30d"
    "&apply_unix_type=all"
    "&TB_iframe=true"
)
session.get(export_page_url)

# CSVダウンロード
csv_url = "https://affi-plus.com/csv.php?module=action_log_raw&run=s_export&tab_type=3&TB_iframe=true"
response = session.post(csv_url, data={"csv_encoding": "UTF-8"})

if response.status_code != 200:
    raise Exception("CSV取得失敗")

print("CSV取得成功")

########################################
# 5. CSV → DataFrame
########################################

csv_data = pd.read_csv(io.StringIO(response.text))
csv_data = csv_data.fillna("")
csv_data = csv_data.iloc[:, :16]   # A〜P列（16列）

########################################
# 6. Google Sheets接続
########################################

GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS")
if GOOGLE_CREDENTIALS is None:
    raise ValueError("GOOGLE_CREDENTIALS が環境変数に設定されていません。")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
if SPREADSHEET_ID is None:
    raise ValueError("SPREADSHEET_ID が環境変数に設定されていません。")

credentials_path = "/tmp/credentials.json"
with open(credentials_path, "w") as f:
    f.write(GOOGLE_CREDENTIALS)

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
client = gspread.authorize(creds)

sheet = client.open_by_key(SPREADSHEET_ID).worksheet("affi-plus_成果結果リスト")  

########################################
# 7. A列重複削除
########################################

# A列のみ取得して最終行と既存IDを判定
col_a = sheet.col_values(1)  # A列の値リスト（ヘッダー含む）

if len(col_a) > 1:
    existing_ids = set(col_a[1:])  # ヘッダー除外
    last_row = len(col_a)          # A列の最終行
else:
    existing_ids = set()
    last_row = 1                   # ヘッダーのみ

# A列で重複除外
filtered_df = csv_data[~csv_data.iloc[:, 0].astype(str).isin(existing_ids)]

if filtered_df.empty:
    print("新規データなし")
else:
    start_row = last_row + 1
    end_row = start_row + len(filtered_df) - 1

    # 行数が足りなければ追加
    if end_row > sheet.row_count:
        sheet.add_rows(end_row - sheet.row_count)

    # A〜P列のみ書き込み（Q列以降の計算式を上書きしない）
    range_name = f"A{start_row}:P{end_row}"

    sheet.update(
        range_name=range_name,
        values=filtered_df.values.tolist(),
        value_input_option="USER_ENTERED"
    )

    print("スプレッドシート更新完了")

driver.quit()
