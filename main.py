import os
import json
import psycopg2
import gspread
from google.oauth2.service_account import Credentials

# ==========================================
# 1. 配置區：未來新增客戶只需在 CLIENT_CONFIGS 加一組字典
# ==========================================
CLIENT_CONFIGS = [
    {
        "schema": "Junior",
        "spreadsheet_id": "1RViVJm5cZ7BntUme9aLFRrsZv6mc9gJGrhymtTRKzXc",
        "views": [
            "Junior_campaign_view",
            "Junior_placement_view",
            "Junior_search_view"
        ]
    }, 
    {
        "schema": "HowCool",
        "spreadsheet_id": "HOWCOOL_SHEET_ID_HERE",
        "views": [
            "HowCool_campaign_view",
            "HowCool_placement_view",
            "HowCool_search_view"
        ]
    }, 
    {
        "schema": "Ksgreen",
        "spreadsheet_id": "KSGREEN_SHEET_ID_HERE",
        "views": [
            "Ksgreen_campaign_view",
            "Ksgreen_placement_view",
            "Ksgreen_search_view"
        ]
    }
]

def sync_data():
    # 從 GitHub Secrets 讀取機密資訊
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    # 讀取 JSON 金鑰字串
    service_account_info = json.loads(os.getenv("G_SERVICE_ACCOUNT_JSON"))

    # 初始化 Google Sheets 客戶端
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    client = gspread.authorize(creds)

    # 連接 Supabase (PostgreSQL)
    conn = psycopg2.connect(
        host=db_host, database=db_name, user=db_user, password=db_pass, port=5432
    )
    cur = conn.cursor()

    try:
        for config in CLIENT_CONFIGS:
            schema = config["schema"]
            sheet_id = config["spreadsheet_id"]
            views = config["views"]

            print(f"--- 正在處理 Schema: {schema} ---")
            sh = client.open_by_key(sheet_id)

            for view_name in views:
                print(f"正在同步 View: {view_name}...")
                
                # 1. 從 Supabase 抓取資料
                query = f'SELECT * FROM "{schema}"."{view_name}";'
                cur.execute(query)
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]

                # 2. 準備寫入資料 (標題 + 內容)
                data_to_write = [colnames] + [list(row) for row in rows]

                # 3. 尋找或建立對應的工作表 (Tab)
                try:
                    worksheet = sh.worksheet(view_name)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = sh.add_worksheet(title=view_name, rows="100", cols="20")

                # 4. 清除舊資料並寫入新資料
                worksheet.clear()
                worksheet.update('A1', data_to_write)
                print(f"View: {view_name} 同步完成！")

    except Exception as e:
        print(f"發生錯誤: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    sync_data()
