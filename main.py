import os
import json
import psycopg2
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import time

# ==========================================
# 1. 配置區：單一整合表 ID 與客戶清單
# ==========================================
# 這是你唯一的整合試算表 ID
TARGET_SPREADSHEET_ID = "1RViVJm5cZ7BntUme9aLFRrsZv6mc9gJGrhymtTRKzXc" 

# 客戶 Schema 清單 (對應 Supabase 的 Schema 名稱)
CLIENT_SCHEMAS = [
    "Junior", "MorningBlues", "Milock(VC)", "Milock(SC)", 
    "Ksgreen", "MOAPLAY", "ShowLai", "ShanShui", "Ductech", "HowCool"
]

# 每位客戶要同步的 View 模板
VIEW_TEMPLATES = ["_campaign_view", "_placement_view", "_search_view"]

# ==========================================
# 2. 定義各 View 的比對維度 (用於處理歸因覆蓋)
# ==========================================
def get_merge_keys(view_name):
    # 使用 in 來判斷，因為現在工作表名稱會包含客戶名
    if "campaign_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Country", "Currency", "Targeting_Type", "Bidding_strategy"]
    elif "search_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Placement", "Country", "Currency", "Bidding_strategy"]
    elif "placement_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Targeting", "Ad_Group", "Match_Type", "Search_Term", "Country", "Currency"]
    return None

def sync_data():
    # --- 讀取機密資訊 ---
    db_host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    
    g_json = os.getenv("G_SERVICE_ACCOUNT_JSON")
    if not g_json:
        print("❌ 錯誤：找不到 G_SERVICE_ACCOUNT_JSON")
        return
    service_account_info = json.loads(g_json)

    # --- 初始化 Google Sheets ---
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    gc = gspread.authorize(creds)

    # --- 開啟整合試算表 ---
    try:
        sh = gc.open_by_key(TARGET_SPREADSHEET_ID)
        print(f"✅ 成功開啟整合試算表: {sh.title}")
    except Exception as e:
        print(f"❌ 無法開啟整合試算表: {e}")
        return

    # --- 連接 Supabase ---
    try:
        conn = psycopg2.connect(
            host=db_host, database=db_name, user=db_user, password=db_pass, 
            port=6543, connect_timeout=10
        )
        print(f"✅ 資料庫連線成功")
    except Exception as e:
        print(f"❌ 無法連接資料庫: {e}")
        return

    try:
        for schema in CLIENT_SCHEMAS:
            print(f"\n🚀 [正在處理] 客戶: {schema}")
            
            # 建立該客戶的所有 View
            for suffix in VIEW_TEMPLATES:
                db_view_name = f"{schema}{suffix}" # 資料庫裡的 View 名稱
                sheet_tab_name = f"{schema}{suffix}" # Google Sheet 裡的分頁名稱
                
                print(f"  🔄 同步中: {sheet_tab_name}")
                
                try:
                    # 1. 抓取資料
                    query = f'SELECT * FROM "{schema}"."{db_view_name}";'
                    df_new = pd.read_sql(query, conn)
                    
                    if df_new.empty:
                        print(f"    ℹ️ 無資料，跳過。")
                        continue
                    
                    if 'Date' in df_new.columns:
                        df_new['Date'] = df_new['Date'].astype(str)

                    # 2. 取得或建立工作表分頁
                    try:
                        worksheet = sh.worksheet(sheet_tab_name)
                        existing_data = worksheet.get_all_records()
                        df_old = pd.DataFrame(existing_data)
                        if not df_old.empty and 'Date' in df_old.columns:
                            df_old['Date'] = df_old['Date'].astype(str)
                    except gspread.exceptions.WorksheetNotFound:
                        # 建立新分頁
                        worksheet = sh.add_worksheet(title=sheet_tab_name, rows="1000", cols="26")
                        df_old = pd.DataFrame()

                    # 3. 執行去重覆蓋
                    merge_keys = get_merge_keys(db_view_name)
                    if not df_old.empty and merge_keys:
                        valid_keys = [k for k in merge_keys if k in df_new.columns and k in df_old.columns]
                        df_combined = pd.concat([df_old, df_new], ignore_index=True)
                        df_final = df_combined.drop_duplicates(subset=valid_keys, keep='last')
                    else:
                        df_final = df_new

                    # 4. 寫回 (轉換 NaN 並包含標題)
                    worksheet.clear()
                    data_to_write = [df_final.columns.values.tolist()] + df_final.fillna("").values.tolist()
                    worksheet.update('A1', data_to_write)
                    
                    print(f"    ✅ 完成 (筆數: {len(df_final)})")
                    time.sleep(0.6) # 稍微增加延遲避免觸發 Google API 頻率限制

                except Exception as e:
                    print(f"    ❌ 失敗: {e}")
                    conn.rollback() # 發生錯誤時回滾，避免 Transaction 鎖死

    finally:
        if 'conn' in locals():
            conn.close()
            print("\n🔌 所有客戶同步完畢，連線已關閉。")

if __name__ == "__main__":
    sync_data()
