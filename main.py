import os
import json
import psycopg2
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import time

# ==========================================
# 1. 配置區：客戶名稱對照表與其 Google Sheet ID
# ==========================================
# 請在下面填入各客戶對應的 Google Sheet ID
CLIENT_CONFIGS = [
    {"schema": "Junior", "spreadsheet_id": "1RViVJm5cZ7BntUme9aLFRrsZv6mc9gJGrhymtTRKzXc"},
    {"schema": "MorningBlues", "spreadsheet_id": "1-gvuQGo88AI6-ma71wcR4OJTu7bc5IQ60GSq3kTBzpM"},
    {"schema": "Milock(VC)", "spreadsheet_id": "1HY5UNSClrQSc_JbesGE8VUmExIqz8p8SNGMF5JplI90"},
    {"schema": "Milock(SC)", "spreadsheet_id": "1NjiVDH3tLBYCfZvIHWt0_6W9eKNyBQpT-IyPh1BNOrg"},
    {"schema": "Ksgreen", "spreadsheet_id": "1yg5GOudfOGDap9udQwBcRlGMV9KWlIjMcx3xD-b94S8"},
    {"schema": "MOAPLAY", "spreadsheet_id": "1yUwDp0VNfokttPaZ_ZH0Xr7jR5YqGHJCamCaJbOobAc"},
    {"schema": "ShowLai", "spreadsheet_id": "填入_ID"},
    {"schema": "ShanShui", "spreadsheet_id": "1GIifS1zFcCwjsfhKXoqMVX3FVU3XNBn1WP2vavLrpow"},
    {"schema": "Ductech", "spreadsheet_id": "1bn4dQVq1QtyvlV6l-Wy6KmfDQP87CNc5uxgI-mev90I"},
    {"schema": "HowCool", "spreadsheet_id": "1FJLN3Ow_xblg9WFComyTSxrBMmyhPEyRSLjcKBMhtsc"}
]

# 每位客戶預設要同步的 View 名稱
VIEW_TEMPLATES = ["_campaign_view", "_placement_view", "_search_view"]

# ==========================================
# 2. 定義各 View 的比對維度 (用於處理歸因覆蓋)
# ==========================================
def get_merge_keys(view_name):
    if "campaign_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Country", "Currency", "Targeting_Type", "Bidding_strategy"]
    elif "search_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Placement", "Country", "Currency", "Bidding_strategy"]
    elif "placement_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Targeting", "Ad_Group", "Match_Type", "Search_Term", "Country", "Currency"]
    return None

def sync_data():
    # --- 讀取機密資訊 (從 GitHub Secrets 讀取) ---
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

    # --- 連接 Supabase ---
    try:
        conn = psycopg2.connect(
            host=db_host, 
            database=db_name, 
            user=db_user, 
            password=db_pass, 
            port=6543,
            connect_timeout=10
        )
        print(f"✅ 資料庫連線成功 (Host: {db_host})")
    except Exception as e:
        print(f"❌ 無法連接資料庫: {e}")
        return

    try:
        for config in CLIENT_CONFIGS:
            schema = config["schema"]
            sheet_id = config["spreadsheet_id"]
            
            if "填入_ID" in sheet_id or not sheet_id:
                continue

            print(f"\n🚀 [開始處理] 客戶: {schema}")
            
            try:
                sh = gc.open_by_key(sheet_id)
            except Exception as e:
                print(f"⚠️ 無法開啟 {schema} 的試算表: {e}")
                continue

            # 根據客戶名稱動態生成 View 清單 (例如 Junior_campaign_view)
            views_to_sync = [f"{schema}{suffix}" for suffix in VIEW_TEMPLATES]

            for view_name in views_to_sync:
                print(f"  🔄 同步中: {view_name}")
                
                try:
                    # 1. 抓取資料 (加上 try 避免單一 View 失敗卡死全部)
                    query = f'SELECT * FROM "{schema}"."{view_name}";'
                    df_new = pd.read_sql(query, conn)
                    
                    if df_new.empty:
                        print(f"    ℹ️ 無資料，跳過。")
                        continue
                    
                    if 'Date' in df_new.columns:
                        df_new['Date'] = df_new['Date'].astype(str)

                    # 2. 取得或建立工作表
                    try:
                        worksheet = sh.worksheet(view_name)
                        existing_data = worksheet.get_all_records()
                        df_old = pd.DataFrame(existing_data)
                        if not df_old.empty and 'Date' in df_old.columns:
                            df_old['Date'] = df_old['Date'].astype(str)
                    except gspread.exceptions.WorksheetNotFound:
                        worksheet = sh.add_worksheet(title=view_name, rows="1000", cols="26")
                        df_old = pd.DataFrame()

                    # 3. 執行去重覆蓋
                    merge_keys = get_merge_keys(view_name)
                    if not df_old.empty and merge_keys:
                        valid_keys = [k for k in merge_keys if k in df_new.columns and k in df_old.columns]
                        df_combined = pd.concat([df_old, df_new], ignore_index=True)
                        df_final = df_combined.drop_duplicates(subset=valid_keys, keep='last')
                    else:
                        df_final = df_new

                    # 4. 寫回
                    worksheet.clear()
                    data_to_write = [df_final.columns.values.tolist()] + df_final.fillna("").values.tolist()
                    worksheet.update('A1', data_to_write)
                    
                    print(f"    ✅ 同步完成 (總筆數: {len(df_final)})")
                    time.sleep(0.5) 

                except Exception as e:
                    print(f"    ❌ {view_name} 失敗: {e}")

    finally:
        if 'conn' in locals():
            conn.close()
            print("\n🔌 所有任務完成，連線已關閉。")

if __name__ == "__main__":
    sync_data()
