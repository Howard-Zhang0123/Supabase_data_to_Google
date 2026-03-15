import os
import json
import psycopg2
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
import time

# ==========================================
# 1. 配置區：維護客戶與其對應的 Google Sheet
# ==========================================
CLIENT_CONFIGS = [
    {
        "schema": "Junior",
        "spreadsheet_id": "1RViVJm5cZ7BntUme9aLFRrsZv6mc9gJGrhymtTRKzXc",
        "views": ["Junior_campaign_view", "Junior_placement_view", "Junior_search_view"]
    },
    {
        "schema": "HowCool",
        "spreadsheet_id": "HOWCOOL_SHEET_ID_HERE",
        "views": ["HowCool_campaign_view", "HowCool_placement_view", "HowCool_search_view"]
    },
    {
        "schema": "Ksgreen",
        "spreadsheet_id": "KSGREEN_SHEET_ID_HERE",
        "views": ["Ksgreen_campaign_view", "Ksgreen_placement_view", "Ksgreen_search_view"]
    }
]

# ==========================================
# 2. 定義各 View 的比對維度 (用於處理歸因覆蓋)
# ==========================================
def get_merge_keys(view_name):
    # 根據 view 類型返回對應的維度欄位組合
    if "campaign_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Country", "Currency", "Targeting_Type", "Bidding_strategy"]
    elif "search_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Placement", "Country", "Currency", "Bidding_strategy"]
    elif "placement_view" in view_name:
        return ["ad_type", "Date", "Portfolio", "Campaign", "Targeting", "Ad_Group", "Match_Type", "Search_Term", "Country", "Currency"]
    return None

def sync_data():
    # --- 讀取機密資訊 ---
    # 這裡使用寫死的 IPv4 Pooler 地址進行測試
    db_host = "aws-1-ap-southeast-1.pooler.supabase.com"
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_pass = os.getenv("DB_PASS")
    
    # 讀取 JSON 金鑰
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
        # ⚠️ 重要修正：使用 Pooler Host 時，Port 必須是 6543
        conn = psycopg2.connect(
            host=db_host, 
            database=db_name, 
            user=db_user, 
            password=db_pass, 
            port=6543,
            connect_timeout=10
        )
        print("✅ 資料庫連線成功 (使用 Port 6543)")
    except Exception as e:
        print(f"❌ 無法連接資料庫: {e}")
        return

    try:
        cur = conn.cursor()
        for config in CLIENT_CONFIGS:
            schema = config["schema"]
            sheet_id = config["spreadsheet_id"]
            
            print(f"\n--- 正在處理客戶: {schema} ---")
            
            try:
                sh = gc.open_by_key(sheet_id)
            except Exception as e:
                print(f"⚠️ 無法開啟試算表 {sheet_id}: {e}")
                continue

            for view_name in config["views"]:
                print(f"🔄 正在同步: {view_name}")
                
                # 1. 從 Supabase 抓取最新資料
                query = f'SELECT * FROM "{schema}"."{view_name}";'
                df_new = pd.read_sql(query, conn)
                
                if df_new.empty:
                    print(f"ℹ️ {view_name} 沒有資料，跳過。")
                    continue
                
                # 確保 Date 欄位是字串，方便比對
                if 'Date' in df_new.columns:
                    df_new['Date'] = df_new['Date'].astype(str)

                # 2. 取得或建立工作表
                try:
                    worksheet = sh.worksheet(view_name)
                    existing_data = worksheet.get_all_records()
                    df_old = pd.DataFrame(existing_data)
                    # 舊資料的 Date 也轉字串確保一致
                    if not df_old.empty and 'Date' in df_old.columns:
                        df_old['Date'] = df_old['Date'].astype(str)
                except gspread.exceptions.WorksheetNotFound:
                    worksheet = sh.add_worksheet(title=view_name, rows="1000", cols="26")
                    df_old = pd.DataFrame()

                # 3. 執行覆蓋邏輯 (Merge & De-duplicate)
                merge_keys = get_merge_keys(view_name)
                
                if not df_old.empty and merge_keys:
                    # 檢查 merge_keys 是否都存在於 columns 中，避免報錯
                    valid_keys = [k for k in merge_keys if k in df_new.columns and k in df_old.columns]
                    
                    # 合併新舊資料，新資料排在後
                    df_combined = pd.concat([df_old, df_new], ignore_index=True)
                    # 保留最後出現的一筆（即資料庫最新抓取的資料）
                    df_final = df_combined.drop_duplicates(subset=valid_keys, keep='last')
                else:
                    df_final = df_new

                # 4. 寫回 Google Sheet
                worksheet.clear()
                # 轉換資料格式為列表並處理 NaN
                data_to_write = [df_final.columns.values.tolist()] + df_final.fillna("").values.tolist()
                
                # 更新 A1 範圍
                worksheet.update('A1', data_to_write)
                
                print(f"✅ {view_name} 同步完成，總筆數: {len(df_final)}")
                time.sleep(1) 

    except Exception as e:
        print(f"❌ 執行過程中發生錯誤: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("🔌 資料庫連線已關閉")

if __name__ == "__main__":
    sync_data()
