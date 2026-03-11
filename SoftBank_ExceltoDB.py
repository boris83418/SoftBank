import pandas as pd
import pyodbc
import logging
from sendEmail import Email
import datetime
import unicodedata

def normalize_to_halfwidth(text):
    """將文字轉為半型（包含特殊符號處理）"""
    if not isinstance(text, str):
        return text
    # 基本半型轉換
    text = unicodedata.normalize('NFKC', text)
    
    # 特殊符號處理（例：全形破折號、全形空白等）
    text = text.replace('－', '-')  # 全形破折號
    text = text.replace('　', ' ')  # 全形空白
    text = text.replace('‐', '-')   # 特殊 Hyphen U+2010 
    return text
 
def setup_logging():
    """設定日誌系統"""
    current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    fn = f"logfile_{current_datetime}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(fn, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return fn


def connect_to_database(server, database):
    """連接資料庫"""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes"
        )
        logging.info(f"成功連線到資料庫: {server}/{database}")
        return conn
    except Exception as e:
        logging.error(f"資料庫連線失敗: {e}")
        raise


def create_or_clear_table(cursor, table_name, column_mappings, sheet_name):
    """創建或清空表格"""
    try:
        cursor.execute(f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL SELECT 1 ELSE SELECT 0")
        table_exists = cursor.fetchone()[0]

        if table_exists == 0:
            # 建立表格
            sql = f"CREATE TABLE {table_name} (\n"
            
            # 為 Orderinfo 添加自動增長主鍵
            if sheet_name == 'Orderinfo':
                sql += "OrderinfoNumber INT IDENTITY(1,1) PRIMARY KEY,\n"
            
            # 添加其他欄位
            for excel_col, (db_col, db_type) in column_mappings[sheet_name].items():
                if (sheet_name == 'CustomerCode' and excel_col == 'ASP施工店') or \
                   (sheet_name == 'FactoryShipment' and excel_col == 'PartNo_ETA_FLTC') or \
                   (sheet_name == 'Productinfo' and excel_col == 'Delta_PartNO'):
                    sql += f"[{db_col}] {db_type} PRIMARY KEY,\n"
                else:
                    sql += f"[{db_col}] {db_type},\n"
            
            sql = sql.rstrip(',\n') + "\n);"
            cursor.execute(sql)
            logging.info(f"✓ {sheet_name} 表格建立完成")
        else:
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            logging.info(f"✓ {sheet_name} 表格資料已清除並重置流水號")

    except Exception as e:
        logging.error(f"處理表格 {table_name} 失敗: {e}")
        raise



def process_factory_shipment_data(df):
    """處理 FactoryShipment 特殊邏輯"""
    # 處理日期欄位
    date_columns = ['PO_Date', 'Actual_Ex_fac_date', 'ETD_SH', 'ETA_FLTC', 'Original_ETA']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 處理數量欄位
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce')
    
    # 填補 ETA_Year 空值
    df['ETA_Year'] = df['ETA_Year'].fillna(df['ETA_FLTC'].dt.year.astype(str))
    
    # 清理 Part_No（注意：這裡不再使用 normalize_text，因為 FactoryShipment 不在標準化清單中）
    df['Part_No'] = df['Part_No'].astype(str).str.strip()
    df['PartNo_ETA_FLTC'] = df['Part_No'].astype(str) + df['ETA_FLTC'].astype(str)
    
    # 按複合鍵分組合併數據
    return df.groupby('PartNo_ETA_FLTC', as_index=False).agg({
        'PO_Date': 'first', 'Item': 'first', 'Qty': 'sum', 'PO_NO': 'first',
        'Part_No': 'first', 'Actual_Ex_fac_date': 'first', 'ETD_SH': 'first',
        'ETA_FLTC': 'first', 'Original_ETA': 'first', 'ship_method': 'first',
        'ETA_Year': 'first', 'Status': 'first'
    })


def insert_data(cursor, table_name, df, column_mappings, sheet_name):
    # 準備欄位映射（排除自動增長欄位）
    valid_columns = [col for col in df.columns if col in column_mappings[sheet_name]]
    db_columns = [f"[{column_mappings[sheet_name][col][0]}]" for col in valid_columns]
    
    # 生成 INSERT 語句
    placeholders = ", ".join(["?"] * len(valid_columns))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(db_columns)}) VALUES ({placeholders})"
    
    success_count = 0
    error_count = 0
    
    for index, row in df.iterrows():
        # 準備資料（將 NaN 轉為 None，並統一轉半型）
        data = []
        for col in valid_columns:
            value = row[col]
            if pd.isna(value):
                data.append(None)
            elif isinstance(value, str):
                data.append(normalize_to_halfwidth(value.strip()))
            else:
                data.append(value)
                
        try:
            cursor.execute(insert_sql, tuple(data))
            success_count += 1
            
        except pyodbc.IntegrityError:
            error_count += 1
            logging.warning(f"❌ {sheet_name} 第 {index+1} 行：重複主鍵值")
            
        except Exception as e:
            error_count += 1
            logging.error(f"❌ {sheet_name} 第 {index+1} 行插入失敗: {e}")
            logging.error(f"   資料: {dict(zip(valid_columns, data))}")
    
    logging.info(f"✓ {sheet_name}: 成功插入 {success_count} 筆，失敗 {error_count} 筆")
    return success_count, error_count



def process_excel_to_sql(excel_file_path, table_mapping, column_mappings):
    """主處理函數"""
    conn = None
    cursor = None
    total_success = 0
    total_errors = 0
    
    try:
        conn = connect_to_database('jpdejitdev01', 'ITQAS2')
        cursor = conn.cursor()

        for sheet_name, table_name in table_mapping.items():
            logging.info(f"📊 處理工作表: {sheet_name}")
            
            # 讀取 Excel 資料
            df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
            
            # 特殊處理 FactoryShipment
            if sheet_name == 'FactoryShipment':
                df = process_factory_shipment_data(df)
            
            # 建立或清空表格
            create_or_clear_table(cursor, table_name, column_mappings, sheet_name)
            
            # 插入資料
            success, errors = insert_data(cursor, table_name, df, column_mappings, sheet_name)
            total_success += success
            total_errors += errors

        conn.commit()
        logging.info(f"🎉 處理完成！總計：成功 {total_success} 筆，失敗 {total_errors} 筆")

    except Exception as e:
        logging.error(f"💥 處理過程中出現錯誤: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
def check_log_error(log_filename):
    """檢查 log 檔是否有 ERROR 行"""
    with open(log_filename, encoding='utf-8') as f:
        for line in f:
            if "ERROR" in line or "❌" in line:
                return True
    return False

def send_notification_email(log_filename, is_error=False):
    """發送通知郵件，根據是否錯誤決定 email 內文"""
    try:
        sender_email = "SRV.ITREMIND.RBT@deltaww.com"
        password = "Dej1tasd"
        email = Email()
        subject = "SoftBank_Update_dataBase"

        # 讀取 log 內容（最多 100 行防止過大）
        with open(log_filename, encoding='utf-8') as f:
            log_lines = f.readlines()
            preview_log = ''.join(log_lines[-100:])  # 只取最後 100 行
        
        if is_error:
            body = (
                "💥 SoftBank 資料庫更新失敗！請參考以下錯誤記錄：\n\n"
                f"{preview_log}\n\n"
                "📎 詳細日誌已附加，請確認處理。"
            )
        else:
            body = "✅ SoftBank 資料庫更新完成，詳細記錄請參考附件。"

        for recipient in ['boris.wang@deltaww.com','GRACE.YC.HSU@deltaww.com','KAE.CHUNG@deltaww.com']:
            email.send_email(sender_email, password, recipient, subject, body, log_filename)
        
        logging.info("✉️ 通知郵件發送完成")
    except Exception as e:
        logging.error(f"📧 郵件發送失敗: {e}")


if __name__ == "__main__":
    # 設定日誌
    log_filename = setup_logging()
    
    # 配置參數
    excel_file_path = r'\\jpdejstcfs01\\STC_share\\JP IT\STC SBK 仕分けリスト\\IT system\\2025_SoftBank_deliverylist.xlsx'
    
    table_mapping = {
        'CustomerCode': 'dbo.SoftBank_Data_CustomerCode',
        'FactoryShipment': 'dbo.SoftBank_Data_FactoryShipment',
        'Orderinfo': 'dbo.SoftBank_Data_Orderinfo',
        'Productinfo': 'dbo.SoftBank_Data_Productinfo'
    }
    
    # 列映射配置
    column_mappings = {
        'CustomerCode': {
            'ASP施工店': ('ASP', 'NVARCHAR(255)'), 
            'Customer code': ('Customer_code', 'NVARCHAR(255)')
        },
        'FactoryShipment': {
            'PartNo_ETA_FLTC': ('PartNo_ETA_FLTC', 'NVARCHAR(255)'),
            'PO_Date': ('PO_Date', 'DATE'),
            'Item': ('Item', 'NVARCHAR(255)'),
            'PO_NO': ('PO_NO', 'NVARCHAR(255)'),
            'Part_No': ('Part_No', 'NVARCHAR(255)'),
            'Qty': ('Qty', 'INT'),
            'Actual_Ex_fac_date': ('Actual_Ex_fac_date', 'DATE'),
            'ETD_SH': ('ETD_SH', 'DATE'),
            'ETA_FLTC': ('ETA_FLTC', 'DATE'),
            'Original_ETA': ('Original_ETA', 'DATE'),
            'ship_method': ('ship_method', 'NVARCHAR(255)'),
            'ETA_Year': ('ETA_Year', 'NVARCHAR(255)'),
            'Status': ('Status', 'NVARCHAR(255)')
        },  
        'Orderinfo': {
            '注文書受領': ('Purchase_Order_Received', 'NVARCHAR(255)'),
            'Pull in 履歴': ('Pull_in_History', 'NVARCHAR(255)'),
            '見積書回答状況': ('Quotation_reply_status', 'NVARCHAR(255)'),
            '注文日': ('Order_Date', 'DATE'),
            'DEJ見積り番号': ('DEJ_Estimate_Number', 'NVARCHAR(255)'),
            '注文書': ('Quotation_status', 'NVARCHAR(255)'),
            '實際出荷日': ('Actual_Shipment_Date', 'DATE'),
            '預計出荷日': ('Estimated_Shipment_Date', 'DATE'),
            '納品日': ('Delivery_Date', 'DATE'),
            '希望納期': ('Desired_Delivery_Date', 'NVARCHAR(255)'),
            '標準納期': ('Standard_Delivery_Date', 'NVARCHAR(255)'),
            '工事名/局名': ('Station_Name', 'NVARCHAR(255)'),
            '品名・規格(PSI)': ('Product_Name_PSI', 'NVARCHAR(255)'),
            'SET': ('SET', 'NVARCHAR(255)'),
            'FOC/Option': ('FOC/Option', 'NVARCHAR(255)'),
            '品名・規格': ('Product_Name', 'NVARCHAR(255)'),
            '台数': ('Quantity', 'INT'),
            '発注先': ('OrdererLocation', 'NVARCHAR(255)'),
            '担当者': ('Person_in_Charge', 'NVARCHAR(255)'),
            '送り先': ('Recipient', 'NVARCHAR(255)'),
            '連絡人': ('Contact_Person', 'NVARCHAR(255)'),
            '住所': ('Contact_Address', 'NVARCHAR(255)'),
            '電話': ('ContactPhone', 'NVARCHAR(255)'),
            '註': ('ContactNotes', 'NVARCHAR(255)'),
            'SO＃': ('SO_NO', 'NVARCHAR(255)'),
            'DN＃': ('DN_NO', 'NVARCHAR(255)'),
            'CustomerCode': ('CustomerCode', 'NVARCHAR(255)'),
            '單價': ('Unitprice', 'NVARCHAR(255)'),
            '見積り＄(請求税抜き)': ('QuotationPrice', 'NVARCHAR(255)'),
            '見積り＄(請求税込み)': ('QuotationPrice_with_tax', 'NVARCHAR(255)'),
            '送り状番号': ('Invoice_Number', 'NVARCHAR(255)')
        },
        'Productinfo': {
            'Delta_PartNO': ('Delta_PartNO', 'NVARCHAR(255)'),
            'REMARK': ('Remark', 'NVARCHAR(255)'),
            'Category': ('Category', 'NVARCHAR(255)'),
            '1SET10PCS': ('1SET10PCS', 'NVARCHAR(255)'),
            'Customer_Model_Name': ('Customer_Model_Name', 'NVARCHAR(255)'),
            'Model': ('Model', 'NVARCHAR(255)'),
            '税抜単価': ('UnitPrice', 'INT'),
            '標準納期': ('Standard_Delivery_Time', 'INT'),
            '月末SAP庫存': ('Month-End_SAP_Inventory', 'INT')
        }
    }

    # 執行主程式
    try:
        logging.info("🚀 開始處理 SoftBank 資料庫更新（強化全角半角標準化版本）")
        process_excel_to_sql(excel_file_path, table_mapping, column_mappings)
        has_error = check_log_error(log_filename)
        send_notification_email(log_filename, is_error=has_error)

    except Exception as e:
        logging.error(f"💥 程式執行失敗: {e}")
        send_notification_email(log_filename, is_error=True)
