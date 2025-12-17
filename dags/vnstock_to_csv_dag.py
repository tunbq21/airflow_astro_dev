from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from vnstock import Vnstock 
import os

# --- 1. Cấu hình DAG và Biến Cục bộ ---

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 12),
    'email': ['tbuiquang103@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Danh sách cổ phiếu ví dụ
TICKERS = ['FPT', 'HPG', 'VCB', 'GAS', 'VNM', 'MSN', 'MWG', 'VPB', 'TCB', 'ACB']

# ĐƯỜNG DẪN TỐI ƯU CHO ASTRO/DOCKER (Ánh xạ đến thư mục 'data_lake' trên máy tính)
BASE_DATA_PATH = '/usr/local/airflow/dags/data_lake/vnstock_prices_csv' 

with DAG(
    'vnstock_to_csv_etl',
    default_args=default_args,
    description='ETL giá chứng khoán VN30 từ vnstock và lưu vào CSV Data Lake',
    schedule='0 17 * * 1-5',  # Chạy lúc 17:00 từ T2 đến T6
    catchup=False,
    tags=['finance', 'vnstock', 'csv'],
) as dag:
    
    # --- 2. Tác vụ 1: Tải dữ liệu giá đóng cửa (Extract) ---
    def extract_close_price(**kwargs):
        """Tải giá lịch sử (O, H, L, C, V) của ngày được chỉ định."""
        
        execution_date = kwargs['ds'] 
        data = []
        
        # Khởi tạo đối tượng Vnstock MỘT LẦN (có thể tối ưu hóa hơn nữa, nhưng cách này hoạt động)
        vnstock_api = Vnstock()
        
        for ticker in TICKERS:
            try:
                # 1. Định nghĩa mã cổ phiếu và nguồn dữ liệu (VCI là nguồn ổn định)
                stock_obj = vnstock_api.stock(symbol=ticker, source="VCI")
                
                # 2. Gọi phương thức history()
                df = stock_obj.quote.history(
                    start=execution_date, 
                    end=execution_date, 
                    interval="1D" # Lấy dữ liệu theo ngày
                )
                
                if not df.empty:
                    # =================================================================
                    # 👉 PHẦN BỔ SUNG: CHUẨN HÓA DATAFRAME TỪ VNSTOCK
                    # =================================================================
                    
                    # 1. Đảm bảo tên cột Ngày là 'Date' (vnstock mới thường trả về 'time' hoặc 'TradingDate')
                    if 'time' in df.columns:
                        df.rename(columns={'time': 'Date'}, inplace=True)
                    elif 'TradingDate' in df.columns:
                        df.rename(columns={'TradingDate': 'Date'}, inplace=True)
                    
                    # 2. Chuẩn hóa các cột OHLCV (nếu cần). Ví dụ: đổi 'ClosePrice' thành 'Close'
                    # Nếu bạn phát hiện tên cột khác, hãy thêm vào đây:
                    # df.rename(columns={'ClosePrice': 'Close', 'TradingVolume': 'Volume'}, inplace=True)
                    
                    # 3. Chuyển cột Date (time) sang định dạng YYYY-MM-DD string
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                    
                    # 4. In tên cột để gỡ lỗi (DEBUGGING)
                    print(f"DEBUG: Các cột DF trước khi XCom: {df.columns.tolist()}") 
                    
                    # =================================================================
                    
                    # Lấy hàng cuối cùng (hoặc hàng duy nhất nếu là 1 ngày)
                    latest_row = df.iloc[-1].to_dict() 
                    
                    # Thêm cột Ticker 
                    latest_row['Ticker'] = ticker
                    # Dòng kiểm tra Date cũ có thể được đơn giản hóa vì chúng ta đã chuẩn hóa ở trên:
                    if 'Date' not in latest_row:
                        latest_row['Date'] = execution_date 
                    
                    data.append(latest_row)
                    print(f"✅ Tải thành công {ticker} cho ngày {execution_date}")
                else:
                    print(f"⚠️ Không có dữ liệu cho {ticker} vào ngày {execution_date}")
            except Exception as e:
                print(f"❌ Lỗi khi tải {ticker}: {e}")
                
        final_df = pd.DataFrame(data)
        # In ra tên cột của DataFrame cuối cùng được gửi qua XCom
        print(f"Tên cột DataFrame cuối cùng: {final_df.columns.tolist()}") 
        
        # XCom Push: Trả về DataFrame chứa dữ liệu thô
        return final_df # Trả về final_df đã được chuẩn hóa
    
    extract_task = PythonOperator(
        task_id='extract_close_price',
        python_callable=extract_close_price,
        do_xcom_push=True,
    )

    # (Các Tác vụ 3 và 4 không cần sửa đổi vì chúng chỉ xử lý DataFrame từ Tác vụ 1)
    # --- 3. Tác vụ 2: Lưu trữ vào CSV Data Lake (Load) ---
    def load_to_csv_data_lake(**kwargs):
        """Lấy dữ liệu từ XCom và lưu dưới dạng CSV phân vùng."""
        
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_close_price')
        
        if df is None or df.empty:
            print("❌ Không có dữ liệu để lưu trữ. Bỏ qua.")
            return

        # Chuẩn bị cột phân vùng
        # *Chú ý: Tùy thuộc vào tên cột ngày/giờ mà Vnstock trả về, bạn có thể cần đổi 'Date' thành 'time'
        # Tôi giữ nguyên 'Date' để phù hợp với logic code cũ, nhưng bạn có thể cần kiểm tra lại tên cột thực tế.
        date_column = 'Date' # Tên cột chứa ngày. Nếu vnstock trả về là 'time', hãy đổi thành 'time'
        
        # Chuyển cột ngày thành định dạng datetime nếu chưa phải
        df[date_column] = pd.to_datetime(df[date_column]) 
        
        df['year'] = df[date_column].dt.year
        df['month'] = df[date_column].dt.month
        
        rows_saved = 0
        
        # Phân vùng và lưu trữ
        for ticker in df['Ticker'].unique():
            df_ticker = df[df['Ticker'] == ticker]
            
            # Định dạng đường dẫn theo phân vùng (ticker/year/month/date.csv)
            date_str = df_ticker[date_column].iloc[0].strftime('%Y-%m-%d')
            year = df_ticker['year'].iloc[0]
            month = df_ticker['month'].iloc[0]

            save_dir = os.path.join(
                BASE_DATA_PATH, 
                ticker, 
                str(year), 
                str(month).zfill(2) # Đảm bảo định dạng 01, 02...
            )
            save_path = os.path.join(save_dir, f"{date_str}.csv")
            
            # Tạo thư mục và Lưu trữ
            os.makedirs(save_dir, exist_ok=True)
            df_ticker.to_csv(save_path, index=False)
            print(f"💾 Lưu trữ thành công {ticker} tại {save_path}")
            rows_saved += len(df_ticker)
        
        # Trả về số lượng dòng đã lưu để Tác vụ 3 kiểm tra
        return rows_saved

    load_task = PythonOperator(
        task_id='load_to_csv_data_lake',
        python_callable=load_to_csv_data_lake,
    )
    
    # --- 4. Tác vụ 3: Kiểm tra Chất lượng Dữ liệu (DQ Check) ---
    # --- 4. Tác vụ 3: Kiểm tra Chất lượng Dữ liệu (DQ Check) ---
    # --- 4. Tác vụ 3: Kiểm tra Chất lượng Dữ liệu (DQ Check) ---
    def data_quality_check(**kwargs):
        """Kiểm tra: Null, Giá trị dương, và số lượng dòng."""
        
        ti = kwargs['ti']
        df = ti.xcom_pull(task_ids='extract_close_price')
        rows_loaded = ti.xcom_pull(task_ids='load_to_csv_data_lake')
        
        if df is None or df.empty:
            print("❌ DQ Check bị bỏ qua: Dữ liệu trống.")
            return
        
        # =================================================================
        # 👉 SỬA LỖI KEYERROR: CHUẨN HÓA TÊN CỘT SANG CHỮ THƯỜNG
        # =================================================================
        # Chuyển tất cả tên cột sang chữ thường để đồng bộ với cách vnstock trả về
        df.columns = [col.lower() for col in df.columns] 
        
        # Định nghĩa lại danh sách cột bắt buộc bằng chữ thường
        required_columns = ['close', 'open', 'high', 'low', 'volume']
        
        # 1. Kiểm tra Null (Lỗi cũ ở đây)
        if df[required_columns].isnull().any().any():
            raise ValueError("DQ Check thất bại: Có giá trị Null trong các cột giá trị.")

        # 2. Kiểm tra Giá trị dương (Sử dụng 'close' chữ thường)
        if (df['close'] <= 0).any():
            raise ValueError("DQ Check thất bại: Giá đóng cửa có giá trị <= 0.")
            
        # 3. Kiểm tra số lượng dòng
        if len(df) != rows_loaded:
            raise ValueError(f"DQ Check thất bại: Số dòng tải về ({len(df)}) khác số dòng đã lưu ({rows_loaded}).")
        
        print(f"✅ Kiểm tra Chất lượng Dữ liệu thành công cho {len(df)} dòng dữ liệu.")

    dq_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check,
    )

    # 5. Định nghĩa luồng chạy (Dependencies) 
    extract_task >> load_task >> dq_task