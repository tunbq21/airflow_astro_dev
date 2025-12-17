from airflow import DAG     
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests, json, csv, os, psycopg2, sys
from shopee_etl_repo import ShopeeETL
from anonymous_path import Path_Folder


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

url = "https://tiki.vn/api/personalish/v1/blocks/listings?limit=10&sort=top_seller&page=1&urlKey=xe-may&category=8597"

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "accept": "application/json",
    "referer": "https://tiki.vn/"
}

conn = psycopg2.connect(
    host="host.docker.internal", 
    port=5432,
    database="mydb",              
    user="postgres",              
    password="123456"             
)

Path_Folder = Path_Folder()

Repo = ShopeeETL(headers, url, conn)

json_key = "data"

# ===================================================================================
item_tags = [
    "sku",
    "name",
    "brand_name",
    "short_description",
    "price",
    "list_price",
    "original_price",
    "discount",
    "discount_rate",
    "rating_average",
    "review_count",
    "order_count",
    "quantity_sold",
    "thumbnail_url",
    "thumbnail_width",
    "thumbnail_height",
    "badges_new",
    "seller_product_id",
    "url_key",
    "url_path",
    "shippable",
    "is_visible",
    "productset_id",
    "impression_info"
]

type_map = {
    "sku": "VARCHAR(50)",
    "name": "TEXT",
    "brand_name": "TEXT",
    "short_description": "TEXT",
    "price": "BIGINT",
    "list_price": "BIGINT",
    "original_price": "BIGINT",
    "discount": "BIGINT",
    "discount_rate": "INT",
    "rating_average": "FLOAT",
    "review_count": "INT",
    "order_count": "INT",
    "quantity_sold": "INT",
    "thumbnail_url": "TEXT",
    "thumbnail_width": "INT",
    "thumbnail_height": "INT",
    "badges_new": "JSONB",
    "seller_product_id": "BIGINT",
    "url_key": "TEXT",
    "url_path": "TEXT",
    "shippable": "BOOLEAN",
    "is_visible": "BOOLEAN",
    "productset_id": "BIGINT",
    "impression_info": "JSONB"
}
item_tag = [tag for tag in item_tags if tag in type_map]
table_name = "tiki_byke_items"
cols = ",\n    ".join([f"{col} {type_map.get(col, 'TEXT')}" for col in item_tag])

command_load_0 = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGINT PRIMARY KEY,
    {cols}
);
"""

command_load_1 = f"""
INSERT INTO {table_name} (
    {", ".join(item_tag)}
) VALUES %s
ON CONFLICT (id) DO UPDATE SET
    {", ".join([f"{col} = EXCLUDED.{col}" for col in item_tag if col != 'id'])}
"""


command_load = [command_load_0, command_load_1]
#===================================================================================
def crawl_data():
    Repo.crawl(Path_Folder.raw_folder_path)

def transform_data():
    Repo.transform(Path_Folder.processed_folder_path, Path_Folder.raw_folder_path, json_key, item_tag)

def load_data():
    # Lấy file CSV mới nhất từ thư mục processed
    files = sorted([f for f in os.listdir(Path_Folder.processed_folder_path) if f.endswith(".csv")])
    if not files:
        raise Exception("No processed files found")
    latest_file = os.path.join(Path_Folder.processed_folder_path, files[-1])
    cursor = conn.cursor()
    
    # Tạo bảng nếu chưa tồn tại
    cursor.execute(command_load[0])
    conn.commit()
    
    # Đọc CSV và insert vào database
    with open(latest_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        data_to_insert = []
        
        for row in reader:
            # Chuyển đổi dữ liệu theo đúng kiểu
            row_data = []
            for tag in item_tag:
                value = row.get(tag, "")
                # Xử lý các kiểu dữ liệu đặc biệt
                if tag in ["price", "list_price", "original_price", "discount", "seller_product_id", "productset_id"]:
                    row_data.append(int(value) if value and value != "" else None)
                elif tag in ["discount_rate", "review_count", "order_count", "quantity_sold", "thumbnail_width", "thumbnail_height"]:
                    row_data.append(int(value) if value and value != "" else None)
                elif tag == "rating_average":
                    row_data.append(float(value) if value and value != "" else None)
                elif tag in ["shippable", "is_visible"]:
                    row_data.append(bool(value) if value and value != "" else None)
                elif tag in ["badges_new", "impression_info"]:
                    # Xử lý JSONB - cần escape đúng cách cho PostgreSQL
                    if isinstance(value, str) and value.strip():
                        try:
                            # Thử parse JSON trước
                            parsed_json = json.loads(value)
                            row_data.append(json.dumps(parsed_json))  # Serialize lại để đảm bảo format đúng
                        except json.JSONDecodeError:
                            # Nếu không parse được, coi như string thường và wrap trong JSON
                            row_data.append(json.dumps(value))
                    else:
                        row_data.append(None)
                else:
                    row_data.append(value if value != "" else None)
            
            data_to_insert.append(tuple(row_data))
    
    
    if data_to_insert:
        from psycopg2.extras import execute_values
        try:
            execute_values(
                cursor,
                command_load[1],
                data_to_insert,
                template=None,
                page_size=100
            )
        except Exception as e:
            print(f"Error inserting data: {e}")
            # Fallback: insert từng row một để debug
            for i, row_data in enumerate(data_to_insert):
                try:
                    cursor.execute(
                        f"INSERT INTO {table_name} ({', '.join(item_tag)}) VALUES ({', '.join(['%s'] * len(item_tag))})",
                        row_data
                    )
                except Exception as row_error:
                    print(f"Error at row {i}: {row_error}")
                    print(f"Row data: {row_data}")
                    raise
    
    conn.commit()
    cursor.close()
    
    print(f"Data loaded successfully from {latest_file} to database. Inserted {len(data_to_insert)} rows.")

with DAG(
    dag_id="tiki_pipeline_byke_items",
    schedule="@daily",
    start_date=datetime(2025, 8, 16),
    catchup=False,
    tags=["crawl", "transform", "load"]
) as dag:

    crawl_task = PythonOperator(
        task_id="crawl_items",
        python_callable=crawl_data
    )

    transform_task = PythonOperator(
        task_id="transform_items",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_items",
        python_callable=load_data
    )

    crawl_task >> transform_task >> load_task
