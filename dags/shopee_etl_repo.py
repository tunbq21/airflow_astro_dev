import requests
from interface.etl_interface import ETLInterface
from datetime import datetime
import sys, os, json, csv
from psycopg2.extras import execute_values

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

class ShopeeETL(ETLInterface):

    def __init__(self,header,url,conn_db_postgres):
        self.header = header
        self.url = url
        self.conn_db_postgres = conn_db_postgres

#   =======================================================================================
    """
    Crawls the Shopee API and saves the raw data to a file.
    Provide the necessary parameters for the API request.
    raw_data_path: The path to save the raw data file.
    """
    def crawl(self,raw_data_path):
        response = requests.get(self.url, headers=self.header)
        if response.status_code == 200:
            os.makedirs(raw_data_path, exist_ok=True)
            file_path = os.path.join(raw_data_path, f"raw_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=2)
            print(f"Saved raw JSON to {file_path}")
        else:
            raise Exception(f"Error fetching data: {response.status_code}")
        return response.status_code

#   =======================================================================================
    """
    Provides a method to transform the raw data into a structured format.
    With item_tag(<item_0>, <field_1>, <field_2>,...) and json_key(<json_key>)
    processed_data_path: The path to save the processed data file.
    """
    # def transform(self, processed_data_path, raw_data_path, json_key, item_tag):
    #     os.makedirs(processed_data_path, exist_ok=True)
    #     files = sorted([f for f in os.listdir(raw_data_path) if f.endswith(".json")])
    #     if not files:
    #         raise Exception("No raw files found")

    #     latest_file = os.path.join(raw_data_path, files[-1])

    #     with open(latest_file, "r", encoding="utf-8") as f:
    #         data = json.load(f)

    #     obj = data.get("data", {}).get(json_key, [])

    #     csv_path = os.path.join(
    #         processed_data_path,
    #         f"{json_key}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    #     )

    #     with open(csv_path, mode="w", encoding="utf-8", newline="") as f:
    #         writer = csv.writer(f)
    #         writer.writerow(item_tag)
    #         for r in obj:
    #             writer.writerow([r.get(item, "") for item in item_tag])

    #     print(f"Saved processed CSV to {csv_path}")

    def transform(self, processed_data_path, raw_data_path, json_key, item_tag):
        os.makedirs(processed_data_path, exist_ok=True)
        files = sorted([f for f in os.listdir(raw_data_path) if f.endswith(".json")])
        if not files:
            raise Exception("No raw files found")

        latest_file = os.path.join(raw_data_path, files[-1])

        with open(latest_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Nếu json_key nằm ngay trong root (vd: "data" là list)
        obj = data.get(json_key, [])
        if not isinstance(obj, list):
            raise Exception(f"{json_key} must be a list, but got {type(obj)}")

        csv_path = os.path.join(
            processed_data_path,
            f"{json_key}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        with open(csv_path, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(item_tag)
            for r in obj:
                row = []
                for item in item_tag:
                    value = r.get(item, "")
                    # xử lý đặc biệt cho nested dict (vd: quantity_sold.value)
                    if isinstance(value, dict) and "value" in value:
                        value = value["value"]
                    row.append(value)
                writer.writerow(row)

        print(f"Saved processed CSV to {csv_path}")


#   ===================================================================================

    """
    Loads the processed data into the database.
    With item_tag(<item_0>, <field_1>, <field_2>,...) and command_load(<command_load_0>,<command_load_1>,<command_load_2>,...)
    processed_data_path: The path to the processed data file.
    For item in <item_tag> must be included in the command_load and have the same type of value.
    """
    def load(self, processed_data_path, command_load, item_tag):
        files = sorted([f for f in os.listdir(processed_data_path) if f.endswith(".csv")])
        if not files:
            raise Exception("No processed files found")
        latest_file = os.path.join(processed_data_path, files[-1])
        
        cursor = self.conn_db_postgres.cursor()
        cursor.execute(command_load[0])
        self.conn_db_postgres.commit()

        with open(latest_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                cursor.execute(command_load[1], {item: row.get(item, "") for item in item_tag})
        self.conn_db_postgres.commit()
        cursor.close()
        self.conn_db_postgres.close()

        print(f"Data loaded successfully from {latest_file} to the database.")





