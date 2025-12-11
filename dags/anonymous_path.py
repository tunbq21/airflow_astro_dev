import os

class Path_Folder:
    base_dir = "/usr/local/airflow/include/data"

    processed_folder_path = f"{base_dir}/processed"
    raw_folder_path = f"{base_dir}/raw"

    def __init__(self):
        os.makedirs(self.processed_folder_path, exist_ok=True)
        os.makedirs(self.raw_folder_path, exist_ok=True)
