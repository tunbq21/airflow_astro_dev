class Path_Folder:
    base_dir = "/usr/local/airflow/data"

    processed_folder_path = f"{base_dir}/processed"
    raw_folder_path = f"{base_dir}/raw"

    def __init__(self):
        self.processed_folder_path = Path_Folder.processed_folder_path
        self.raw_folder_path = Path_Folder.raw_folder_path
