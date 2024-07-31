import os
import shutil
import time


def delete_files_and_folders():
    database_folder = 'database'
    config_folder = 'config'
    time.sleep(10)
    # Delete files in the database folder that do not start with 'ddl_'
    for filename in os.listdir(database_folder):
        if not filename.startswith('ddl_'):
            file_path = os.path.join(database_folder, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)

    # Delete the entire config folder
    if os.path.exists(config_folder):
        shutil.rmtree(config_folder)
