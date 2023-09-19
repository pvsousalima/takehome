from ui import render
from indexer import index
from os import environ
from time import sleep

if __name__ == '__main__':
    # Index BASE_DIRECTORY folder or test_data as default
    base_dir = environ.get("BASE_DIRECTORY", "test_data")
    indexer = index.FileIndexer(base_dir, "file_index.parquet")
    indexer.create_index()
    print("Launching UI...")
    sleep(1)

    # Render Application from index file
    app = render.App("file_index.parquet")
    app.run()