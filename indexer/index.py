import os
import mimetypes
from multiprocessing import Pool
import pyarrow as pa
import pyarrow.parquet as pq
from rich import print

class FileIndexer:
    def __init__(self, base_dir, index_file):
        self.base_dir = base_dir
        self.index_file = index_file

    def get_file_info(self, path):
        # Get file name, size, and content type
        file_name = os.path.basename(path)
        file_size = os.path.getsize(path)
        content_type, _ = mimetypes.guess_type(path)
        return file_name, file_size, content_type

    def process_directory(self, dir_path):
        results = []
        for root, _, files in os.walk(dir_path):
            for file in files:
                file_path = os.path.join(root, file)

                # Attempt to get file information, handle FileNotFoundError
                try:
                    file_name, file_size, content_type = self.get_file_info(file_path)
                    results.append((file_name, file_size, content_type))
                except FileNotFoundError:
                    print(f"File not found: {file_path}")
        return results

    def create_index(self):
        try:
            # Create a Pool of worker processes
            with Pool() as pool:
                # Get file information from directories in parallel
                results = pool.map(self.process_directory, [self.base_dir])

            # Flatten the results list
            file_info_list = [info for sublist in results for info in sublist]

            # Unzip the results into separate lists for each column
            file_names, file_sizes, content_types = zip(*file_info_list)

            # Create a schema
            schema = pa.schema([
                ('File Name', pa.string()),
                ('File Size', pa.int64()),
                ('Content Type', pa.string())
            ])

            # Create a ParquetWriter
            with open(self.index_file, 'wb') as sink:
                with pq.ParquetWriter(sink, schema, compression='BROTLI') as writer:
                    # Write the values directly to the Parquet file
                    writer.write_table(pa.Table.from_arrays([file_names, file_sizes, content_types], schema=schema))

            # Open the Parquet file for reading and print the table index
            with open(self.index_file, 'rb') as source:
                table = pq.read_table(source)
                print("[red]{}[/red]".format(table))

            # Print the count of files in the index
            file_count = len(file_names)
            print(f"Total files indexed: {file_count}")

        except Exception as e:
            print(f"An error occurred: {e}")
