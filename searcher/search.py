import pyarrow.parquet as pq

class FileSearcher:
    def __init__(self, index_file):
        self.index_file = index_file

    def search_files(self, query):
        matching_files = []
        try:
            # Convert the query to lowercase for case-insensitive matching
            query = query.lower()

            # Search for matching filenames in the Parquet index
            with open(self.index_file, 'rb') as source:
                table = pq.read_table(source)
                file_names = table.column('File Name')
                for _, file_name in enumerate(file_names):
                    if query in file_name.as_py().lower():
                        matching_files.append(file_name.as_py())

        except Exception as e:
            return []

        return matching_files
