import os
import tempfile
import pytest
from indexer import index

# Create a temporary directory for testing
@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

# Test the get_file_info method
def test_get_file_info(temp_dir):
    # Create a temporary test file
    test_file = os.path.join(temp_dir, "test.txt")
    with open(test_file, "w") as f:
        f.write("Test content")

    indexer = index.FileIndexer(temp_dir, "index.parquet")
    file_name, file_size, content_type = indexer.get_file_info(test_file)

    assert file_name == "test.txt"
    assert file_size == os.path.getsize(test_file)
    assert content_type == "text/plain"

# Test the process_directory method
def test_process_directory(temp_dir):
    # Create some test files and directories
    test_dir = os.path.join(temp_dir, "test_dir")
    os.makedirs(test_dir)
    test_file1 = os.path.join(test_dir, "file1.txt")
    with open(test_file1, "w") as f:
        f.write("Test content")
    test_file2 = os.path.join(test_dir, "file2.txt")
    with open(test_file2, "w") as f:
        f.write("Test content")

    indexer = index.FileIndexer(temp_dir, "index.parquet")
    results = indexer.process_directory(temp_dir)

    assert len(results) == 2
    assert ("file1.txt", os.path.getsize(test_file1), "text/plain") in results
    assert ("file2.txt", os.path.getsize(test_file2), "text/plain") in results

# Test the create_index method
def test_create_index(temp_dir):
    # Create some test files and directories
    test_dir = os.path.join(temp_dir, "test_dir")
    os.makedirs(test_dir)
    test_file1 = os.path.join(test_dir, "file1.txt")
    with open(test_file1, "w") as f:
        f.write("Test content")
    test_file2 = os.path.join(test_dir, "file2.txt")
    with open(test_file2, "w") as f:
        f.write("Test content")

    index_file = os.path.join(temp_dir, "index.parquet")
    indexer = index.FileIndexer(temp_dir, index_file)

    indexer.create_index()

    # Check if the index file exists
    assert os.path.exists(index_file)

