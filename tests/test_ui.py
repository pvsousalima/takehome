import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import MagicMock
from ui import render 

# Mock the pq.read_table function
def mock_read_table(parquet_file):
    # Create a simple mock table with the same structure
    columns = {
        'File Name': ['file1.txt', 'file2.txt'],
        'File Size': [1024, 2048],
        'Content Type': ['text/plain', 'application/pdf']
    }
    return MagicMock(columns=columns)

@pytest.fixture
def app(monkeypatch, tmp_path):
    # Create a temporary directory for testing
    temp_dir = tmp_path / "test_data"
    temp_dir.mkdir()

    # Provide the path to the mock Parquet file within the temporary directory
    test_parquet_file = temp_dir / "mock.parquet"

    # Create the mock Parquet file
    data = {
        'File Name': ['file1.txt', 'file2.txt'],
        'File Size': [1024, 2048],
        'Content Type': ['text/plain', 'application/pdf']
    }
    table = pa.table(data)
    pq.write_table(table, test_parquet_file)

    # Monkeypatch pq.read_table to use the mock function
    monkeypatch.setattr("pyarrow.parquet.read_table", mock_read_table)

    return render.App(str(test_parquet_file))

# Test the format_file_size method
def test_format_file_size(app):
    assert app.format_file_size(1023) == "1023 bytes"
    assert app.format_file_size(1024) == "1.00 KB"
    assert app.format_file_size(1048576) == "1.00 MB"
    assert app.format_file_size(1073741824) == "1.00 GB"

# Test the initialize_ui method
def test_initialize_ui(app):
    # Initialize the UI
    app.initialize_ui()

    # Verify that file_widgets list contains the expected number of widgets
    assert len(app.file_widgets) == 2  
