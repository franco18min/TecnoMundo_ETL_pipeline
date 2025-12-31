import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path
from src.tecno_etl.extractors.local_file_extractor import read_file

class TestLocalFileExtractor:

    @patch("src.tecno_etl.extractors.local_file_extractor.pd.read_csv")
    def test_read_csv_success(self, mock_read_csv):
        # Setup mock
        mock_df = pd.DataFrame({"col1": [1, 2]})
        mock_read_csv.return_value = mock_df
        
        file_path = Path("dummy/path/data.csv")
        
        # Execute
        df, file_type = read_file(file_path)
        
        # Assert
        assert file_type == 'csv'
        pd.testing.assert_frame_equal(df, mock_df)
        mock_read_csv.assert_called_once()

    @patch("src.tecno_etl.extractors.local_file_extractor.pd.read_excel")
    def test_read_excel_success(self, mock_read_excel):
        # Setup mock
        mock_df = pd.DataFrame({"col1": [1, 2]})
        mock_read_excel.return_value = mock_df
        
        file_path = Path("dummy/path/data.xlsx")
        
        # Execute
        df, file_type = read_file(file_path)
        
        # Assert
        assert file_type == 'excel'
        pd.testing.assert_frame_equal(df, mock_df)
        
    def test_unsupported_format(self):
        file_path = Path("dummy/path/image.png")
        df, file_type = read_file(file_path)
        
        assert df is None
        assert file_type is None

    @patch("src.tecno_etl.extractors.local_file_extractor.pd.read_csv")
    def test_file_not_found_exception(self, mock_read_csv):
        # Setup mock to raise FileNotFoundError
        # Note: read_file catches FileNotFoundError but the function logic checks extension first.
        # If pd.read_csv raises FileNotFoundError, it should be caught.
        mock_read_csv.side_effect = FileNotFoundError("File not found")
        
        file_path = Path("dummy/path/missing.csv")
        
        df, file_type = read_file(file_path)
        
        assert df is None
        assert file_type is None
