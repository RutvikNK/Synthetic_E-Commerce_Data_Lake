import pytest
import base64
import json

from unittest.mock import MagicMock, patch, ANY
from src.ingestion.main import ingest_event

@pytest.fixture
def mock_cloud_event():
    """Helper to create a fake Pub/Sub message event."""
    def _create_event(data_dict):
        json_data = json.dumps(data_dict)
        encoded_data = base64.b64encode(json_data.encode("utf-8"))
        
        event = MagicMock()
        event.data = {"message": {"data": encoded_data}}
        return event
    return _create_event

# We patch the GLOBAL variables in the module directly
@patch("src.ingestion.main.storage_client")
@patch("src.ingestion.main.BUCKET_NAME", "test-main-bucket")
@patch("src.ingestion.main.QUARANTINE_BUCKET", "test-quarantine-bucket")
def test_ingest_valid_purchase(mock_storage, mock_cloud_event):
    """Test that a valid purchase is routed to the correct folder path."""
    
    # 1. Setup Data
    payload = {
        "event_type": "purchase",
        "timestamp": "2026-01-19T12:00:00",
        "event_id": "abc-123",
        "user_id": "u1"
    }
    event = mock_cloud_event(payload)
    
    # 2. Setup Mock Bucket
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    # 3. Run Function
    ingest_event(event)

    # 4. Assertions
    # Verify it retrieved the main bucket
    mock_storage.bucket.assert_any_call("test-main-bucket")
    
    # Verify path (event_type=purchase/year=2026/month=01/day=19/...)
    expected_blob_name = "event_type=purchase/year=2026/month=01/day=19/abc-123.json"
    mock_bucket.blob.assert_called_with(expected_blob_name)
    
    # Verify upload content
    mock_blob.upload_from_string.assert_called()
    uploaded_data = json.loads(mock_blob.upload_from_string.call_args[0][0])
    assert uploaded_data["event_type"] == "purchase"

@patch("src.ingestion.main.storage_client")
@patch("src.ingestion.main.BUCKET_NAME", "test-main-bucket")
@patch("src.ingestion.main.QUARANTINE_BUCKET", "test-quarantine-bucket")
def test_ingest_poison_pill_to_quarantine(mock_storage, mock_cloud_event):
    """Test that data missing a timestamp goes to Quarantine."""
    
    # 1. Setup Bad Data (No Timestamp)
    payload = {
        "event_type": "page_view",
        # "timestamp": "MISSING", 
        "event_id": "bad-123"
    }
    event = mock_cloud_event(payload)

    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_storage.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    # 2. Run Function
    ingest_event(event)

    # 3. Assertions
    # Verify it tried to write to quarantine
    mock_storage.bucket.assert_called_with("test-quarantine-bucket")
    
    # Verify it uploaded an error report
    mock_blob.upload_from_string.assert_called()
    
    # Check the JSON content of the error report
    uploaded_error = json.loads(mock_blob.upload_from_string.call_args[0][0])
    
    assert "error" in uploaded_error
    assert "Missing 'timestamp'" in uploaded_error["error"]
    # Your code uses "payload", not "original_payload"
    assert "payload" in uploaded_error